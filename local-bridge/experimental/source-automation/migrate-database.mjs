// Rehearsal only: every output goes into a brand-new directory; the source DB is read-only.
import { DatabaseSync, backup } from 'node:sqlite';
import { mkdir, readFile, writeFile, stat } from 'node:fs/promises';
import { createReadStream, existsSync } from 'node:fs';
import { resolve, isAbsolute, join } from 'node:path';
import { createHash, randomBytes } from 'node:crypto';
import assert from 'node:assert/strict';
import { TransportCore } from '../../src/core.mjs';
import { canonical, insist } from '../../src/protocol.mjs';
import { CandidateCore, CANDIDATE_SCHEMA } from './candidate-core.mjs';

export const LEGACY_TABLES = ['metadata', 'endpoints', 'grants', 'archives', 'archive_sources', 'indexed_records',
  'history_subscriptions', 'history_fanout', 'source_records', 'history_source_meta', 'history_source_parts',
  'history_read_parts', 'messages', 'history_received', 'history_cursors', 'offers', 'offer_items'];
const quoted = value => '"' + value.replaceAll('"', '""') + '"';
export function legacySnapshot(db, { normalizeRestart = false, maxRows = 200000, maxBytes = 256 * 1024 * 1024 } = {}) {
  const result = {}; let totalRows = 0, totalBytes = 0;
  for (const table of LEGACY_TABLES) {
    const columns = db.prepare(`PRAGMA table_info(${quoted(table)})`).all();
    insist(columns.length > 0, 'MIGRATION_SCHEMA_MISMATCH', 'Legacy table missing');
    const primary = columns.filter(c => c.pk).sort((a, b) => a.pk - b.pk).map(c => quoted(c.name));
    const where = table === 'metadata' ? " WHERE key NOT GLOB 'candidate_*'" : '';
    const query = `SELECT * FROM ${quoted(table)}${where} ORDER BY ${primary.length ? primary.join(',') : 'rowid'}`;
    const hash = createHash('sha256'); let count = 0;
    for (let row of db.prepare(query).iterate()) {
      row = { ...row };
      if (normalizeRestart && table === 'endpoints') { delete row.connection_hash; delete row.lease_until; }
      if (normalizeRestart && table === 'offers' && row.state === 'in_flight') { row.state = 'uncertain'; row.error = 'Service stopped before host response'; }
      const encoded = canonical(row) + '\n'; count++; totalRows++; totalBytes += Buffer.byteLength(encoded);
      insist(totalRows <= maxRows && totalBytes <= maxBytes, 'MIGRATION_BUDGET', 'Snapshot audit budget exceeded; use an explicit larger audit budget');
      hash.update(encoded);
    }
    result[table] = { count, sha256: hash.digest('hex') };
  }
  return result;
}
export async function fileSha256(path) {
  const hash = createHash('sha256'); for await (const chunk of createReadStream(path, { highWaterMark: 65536 })) hash.update(chunk);
  return hash.digest('hex');
}
export function verifyLegacyRows(baselinePath, current) {
  const baseline = new DatabaseSync(baselinePath, { readOnly: true }); let rows = 0;
  const normalize = (table, value) => {
    const row = { ...value };
    if (table === 'endpoints') { delete row.connection_hash; delete row.lease_until; }
    if (table === 'offers' && row.state === 'in_flight') { row.state = 'uncertain'; row.error = 'Service stopped before host response'; }
    return row;
  };
  try {
    for (const table of LEGACY_TABLES) {
      const columns = baseline.prepare(`PRAGMA table_info(${quoted(table)})`).all().filter(c => c.pk).sort((a, b) => a.pk - b.pk);
      insist(columns.length > 0, 'MIGRATION_SCHEMA_MISMATCH', 'Original table needs a stable primary key');
      const lookup = current.prepare(`SELECT * FROM ${quoted(table)} WHERE ${columns.map(c => quoted(c.name) + ' IS ?').join(' AND ')}`);
      for (const row of baseline.prepare(`SELECT * FROM ${quoted(table)}`).iterate()) {
        const after = lookup.get(...columns.map(c => row[c.name]));
        assert.ok(after, 'A preserved legacy row disappeared');
        assert.equal(canonical(normalize(table, after)), canonical(normalize(table, row)), `Preserved legacy row changed in ${table}`);
        rows++;
      }
    }
    return { tables: 17, originalRowsVerified: rows, allPreserved: true };
  } finally { baseline.close(); }
}
function inspect(db) {
  assert.equal(db.prepare('PRAGMA quick_check').get().quick_check, 'ok');
  assert.deepEqual(db.prepare('PRAGMA foreign_key_check').all(), []);
}
export async function migrateLegacyCopy({ source, directory, queueLimits = {} }) {
  insist(isAbsolute(source) && isAbsolute(directory), 'INVALID_INPUT', 'Migration paths must be explicit and absolute');
  await mkdir(directory, { recursive: false });
  const beforePath = resolve(directory, 'before.sqlite'), target = resolve(directory, 'candidate.sqlite');
  insist(resolve(source) !== beforePath && resolve(source) !== target && !existsSync(beforePath) && !existsSync(target), 'INVALID_INPUT', 'Migration outputs must be new and distinct from the source');
  const live = new DatabaseSync(source, { readOnly: true });
  let before;
  try {
    insist(!live.prepare("SELECT 1 FROM metadata WHERE key GLOB 'candidate_*'").get(), 'MIGRATION_SCHEMA_MISMATCH', 'This migration only accepts a legacy service DB');
    const tables = live.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT GLOB 'sqlite_*' ORDER BY name").all().map(r => r.name);
    assert.deepEqual(tables, [...LEGACY_TABLES].sort(), 'Only the complete known legacy schema can be migrated');
    before = legacySnapshot(live, { normalizeRestart: true });
    await backup(live, beforePath);
    assert.deepEqual(legacySnapshot(live, { normalizeRestart: true }), before, 'Source durable data changed during backup; retry when idle');
  } finally { live.close(); }
  const saved = new DatabaseSync(beforePath, { readOnly: true });
  let original, expected, workspace, legacySources, preservedGrants, preservedSubscriptions, oldSchema;
  try {
    inspect(saved); original = legacySnapshot(saved); expected = legacySnapshot(saved, { normalizeRestart: true });
    assert.deepEqual(expected, before);
    workspace = saved.prepare("SELECT value FROM metadata WHERE key='workspace'").get().value;
    legacySources = saved.prepare('SELECT * FROM archive_sources ORDER BY archive_id').all();
    preservedGrants = saved.prepare('SELECT * FROM grants ORDER BY id').all();
    preservedSubscriptions = saved.prepare('SELECT * FROM history_subscriptions ORDER BY id').all();
    oldSchema = saved.prepare("SELECT name,sql FROM sqlite_master WHERE type IN ('table','index') AND sql IS NOT NULL ORDER BY name").all();
    await backup(saved, target);
  } finally { saved.close(); }
  const savedHash = await fileSha256(beforePath);
  const adminToken = randomBytes(32).toString('base64url');
  const core = new TransportCore({ database: target, workspace, adminToken });
  try {
    CandidateCore.prototype._initializeCandidate.call(core, queueLimits, { legacySources, preservedGrants, preservedSubscriptions });
    inspect(core.db); assert.deepEqual(legacySnapshot(core.db, { normalizeRestart: true }), expected);
    for (const schema of oldSchema) assert.equal(core.db.prepare('SELECT sql FROM sqlite_master WHERE name=?').get(schema.name)?.sql, schema.sql);
  } finally { core.close(); }
  assert.equal(await fileSha256(beforePath), savedHash);
  const report = { checkedAt: new Date().toISOString(), result: 'passed', source, directory, target, baseline: beforePath,
    workspace, schema: CANDIDATE_SCHEMA, baselineSha256: savedHash, originalTables: original, normalizedTables: expected,
    legacySourcesFrozen: legacySources.length, preservedGrants: preservedGrants.length, preservedSubscriptions: preservedSubscriptions.length,
    originalSchemaRetained: true, quickCheck: 'ok', foreignKeyErrors: 0, sourceOpenedReadOnly: true,
    startupNormalization: ['endpoint connection hash/lease invalidation', 'in_flight offers become uncertain'],
    liveRestorePerformed: false, nativeModelTurns: 0 };
  await writeFile(join(directory, 'migration-report.json'), JSON.stringify(report, null, 2) + '\n');
  return report;
}
export async function verifyRollbackCopy(report) {
  assert.equal(await fileSha256(report.baseline), report.baselineSha256, 'Immutable rollback baseline changed');
  const target = resolve(report.directory, 'rollback-verified.sqlite');
  insist(!existsSync(target) && target !== resolve(report.source), 'INVALID_INPUT', 'Rollback rehearsal must create a fresh file');
  const baseline = new DatabaseSync(report.baseline, { readOnly: true });
  try { inspect(baseline); assert.deepEqual(legacySnapshot(baseline), report.originalTables); await backup(baseline, target); }
  finally { baseline.close(); }
  const restored = new DatabaseSync(target, { readOnly: true });
  try {
    inspect(restored); assert.deepEqual(legacySnapshot(restored), report.originalTables);
    assert.equal(restored.prepare("SELECT 1 FROM metadata WHERE key='candidate_schema'").get(), undefined);
  } finally { restored.close(); }
  assert.equal(await fileSha256(report.baseline), report.baselineSha256);
  const result = { checkedAt: new Date().toISOString(), result: 'passed', target, all17TablesEqual: true,
    immutableBaselineVerified: true, liveRestorePerformed: false, sourceOpenedForWriting: false };
  await writeFile(join(report.directory, 'rollback-report.json'), JSON.stringify(result, null, 2) + '\n');
  return result;
}
