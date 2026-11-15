import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, appendFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { migrateLegacyCopy, verifyRollbackCopy, legacySnapshot } from './migrate-database.mjs';
import { CandidateCore } from './candidate-core.mjs';
import { BoundedSourceIndexer } from './source-pump.mjs';
import { SourceIndexer } from '../../src/indexer.mjs';
import { TransportCore } from '../../src/core.mjs';
import { fixture, ADMIN, ack, message } from '../../tests/helpers.mjs';

const row = text => JSON.stringify({ type: 'response_item', payload: { type: 'message', role: 'user', content: [{ type: 'input_text', text }] } }) + '\n';
async function setup(t) {
  const dir = await mkdtemp(join(tmpdir(), 'local-bridge-migration-')), source = join(dir, 'legacy.sqlite'), path = join(dir, 'legacy.jsonl');
  const f = fixture({ database: source });
  const cores = [f.core];
  t.after(async () => {
    cores.forEach(c => c.close());
    assert.equal(dirname(dir), resolve(tmpdir())); assert.ok(dir.startsWith(join(resolve(tmpdir()), 'local-bridge-migration-')));
    await rm(dir, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 });
  });
  await writeFile(path, row('Read'));
  f.core.attachSource(ADMIN, { archive_id: 'archive-a', path, format: 'codex-jsonl' });
  f.core.subscribeHistory(ADMIN, { id: 'legacy-sub', archive_id: 'archive-a', to: f.b.id, grant_id: 'a-b' });
  const indexer = new SourceIndexer({ core: f.core, adminToken: ADMIN }); await indexer.scan('archive-a');
  const read = f.core.receive(f.cb).items[0]; f.core.acknowledge(f.cb, [ack(read)]);
  await appendFile(path, row('Pending')); await indexer.scan('archive-a');
  const event = f.core.publish(f.ca, message({ id: 'legacy-pending-event' }));
  f.core.setCapabilities(ADMIN, f.b.id, { can_enqueue: true });
  f.core.beginOffer(f.b.id, [event.id], false);
  const before = legacySnapshot(f.core.db, { normalizeRestart: true });
  const report = await migrateLegacyCopy({ source, directory: join(dir, 'migration') });
  assert.deepEqual(legacySnapshot(f.core.db, { normalizeRestart: true }), before);
  const candidate = new CandidateCore({ database: report.target, workspace: report.workspace, adminToken: ADMIN, now: () => f.clock.now }); cores.push(candidate);
  return { ...f, source, dir, path, report, candidate, cores, read };
}

test('WAL-backed copy migration preserves all legacy tables and verifies a fresh rollback copy', async t => {
  const f = await setup(t);
  assert.equal(f.report.legacySourcesFrozen, 1); assert.equal(f.report.originalSchemaRetained, true);
  assert.equal(f.candidate.db.prepare("SELECT state FROM offers WHERE state='uncertain'").get().state, 'uncertain');
  assert.equal(f.candidate.historyCursor(f.candidate.connect(f.b.id, f.b.credential), 'archive-a', 'g1'), 0);
  const rollback = await verifyRollbackCopy(f.report); assert.equal(rollback.all17TablesEqual, true);
  const restored = new DatabaseSync(rollback.target, { readOnly: true });
  try { assert.deepEqual(legacySnapshot(restored), f.report.originalTables); } finally { restored.close(); }
  await assert.rejects(verifyRollbackCopy(f.report), e => e.code === 'INVALID_INPUT');
});

test('frozen sources reject scan, fanout and rebinding while existing pending history can still be read and acknowledged', async t => {
  const f = await setup(t), c = f.candidate, cp = c.sourceState(ADMIN, 'archive-a').checkpoint;
  const indexer = new BoundedSourceIndexer({ core: c, adminToken: ADMIN });
  await assert.rejects(indexer.scan('archive-a'), e => e.code === 'LEGACY_SOURCE_FROZEN');
  assert.throws(() => c.backfillHistory(ADMIN, 'archive-a'), e => e.code === 'LEGACY_SOURCE_FROZEN');
  assert.throws(() => c.subscribeHistory(ADMIN, { id: 'new', archive_id: 'archive-a' }), e => e.code === 'LEGACY_SOURCE_FROZEN');
  await assert.rejects(c.bindSource(ADMIN, { archive_id: 'archive-a', author_ref: 'test' }), e => e.code === 'LEGACY_SOURCE_FROZEN');
  assert.deepEqual(c.sourceState(ADMIN, 'archive-a').checkpoint, cp);
  const connection = c.connect(f.b.id, f.b.credential), items = c.receive(connection).items;
  assert.equal(items.filter(i => i.content_type === 'history_sync').length, 1, 'Previously read legacy history is excluded');
  const h = items.find(i => i.content_type === 'history_sync'); assert.equal(h.payload.records[0].text, 'Pending');
  assert.equal(c.acknowledge(connection, [ack(h)])[0].ok, true);
  assert.equal(c.historyCursor(connection, 'archive-a', 'g1'), 1);
});

test('database triggers prevent an old core from scanning or creating new legacy history fanout', async t => {
  const f = await setup(t); f.candidate.close();
  const old = new TransportCore({ database: f.report.target, workspace: f.report.workspace, adminToken: ADMIN, now: () => f.clock.now }); f.cores.push(old);
  const before = old.db.prepare('SELECT count(*) AS n FROM messages').get().n;
  await assert.rejects(new SourceIndexer({ core: old, adminToken: ADMIN }).scan('archive-a'), /SOURCE_BINDING_REQUIRED/);
  old.grant(ADMIN, { id: 'legacy-extra', from: f.a.id, to: 'other', content_types: ['history_sync'], allow_wake: false, author_ref: 'test' });
  assert.throws(() => old.subscribeHistory(ADMIN, { id: 'unsafe', archive_id: 'archive-a', to: 'other', grant_id: 'legacy-extra' }), /LEGACY_SOURCE_FROZEN/);
  assert.equal(old.db.prepare('SELECT count(*) AS n FROM messages').get().n, before);
  assert.equal(old.db.prepare("SELECT 1 FROM history_subscriptions WHERE id='unsafe'").get(), undefined);
});

test('preserved author scope can be revoked or disabled but cannot silently widen', async t => {
  const f = await setup(t), c = f.candidate;
  assert.throws(() => c.db.exec("UPDATE grants SET author_ref='different' WHERE id='a-b'"), /PRESERVED_SCOPE_IMMUTABLE/);
  c.revoke(ADMIN, 'a-b'); assert.equal(c.preservedScope(ADMIN, 'grants').find(g => g.id === 'a-b').revoked, 1);
  assert.throws(() => c.db.exec("UPDATE grants SET revoked=0 WHERE id='a-b'"), /PRESERVED_SCOPE_IMMUTABLE/);
  c.db.exec("UPDATE history_subscriptions SET enabled=0 WHERE id='legacy-sub'");
  assert.throws(() => c.db.exec("UPDATE history_subscriptions SET enabled=1 WHERE id='legacy-sub'"), /PRESERVED_SCOPE_IMMUTABLE/);
  assert.equal(c.endpoint(f.b.id).capabilities.can_wake, false);
});

test('rollback refuses a modified baseline and never writes into the live source', async t => {
  const f = await setup(t), before = legacySnapshot(f.core.db);
  await appendFile(f.report.baseline, 'tamper');
  await assert.rejects(verifyRollbackCopy(f.report), /Immutable rollback baseline changed/);
  assert.deepEqual(legacySnapshot(f.core.db), before);
});
