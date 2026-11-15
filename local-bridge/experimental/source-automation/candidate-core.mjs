// Candidate-only database gates. This module is never imported by the active service.
import { DatabaseSync } from 'node:sqlite';
import { mkdirSync, openSync, closeSync, realpathSync } from 'node:fs';
import { realpath } from 'node:fs/promises';
import { dirname, resolve, isAbsolute } from 'node:path';
import { randomBytes } from 'node:crypto';
import { TransportCore } from '../../src/core.mjs';
import { BridgeError, insist, text, integer, canonical, digest, keys, MAX_BATCH_COUNT, MAX_BATCH_BYTES } from '../../src/protocol.mjs';
import { READER_VERSION, inspectSourceHeader } from './bounded-reader.mjs';

export const CANDIDATE_SCHEMA = 'local-bridge-candidate-bounded/4';
const SCHEMA = CANDIDATE_SCHEMA;
const samePath = (a, b) => process.platform === 'win32' ? a.toLowerCase() === b.toLowerCase() : a === b;
const requiredTriggers = ['candidate_source_insert', 'candidate_source_update', 'candidate_source_delete',
  'candidate_binding_immutable', 'candidate_binding_delete', 'candidate_record_insert', 'candidate_queue_insert',
  'candidate_legacy_bind', 'candidate_legacy_history', 'candidate_preserved_grant', 'candidate_preserved_subscription'];
const requiredTables = ['metadata', 'endpoints', 'grants', 'archives', 'archive_sources', 'indexed_records',
  'history_subscriptions', 'history_fanout', 'source_records', 'history_source_meta', 'history_source_parts',
  'history_read_parts', 'messages', 'history_received', 'history_cursors', 'offers', 'offer_items',
  'candidate_bindings', 'candidate_limits', 'candidate_legacy_sources', 'candidate_preserved_scope'].sort();
export function preservedFingerprint(table, row) {
  const value = { ...row };
  if (table === 'grants') delete value.revoked;
  else if (table === 'history_subscriptions') { delete value.enabled; delete value.last_error; }
  else throw new BridgeError('INVALID_INPUT', 'Unsupported preserved scope table');
  return digest(canonical(value));
}
const defaultLimits = { pending_count: 64, pending_bytes: 256 * 1024, history_count: 48, history_bytes: 192 * 1024 };
function validateLimits(patch, base = defaultLimits) {
  keys(patch, Object.keys(defaultLimits), 'queueLimits');
  const p = { ...base, ...patch };
  integer(p.pending_count, 'pending_count', 1, 10000); integer(p.pending_bytes, 'pending_bytes', 2048, 64 * 1024 * 1024);
  integer(p.history_count, 'history_count', 1, p.pending_count); integer(p.history_bytes, 'history_bytes', 2048, p.pending_bytes);
  insist(p.history_count < p.pending_count && p.history_bytes < p.pending_bytes, 'INVALID_INPUT', 'Reserve count and byte capacity for message traffic');
  return p;
}

function prepareDatabase(database) {
  if (database === ':memory:') return true;
  insist(isAbsolute(database), 'INVALID_INPUT', 'Candidate database must use an explicit absolute path');
  mkdirSync(dirname(database), { recursive: true });
  let fresh = false;
  try { const fd = openSync(database, 'wx', 0o600); closeSync(fd); fresh = true; }
  catch (error) { if (error.code !== 'EEXIST') throw error; }
  const db = new DatabaseSync(database, { readOnly: !fresh });
  try {
    if (fresh) {
      db.exec('BEGIN IMMEDIATE; CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);');
      db.prepare('INSERT INTO metadata VALUES(?,?)').run('candidate_schema', SCHEMA);
      db.prepare('INSERT INTO metadata VALUES(?,?)').run('candidate_state', 'initializing');
      db.exec('COMMIT');
    } else {
      let valid = false;
      try {
        valid = db.prepare("SELECT value FROM metadata WHERE key='candidate_schema'").get()?.value === SCHEMA
          && db.prepare("SELECT value FROM metadata WHERE key='candidate_state'").get()?.value === 'ready';
        const triggers = new Set(db.prepare("SELECT name FROM sqlite_master WHERE type='trigger'").all().map(r => r.name));
        valid &&= requiredTriggers.every(name => triggers.has(name));
        const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").all().map(r => r.name);
        valid &&= canonical(tables) === canonical(requiredTables);
      } catch { valid = false; }
      insist(valid, 'CANDIDATE_DATABASE_REQUIRED', 'Refusing a legacy, incomplete, or incompatible database before core initialization');
    }
    return fresh;
  } finally { db.close(); }
}

function sqlError(error) {
  for (const code of ['QUEUE_PRESSURE', 'READER_VERSION_MISMATCH', 'SOURCE_BINDING_REQUIRED'])
    if (String(error.message).includes(code)) return new BridgeError(code, code);
  return error;
}

export class CandidateCore extends TransportCore {
  constructor({ database = ':memory:', allowedPaths = [], queueLimits = {}, batchScope, ...options }) {
    insist(Array.isArray(allowedPaths) && allowedPaths.length <= 64, 'INVALID_INPUT', 'Candidate source allowlist must be bounded');
    const canonicalPaths = allowedPaths.map(p => { insist(isAbsolute(p), 'INVALID_INPUT', 'Allowlisted paths must be absolute'); return realpathSync(p); });
    const fresh = prepareDatabase(database);
    super({ ...options, database });
    this.allowedPaths = canonicalPaths;
    this.batchScope = batchScope;
    try {
      if (fresh || database === ':memory:') this._initializeCandidate(queueLimits);
      else if (Object.keys(queueLimits).length) this.setQueueLimits(options.adminToken, queueLimits);
      this.db.exec('BEGIN IMMEDIATE');
      insist(this.db.prepare("SELECT value FROM metadata WHERE key='candidate_schema'").get()?.value === SCHEMA
        && this.db.prepare("SELECT value FROM metadata WHERE key='candidate_state'").get()?.value === 'ready',
      'CANDIDATE_DATABASE_REQUIRED', 'Candidate marker changed during startup');
      this.db.exec('COMMIT');
    } catch (error) { try { this.db.exec('ROLLBACK'); } catch {} this.close(); throw error; }
  }
  _initializeCandidate(limits, { legacySources = [], preservedGrants = [], preservedSubscriptions = [] } = {}) {
    const p = validateLimits(limits);
    this.db.exec(`BEGIN IMMEDIATE;
      INSERT OR REPLACE INTO metadata VALUES('candidate_schema','${SCHEMA}');
      CREATE TABLE candidate_limits(id INTEGER PRIMARY KEY CHECK(id=1),pending_count INTEGER NOT NULL,pending_bytes INTEGER NOT NULL,history_count INTEGER NOT NULL,history_bytes INTEGER NOT NULL);
      CREATE TABLE candidate_bindings(archive_id TEXT PRIMARY KEY REFERENCES archives(id),owner_id TEXT NOT NULL,
        native_session_id TEXT NOT NULL,generation TEXT NOT NULL,path TEXT NOT NULL,format TEXT NOT NULL,
        reader_version TEXT NOT NULL,file_identity TEXT NOT NULL,header_sha256 TEXT NOT NULL,
        initial_mode TEXT NOT NULL,initial_bytes INTEGER NOT NULL,seeding INTEGER NOT NULL,author_ref TEXT NOT NULL);
      CREATE TABLE candidate_legacy_sources(archive_id TEXT PRIMARY KEY REFERENCES archives(id),source_snapshot TEXT NOT NULL,fingerprint TEXT NOT NULL);
      CREATE TABLE candidate_preserved_scope(table_name TEXT NOT NULL,id TEXT NOT NULL,fingerprint TEXT NOT NULL,PRIMARY KEY(table_name,id));
      CREATE TRIGGER candidate_legacy_bind BEFORE INSERT ON candidate_bindings WHEN EXISTS(SELECT 1 FROM candidate_legacy_sources WHERE archive_id=NEW.archive_id)
        BEGIN SELECT RAISE(ABORT,'LEGACY_SOURCE_FROZEN'); END;
      CREATE TRIGGER candidate_legacy_history BEFORE INSERT ON messages WHEN NEW.content_type='history_sync' AND EXISTS(SELECT 1 FROM candidate_legacy_sources WHERE archive_id=json_extract(NEW.payload,'$.archive_id'))
        BEGIN SELECT RAISE(ABORT,'LEGACY_SOURCE_FROZEN'); END;
      CREATE TRIGGER candidate_preserved_grant BEFORE UPDATE ON grants WHEN EXISTS(SELECT 1 FROM candidate_preserved_scope WHERE table_name='grants' AND id=OLD.id) AND
        (NEW.id IS NOT OLD.id OR NEW.from_id IS NOT OLD.from_id OR NEW.to_id IS NOT OLD.to_id OR NEW.content_types IS NOT OLD.content_types
          OR NEW.kinds IS NOT OLD.kinds OR NEW.work_domains IS NOT OLD.work_domains OR NEW.allow_wake IS NOT OLD.allow_wake OR NEW.author_ref IS NOT OLD.author_ref OR NEW.revoked<OLD.revoked)
        BEGIN SELECT RAISE(ABORT,'PRESERVED_SCOPE_IMMUTABLE'); END;
      CREATE TRIGGER candidate_preserved_subscription BEFORE UPDATE ON history_subscriptions WHEN EXISTS(SELECT 1 FROM candidate_preserved_scope WHERE table_name='history_subscriptions' AND id=OLD.id) AND
        (NEW.id IS NOT OLD.id OR NEW.archive_id IS NOT OLD.archive_id OR NEW.target_id IS NOT OLD.target_id OR NEW.grant_id IS NOT OLD.grant_id OR NEW.enabled>OLD.enabled)
        BEGIN SELECT RAISE(ABORT,'PRESERVED_SCOPE_IMMUTABLE'); END;
      CREATE TRIGGER candidate_source_insert BEFORE INSERT ON archive_sources BEGIN
        SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM candidate_bindings b WHERE b.archive_id=NEW.archive_id AND b.path=NEW.path AND b.format=NEW.format)
          THEN RAISE(ABORT,'SOURCE_BINDING_REQUIRED') END;
        SELECT CASE WHEN NEW.checkpoint IS NOT NULL AND COALESCE(json_extract(NEW.checkpoint,'$.reader_version'),'')!='${READER_VERSION}'
          THEN RAISE(ABORT,'READER_VERSION_MISMATCH') END;
        SELECT CASE WHEN NEW.checkpoint IS NOT NULL AND NOT EXISTS(SELECT 1 FROM candidate_bindings b WHERE b.archive_id=NEW.archive_id
          AND json_type(NEW.checkpoint,'$.file_identity')='object'
          AND json_extract(NEW.checkpoint,'$.file_identity.dev') IS json_extract(b.file_identity,'$.dev')
          AND json_extract(NEW.checkpoint,'$.file_identity.ino') IS json_extract(b.file_identity,'$.ino')
          AND json_extract(NEW.checkpoint,'$.file_identity.birthtime_ns') IS json_extract(b.file_identity,'$.birthtime_ns'))
          THEN RAISE(ABORT,'READER_VERSION_MISMATCH') END;
      END;
      CREATE TRIGGER candidate_source_update BEFORE UPDATE ON archive_sources BEGIN
        SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM candidate_bindings b WHERE b.archive_id=NEW.archive_id AND b.path=NEW.path AND b.format=NEW.format)
          THEN RAISE(ABORT,'SOURCE_BINDING_REQUIRED') END;
        SELECT CASE WHEN NEW.checkpoint IS NOT NULL AND (COALESCE(json_extract(NEW.checkpoint,'$.reader_version'),'')!='${READER_VERSION}'
          OR json_type(NEW.checkpoint,'$.file_identity') IS NOT 'object'
          OR NOT EXISTS(SELECT 1 FROM candidate_bindings b WHERE b.archive_id=NEW.archive_id
            AND json_extract(NEW.checkpoint,'$.file_identity.dev') IS json_extract(b.file_identity,'$.dev')
            AND json_extract(NEW.checkpoint,'$.file_identity.ino') IS json_extract(b.file_identity,'$.ino')
            AND json_extract(NEW.checkpoint,'$.file_identity.birthtime_ns') IS json_extract(b.file_identity,'$.birthtime_ns')))
          THEN RAISE(ABORT,'READER_VERSION_MISMATCH') END;
      END;
      CREATE TRIGGER candidate_source_delete BEFORE DELETE ON archive_sources BEGIN SELECT RAISE(ABORT,'SOURCE_BINDING_REQUIRED'); END;
      CREATE TRIGGER candidate_binding_immutable BEFORE UPDATE ON candidate_bindings WHEN
        OLD.archive_id IS NOT NEW.archive_id OR OLD.owner_id IS NOT NEW.owner_id OR OLD.native_session_id IS NOT NEW.native_session_id
        OR OLD.generation IS NOT NEW.generation OR OLD.path IS NOT NEW.path OR OLD.format IS NOT NEW.format
        OR OLD.reader_version IS NOT NEW.reader_version OR OLD.file_identity IS NOT NEW.file_identity
        OR OLD.header_sha256 IS NOT NEW.header_sha256 OR OLD.initial_mode IS NOT NEW.initial_mode OR OLD.initial_bytes IS NOT NEW.initial_bytes
        BEGIN SELECT RAISE(ABORT,'SOURCE_BINDING_REQUIRED'); END;
      CREATE TRIGGER candidate_binding_delete BEFORE DELETE ON candidate_bindings BEGIN SELECT RAISE(ABORT,'SOURCE_BINDING_REQUIRED'); END;
      CREATE TRIGGER candidate_record_insert BEFORE INSERT ON indexed_records BEGIN
        SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM candidate_bindings b WHERE b.archive_id=NEW.archive_id AND b.generation=NEW.generation)
          THEN RAISE(ABORT,'SOURCE_BINDING_REQUIRED') END;
      END;
      CREATE TRIGGER candidate_queue_insert BEFORE INSERT ON messages WHEN NEW.delivery NOT IN ('received','delivery_failed') BEGIN
        SELECT CASE WHEN
          (SELECT count(*) FROM messages WHERE to_id=NEW.to_id AND delivery NOT IN ('received','delivery_failed'))+1 > (SELECT pending_count FROM candidate_limits WHERE id=1)
          OR (SELECT COALESCE(sum(payload_bytes),0) FROM messages WHERE to_id=NEW.to_id AND delivery NOT IN ('received','delivery_failed'))+NEW.payload_bytes > (SELECT pending_bytes FROM candidate_limits WHERE id=1)
          OR (NEW.content_type='history_sync' AND (
            (SELECT count(*) FROM messages WHERE to_id=NEW.to_id AND content_type='history_sync' AND delivery NOT IN ('received','delivery_failed'))+1 > (SELECT history_count FROM candidate_limits WHERE id=1)
            OR (SELECT COALESCE(sum(payload_bytes),0) FROM messages WHERE to_id=NEW.to_id AND content_type='history_sync' AND delivery NOT IN ('received','delivery_failed'))+NEW.payload_bytes > (SELECT history_bytes FROM candidate_limits WHERE id=1)))
          THEN RAISE(ABORT,'QUEUE_PRESSURE') END;
      END;`);
    this.db.prepare('INSERT INTO candidate_limits VALUES(1,?,?,?,?)').run(p.pending_count, p.pending_bytes, p.history_count, p.history_bytes);
    for (const row of legacySources) this.db.prepare('INSERT INTO candidate_legacy_sources VALUES(?,?,?)').run(row.archive_id, canonical(row), digest(canonical(row)));
    for (const [table, rows] of [['grants', preservedGrants], ['history_subscriptions', preservedSubscriptions]])
      for (const row of rows) this.db.prepare('INSERT INTO candidate_preserved_scope VALUES(?,?,?)').run(table, row.id, preservedFingerprint(table, row));
    this.db.prepare('INSERT OR REPLACE INTO metadata VALUES(?,?)').run('candidate_state', 'ready');
    this.db.exec('COMMIT');
  }
  isLegacySource(archiveId) { return Boolean(this.db.prepare('SELECT 1 FROM candidate_legacy_sources WHERE archive_id=?').get(archiveId)); }
  preservedScope(admin, table) {
    this._admin(admin); insist(['grants', 'history_subscriptions'].includes(table), 'INVALID_INPUT', 'Unsupported preserved scope');
    return this.db.prepare('SELECT id,fingerprint FROM candidate_preserved_scope WHERE table_name=?').all(table).map(saved => {
      const row = this.db.prepare(`SELECT * FROM ${table} WHERE id=?`).get(saved.id);
      insist(row && preservedFingerprint(table, row) === saved.fingerprint, 'PRESERVED_SCOPE_IMMUTABLE', 'Preserved operator scope changed');
      return row;
    });
  }
  setCapabilities(admin, id, value) { return super.setCapabilities(admin, id, { ...value, can_wake: false }); }
  endpoint(id) { const value = super.endpoint(id); return { ...value, capabilities: { ...value.capabilities, can_wake: false } }; }
  sourceState(admin, archiveId) {
    const value = super.sourceState(admin, archiveId);
    return this.isLegacySource(archiveId) ? { ...value, status: 'legacy_frozen', legacy_frozen: true } : value;
  }
  _fanoutIndexed(archiveId) {
    insist(!this.isLegacySource(archiveId), 'LEGACY_SOURCE_FROZEN', 'Legacy source fanout is frozen');
    return super._fanoutIndexed(archiveId);
  }
  subscribeHistory(admin, input) { this._admin(admin); insist(!this.isLegacySource(input.archive_id), 'LEGACY_SOURCE_FROZEN', 'Use a new bound archive'); return super.subscribeHistory(admin, input); }
  backfillHistory(admin, archiveId) { this._admin(admin); insist(!this.isLegacySource(archiveId), 'LEGACY_SOURCE_FROZEN', 'Legacy backfill is frozen'); return super.backfillHistory(admin, archiveId); }
  setQueueLimits(admin, patch) {
    this._admin(admin);
    const p = validateLimits(patch, this.db.prepare('SELECT pending_count,pending_bytes,history_count,history_bytes FROM candidate_limits WHERE id=1').get());
    this._tx(() => {
      const targets = this.db.prepare("SELECT to_id,count(*) AS n,sum(payload_bytes) AS bytes,sum(content_type='history_sync') AS hn,sum(CASE WHEN content_type='history_sync' THEN payload_bytes ELSE 0 END) AS hb FROM messages WHERE delivery NOT IN ('received','delivery_failed') GROUP BY to_id").all();
      insist(targets.every(t => t.n <= p.pending_count && t.bytes <= p.pending_bytes && t.hn <= p.history_count && t.hb <= p.history_bytes),
        'QUEUE_LIMIT_BELOW_PENDING', 'New queue limits cannot be smaller than current pending data');
      this.db.prepare('UPDATE candidate_limits SET pending_count=?,pending_bytes=?,history_count=?,history_bytes=? WHERE id=1').run(p.pending_count, p.pending_bytes, p.history_count, p.history_bytes);
    });
    return p;
  }
  async configureStartup(configure) {
    insist(!this.transactionDepth && !this.startupConfigured, 'INVALID_STATE', 'Startup configuration is a one-time operation before serving requests');
    this.startupConfigured = true;
    this.db.exec('BEGIN IMMEDIATE'); this.transactionDepth = 1; this.deferredActivity = new Set();
    try {
      const result = await configure(); this.db.exec('COMMIT'); return result;
    } catch (error) { this.db.exec('ROLLBACK'); throw error; }
    finally { this.transactionDepth = 0; this.deferredActivity.clear(); }
  }
  attachSource() { throw new BridgeError('SOURCE_BINDING_REQUIRED', 'Use the validated operator binding entry point'); }
  async bindSource(admin, input) {
    this._admin(admin); text(input.archive_id, 'archive_id', 256); text(input.author_ref, 'author_ref', 512);
    insist(!this.isLegacySource(input.archive_id), 'LEGACY_SOURCE_FROZEN', 'Legacy sources require a separate new archive/generation');
    insist(['from_start', 'from_now'].includes(input.initial_mode), 'INVALID_INPUT', 'Initial sync mode must be explicit');
    insist(isAbsolute(input.path), 'INVALID_INPUT', 'Source path must be absolute');
    const archive = this.db.prepare('SELECT a.*,e.harness,e.native_session_id FROM archives a JOIN endpoints e ON e.id=a.owner_id WHERE a.id=?').get(input.archive_id);
    insist(archive, 'NOT_FOUND', 'Archive is not registered');
    const format = archive.harness === 'codex' ? 'codex-jsonl' : archive.harness === 'dsh' ? 'dsh-zstd' : null;
    insist(format && input.format === format, 'SOURCE_IDENTITY_MISMATCH', 'Archive format does not match endpoint harness');
    const path = await realpath(input.path);
    insist(this.allowedPaths.some(p => samePath(path, p)), 'SOURCE_NOT_ALLOWED', 'Source path is not explicitly allowlisted');
    const header = await inspectSourceHeader(path, format);
    insist(header.native_session_id === archive.native_session_id, 'SOURCE_IDENTITY_MISMATCH', 'Native source header belongs to another session');
    return this._tx(() => {
      this.db.prepare('INSERT INTO candidate_bindings VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)').run(
        archive.id, archive.owner_id, archive.native_session_id, archive.generation, path, format, READER_VERSION,
        canonical(header.file_identity), header.header_sha256, input.initial_mode, header.snapshot_bytes,
        input.initial_mode === 'from_now' ? 1 : 0, input.author_ref);
      return super.attachSource(admin, { archive_id: archive.id, path, format, max_decoded_bytes: input.max_decoded_bytes });
    });
  }
  binding(admin, archiveId) {
    this._admin(admin); const row = this.db.prepare('SELECT * FROM candidate_bindings WHERE archive_id=?').get(archiveId);
    insist(row, 'SOURCE_BINDING_REQUIRED', 'Source lacks a native identity binding');
    return { ...row, file_identity: JSON.parse(row.file_identity) };
  }
  indexReadOptions(admin, archiveId) {
    const b = this.binding(admin, archiveId);
    insist(this.allowedPaths.some(p => samePath(b.path, p)), 'SOURCE_NOT_ALLOWED', 'Persisted source is absent from this process allowlist');
    return { expectedFileIdentity: b.file_identity, expectedHeaderSha256: b.header_sha256,
      ...(b.seeding ? { snapshotBytes: b.initial_bytes } : {}) };
  }
  prepareIndex(admin, archiveId) {
    this._admin(admin);
    insist(!this.isLegacySource(archiveId), 'LEGACY_SOURCE_FROZEN', 'Legacy scanning is frozen');
    this._tx(() => {
      for (const t of this.db.prepare('SELECT target_id FROM history_subscriptions WHERE archive_id=? AND enabled=1').all(archiveId))
        this._settle(t.target_id, this.now());
    });
  }
  commitIndexedPage(admin, archiveId, expected, page) {
    try {
      return this._tx(() => {
        const b = this.binding(admin, archiveId);
        insist(page.checkpoint.reader_version === READER_VERSION
          && canonical(page.checkpoint.file_identity) === canonical(b.file_identity), 'READER_VERSION_MISMATCH', 'Checkpoint lacks bound reader identity');
        let candidate = page;
        if (b.seeding) candidate = { ...page, records: [], checkpoint: { ...page.checkpoint, visible_next: expected?.visible_next ?? 0 } };
        if (candidate.records.length) this.batchScope?.checkHistory(archiveId, candidate.records, this.db);
        const result = super.commitIndexedPage(admin, archiveId, expected, candidate);
        if (b.seeding && !page.has_more) {
          this.db.prepare('UPDATE candidate_bindings SET seeding=0 WHERE archive_id=?').run(archiveId);
          result.has_more = true; // Scan once after the fixed initial boundary, even if growth preceded polling.
        }
        return result;
      });
    } catch (error) {
      const mapped = sqlError(error);
      if (mapped.code === 'QUEUE_PRESSURE') {
        const targets = this.db.prepare('SELECT target_id FROM history_subscriptions WHERE archive_id=? AND enabled=1').all(archiveId);
        const allEmpty = targets.length > 0 && targets.every(t => this.db.prepare("SELECT count(*) AS n FROM messages WHERE to_id=? AND delivery NOT IN ('received','delivery_failed')").get(t.target_id).n === 0);
        if (allEmpty) throw new BridgeError('PAGE_QUEUE_BUDGET', 'One atomic page cannot fit an empty queue; adjust page or queue limits explicitly');
      }
      throw mapped;
    }
  }
  publish(connection, input) {
    if (this.batchScope) this.batchScope.checkPublication(this._actor(connection).id, input);
    return super.publish(connection, input);
  }
  _publishAs(sender, input, now = this.now()) {
    try {
      return this._tx(() => {
        if (typeof input?.to === 'string' && this.db.prepare('SELECT 1 FROM endpoints WHERE id=?').get(input.to)) this._settle(input.to, now);
        return super._publishAs(sender, input, now);
      });
    } catch (error) { throw sqlError(error); }
  }
  acknowledge(connection, items) {
    const result = super.acknowledge(connection, items);
    const changed = Array.isArray(result) ? result.some(item => item.ok) : !result.duplicate;
    if (changed) for (const observer of this.listeners('capacity')) {
      try { Promise.resolve(observer(connection.endpoint_id)).catch(() => {}); } catch {}
    }
    return result;
  }
  receive(connection, input = {}) {
    if (input?.archive_id !== undefined) return super.receive(connection, input);
    const { limit = MAX_BATCH_COUNT, budgetBytes = MAX_BATCH_BYTES, ids } = input;
    integer(limit, 'limit', 1, MAX_BATCH_COUNT); integer(budgetBytes, 'budgetBytes', 1, MAX_BATCH_BYTES);
    if (ids !== undefined) insist(Array.isArray(ids) && ids.length > 0 && ids.length <= MAX_BATCH_COUNT
      && ids.every(id => typeof id === 'string' && id.length <= 256) && new Set(ids).size === ids.length, 'INVALID_INPUT', 'Invalid requested message IDs');
    const now = this.now(), actor = this._actor(connection, now);
    return this._tx(() => {
      this._settle(actor.id, now);
      let where = "to_id=? AND delivery!='delivery_failed' AND processing!='handled' AND (outcome IS NULL OR outcome='accepted') AND (claim_until IS NULL OR claim_until<=?)";
      const params = [actor.id, now];
      if (!this._receivesHistory(actor)) where += " AND content_type!='history_sync'";
      if (ids) {
        for (const id of ids) insist(this.db.prepare('SELECT 1 FROM messages WHERE id=? AND to_id=?').get(id, actor.id), 'NOT_FOUND', 'Requested message not found');
        where += ` AND id IN (${ids.map(() => '?').join(',')})`; params.push(...ids);
      } else where += " AND NOT(content_type='history_sync' AND delivery='received')";
      const count = this.db.prepare(`SELECT count(*) AS n FROM messages WHERE ${where}`).get(...params).n;
      const rows = this.db.prepare(`SELECT * FROM messages WHERE ${where} ORDER BY queue_seq LIMIT ?`).all(...params, limit);
      const items = []; let used = 0;
      for (const row of rows) {
        if (used + row.payload_bytes > budgetBytes) { insist(items.length > 0, 'BUDGET_TOO_SMALL', 'First pending message exceeds this receive budget'); break; }
        const token = randomBytes(32).toString('base64url');
        this.db.prepare('UPDATE messages SET claim_epoch=?,claim_hash=?,claim_until=? WHERE id=?').run(actor.epoch, digest(token), now + this.leaseMs, row.id);
        items.push({ ...this._view(row), claim: { token, epoch: actor.epoch, expires_at: now + this.leaseMs } }); used += row.payload_bytes;
      }
      return { items, payload_bytes: used, remaining: count - items.length };
    });
  }
  sourceError(admin, archiveId, reason) {
    this._admin(admin); if (this.isLegacySource(archiveId)) return;
    super.sourceError(admin, archiveId, reason);
    if (reason === 'QUEUE_PRESSURE') this.db.prepare("UPDATE archive_sources SET status='paused_backpressure' WHERE archive_id=?").run(archiveId);
  }
}
