import { DatabaseSync } from 'node:sqlite';
import { randomBytes, randomUUID, timingSafeEqual } from 'node:crypto';
import { mkdirSync, openSync, closeSync, writeFileSync, readFileSync, unlinkSync } from 'node:fs';
import { dirname, resolve, isAbsolute } from 'node:path';
import { EventEmitter } from 'node:events';
import { historyEnvelopes } from './history.mjs';
import {
  PROTOCOL, BridgeError, insist, object, text, integer, choices, canonical, digest, settings,
  capabilities, CONTENT_TYPES, DEFAULT_SETTINGS, MAX_BATCH_BYTES, MAX_BATCH_COUNT, OUTCOMES,
  DEFAULT_HISTORY_BATCH_BYTES, MAX_HISTORY_BATCH_BYTES, keys,
  validateEnvelope,
} from './protocol.mjs';

const token = () => randomBytes(32).toString('base64url');
const same = (value, expected) => typeof value === 'string' && typeof expected === 'string' && /^[a-f0-9]{64}$/.test(expected)
  && timingSafeEqual(Buffer.from(digest(value), 'hex'), Buffer.from(expected, 'hex'));
const parse = JSON.parse;
const TERMINAL = ['completed', 'declined', 'failed', 'cancelled'];
const unreadHistory = `to_id=? AND content_type='history_sync' AND delivery IN ('queued','offered')
  AND processing!='handled' AND (outcome IS NULL OR outcome='accepted')
  AND json_extract(payload,'$.archive_id')=? AND json_extract(payload,'$.generation')=?`;
const historyClaimHash = receipt => 'history:' + digest(receipt);
const SYNC_REQUEST_TTL_MS = 10 * 60_000;

function syncRequestKey(target, id) {
  insist(typeof id === 'string' && /^[a-f0-9]{32}$/.test(id), 'INVALID_INPUT', 'Invalid history synchronization request ID');
  return 'history-sync:' + digest(canonical([target, id])).slice(0, 32);
}

function syncOperation(db, key) {
  const row = db.prepare('SELECT value FROM metadata WHERE key=?').get(key);
  return row ? parse(row.value) : null;
}

function saveSyncOperation(db, key, value) {
  db.prepare('INSERT INTO metadata(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value')
    .run(key, canonical(value));
}

function historyRuns(packets) {
  const records = packets.flatMap(packet => packet.records).sort((a, b) => a.seq - b.seq
    || (a.fragment?.index ?? 0) - (b.fragment?.index ?? 0));
  const runs = [];
  let previous;
  for (const record of records) {
    if (!previous || previous.role !== record.role || record.seq > previous.seq + 1) {
      runs.push({ role: record.role, messages: [] });
    }
    const entry = [record.seq, record.text];
    if (record.fragment) entry.push([record.fragment.index + 1, record.fragment.count]);
    runs.at(-1).messages.push(entry);
    previous = record;
  }
  return { runs, range: records.length ? [records[0].seq, records.at(-1).seq] : null };
}

function exclusiveOwner(database) {
  if (database === ':memory:') return () => {};
  const lock = resolve(database) + '.owner';
  const identity = canonical({ pid: process.pid, id: randomUUID() });
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const fd = openSync(lock, 'wx', 0o600);
      try { writeFileSync(fd, identity); } finally { closeSync(fd); }
      return () => { try { if (readFileSync(lock, 'utf8') === identity) unlinkSync(lock); } catch (error) { if (error.code !== 'ENOENT') throw error; } };
    } catch (error) {
      if (error.code !== 'EEXIST') throw error;
      let before, owner;
      try { before = readFileSync(lock, 'utf8'); owner = JSON.parse(before); } catch { throw new BridgeError('CORE_ALREADY_RUNNING', 'Owner lock requires inspection'); }
      insist(Number.isSafeInteger(owner.pid) && owner.pid > 0, 'CORE_ALREADY_RUNNING', 'Owner lock requires inspection');
      let dead = false;
      try { process.kill(owner.pid, 0); } catch (probe) { dead = probe.code === 'ESRCH'; }
      insist(dead, 'CORE_ALREADY_RUNNING', 'Another process already owns this workspace database');
      try { if (readFileSync(lock, 'utf8') === before) unlinkSync(lock); } catch (cleanup) { if (cleanup.code !== 'ENOENT') throw cleanup; }
    }
  }
  throw new BridgeError('CORE_ALREADY_RUNNING', 'Could not acquire workspace database ownership');
}

export class TransportCore extends EventEmitter {
  constructor({ database = ':memory:', workspace = 'test-workspace', adminToken, now = Date.now,
    leaseMs = 30_000, historyClaimMs = 180_000 } = {}) {
    super();
    text(adminToken, 'adminToken'); insist(adminToken.length >= 16, 'INVALID_INPUT', 'Admin token must have at least 16 characters');
    integer(leaseMs, 'leaseMs', 100, 3_600_000);
    integer(historyClaimMs, 'historyClaimMs', 100, SYNC_REQUEST_TTL_MS);
    this.adminHash = digest(adminToken); this.now = now; this.leaseMs = leaseMs; this.closed = false;
    this.historyClaimMs = historyClaimMs;
    if (database !== ':memory:') mkdirSync(dirname(database), { recursive: true });
    this.releaseOwner = exclusiveOwner(database);
    try {
    this.db = new DatabaseSync(database);
    this.db.exec(`PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=3000; PRAGMA synchronous=FULL;
      CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS endpoints(
        id TEXT PRIMARY KEY,harness TEXT NOT NULL,native_session_id TEXT NOT NULL,
        work_domains TEXT NOT NULL,settings TEXT NOT NULL,capabilities TEXT NOT NULL,
        credential_hash TEXT NOT NULL,epoch INTEGER NOT NULL DEFAULT 0,connection_hash TEXT,lease_until INTEGER NOT NULL DEFAULT 0,
        UNIQUE(harness,native_session_id));
      CREATE TABLE IF NOT EXISTS grants(
        id TEXT PRIMARY KEY,from_id TEXT NOT NULL REFERENCES endpoints(id),to_id TEXT NOT NULL REFERENCES endpoints(id),
        content_types TEXT NOT NULL,kinds TEXT NOT NULL,work_domains TEXT NOT NULL,allow_wake INTEGER NOT NULL,
        author_ref TEXT NOT NULL,revoked INTEGER NOT NULL DEFAULT 0);
      CREATE TABLE IF NOT EXISTS archives(id TEXT PRIMARY KEY,owner_id TEXT NOT NULL REFERENCES endpoints(id),generation TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS archive_sources(archive_id TEXT PRIMARY KEY REFERENCES archives(id),path TEXT NOT NULL,format TEXT NOT NULL,max_decoded_bytes INTEGER NOT NULL,checkpoint TEXT,status TEXT NOT NULL DEFAULT 'ready',last_error TEXT,updated_at INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS indexed_records(archive_id TEXT NOT NULL,generation TEXT NOT NULL,seq INTEGER NOT NULL,record TEXT NOT NULL,fingerprint TEXT NOT NULL,PRIMARY KEY(archive_id,generation,seq));
      CREATE TABLE IF NOT EXISTS history_subscriptions(id TEXT PRIMARY KEY,archive_id TEXT NOT NULL REFERENCES archives(id),target_id TEXT NOT NULL REFERENCES endpoints(id),grant_id TEXT NOT NULL REFERENCES grants(id),enabled INTEGER NOT NULL DEFAULT 1,last_error TEXT,UNIQUE(archive_id,target_id));
      CREATE TABLE IF NOT EXISTS history_fanout(target_id TEXT NOT NULL,archive_id TEXT NOT NULL,generation TEXT NOT NULL,seq INTEGER NOT NULL,PRIMARY KEY(target_id,archive_id,generation,seq));
      CREATE TABLE IF NOT EXISTS source_records(archive_id TEXT NOT NULL,generation TEXT NOT NULL,seq INTEGER NOT NULL,fingerprint TEXT NOT NULL,PRIMARY KEY(archive_id,generation,seq));
      CREATE TABLE IF NOT EXISTS history_source_meta(archive_id TEXT NOT NULL,generation TEXT NOT NULL,seq INTEGER NOT NULL,parts INTEGER NOT NULL,full_sha TEXT NOT NULL,metadata_sha TEXT NOT NULL,PRIMARY KEY(archive_id,generation,seq));
      CREATE TABLE IF NOT EXISTS history_source_parts(archive_id TEXT NOT NULL,generation TEXT NOT NULL,seq INTEGER NOT NULL,part INTEGER NOT NULL,text TEXT NOT NULL,part_sha TEXT NOT NULL,PRIMARY KEY(archive_id,generation,seq,part));
      CREATE TABLE IF NOT EXISTS history_read_parts(target_id TEXT NOT NULL,archive_id TEXT NOT NULL,generation TEXT NOT NULL,seq INTEGER NOT NULL,part INTEGER NOT NULL,PRIMARY KEY(target_id,archive_id,generation,seq,part));
      CREATE TABLE IF NOT EXISTS messages(
        id TEXT PRIMARY KEY,from_id TEXT NOT NULL REFERENCES endpoints(id),to_id TEXT NOT NULL REFERENCES endpoints(id),
        queue_seq INTEGER NOT NULL,content_type TEXT NOT NULL,payload TEXT NOT NULL,payload_bytes INTEGER NOT NULL,
        grant_id TEXT NOT NULL REFERENCES grants(id),fingerprint TEXT NOT NULL,created_at INTEGER NOT NULL,expires_at INTEGER,
        delivery TEXT NOT NULL DEFAULT 'queued',processing TEXT NOT NULL DEFAULT 'unread',outcome TEXT,reason TEXT,
        claim_epoch INTEGER,claim_hash TEXT,claim_until INTEGER,received_at INTEGER,handled_at INTEGER,
        UNIQUE(to_id,queue_seq));
      CREATE INDEX IF NOT EXISTS pending_messages ON messages(to_id,delivery,queue_seq);
      CREATE TABLE IF NOT EXISTS history_received(
        message_id TEXT PRIMARY KEY REFERENCES messages(id),target_id TEXT NOT NULL,archive_id TEXT NOT NULL,
        generation TEXT NOT NULL,start_seq INTEGER NOT NULL,end_seq INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS history_cursors(
        target_id TEXT NOT NULL,archive_id TEXT NOT NULL,generation TEXT NOT NULL,read_through INTEGER NOT NULL,
        PRIMARY KEY(target_id,archive_id,generation));
      CREATE TABLE IF NOT EXISTS offers(
        id TEXT PRIMARY KEY,target_id TEXT NOT NULL REFERENCES endpoints(id),epoch INTEGER NOT NULL,
        state TEXT NOT NULL,ids TEXT NOT NULL,wake INTEGER NOT NULL,created_at INTEGER NOT NULL,
        native_ref TEXT,error TEXT);
      CREATE TABLE IF NOT EXISTS offer_items(offer_id TEXT NOT NULL REFERENCES offers(id),message_id TEXT NOT NULL REFERENCES messages(id),PRIMARY KEY(offer_id,message_id));`);
    const stored = this.db.prepare('SELECT value FROM metadata WHERE key=?').get('workspace');
    if (stored && stored.value !== workspace) throw new BridgeError('WORKSPACE_MISMATCH', 'Database belongs to another workspace');
    this.db.prepare('INSERT OR IGNORE INTO metadata VALUES(?,?)').run('workspace', workspace);
    this.db.exec('UPDATE endpoints SET connection_hash=NULL,lease_until=0');
    // An interrupted external call may already have reached its host.
    this.db.exec("UPDATE offers SET state='uncertain',error='Service stopped before host response' WHERE state='in_flight'");
    } catch (error) { try { this.db?.close(); } finally { this.releaseOwner(); } throw error; }
  }
  close() { if (!this.closed) { this.closed = true; try { this.db.close(); } finally { this.releaseOwner(); this.removeAllListeners(); } } }
  _admin(secret) { insist(same(secret, this.adminHash), 'UNAUTHORIZED', 'Administrator authentication required'); }
  _tx(fn) {
    const outer = !this.transactionDepth, savepoint = `bridge_${(this.savepointSerial = (this.savepointSerial ?? 0) + 1)}`;
    if (outer) this.deferredActivity = new Set();
    const previousActivity = new Set(this.deferredActivity);
    this.db.exec(outer ? 'BEGIN IMMEDIATE' : `SAVEPOINT ${savepoint}`);
    this.transactionDepth = (this.transactionDepth ?? 0) + 1;
    let result;
    try { result = fn(); this.db.exec(outer ? 'COMMIT' : `RELEASE ${savepoint}`); }
    catch (error) {
      try { this.db.exec(outer ? 'ROLLBACK' : `ROLLBACK TO ${savepoint}; RELEASE ${savepoint}`); } catch { /* Keep the original storage failure. */ }
      this.deferredActivity = previousActivity; throw error;
    } finally { this.transactionDepth--; }
    if (outer) { const ready = [...this.deferredActivity]; this.deferredActivity.clear(); ready.forEach(endpoint => this._notify(endpoint)); }
    return result;
  }
  _notify(endpoint) {
    if (this.transactionDepth) { this.deferredActivity.add(endpoint); return; }
    for (const fn of this.listeners('activity')) { try { Promise.resolve(fn(endpoint)).catch(() => {}); } catch { /* Durable commit is independent of observers. */ } }
  }
  _endpoint(id) { const row = this.db.prepare('SELECT * FROM endpoints WHERE id=?').get(id); insist(row, 'NOT_FOUND', 'Endpoint not found'); return row; }
  endpoint(id) {
    const row = this._endpoint(id);
    return { id: row.id, harness: row.harness, native_session_id: row.native_session_id, work_domains: parse(row.work_domains),
      settings: parse(row.settings), capabilities: parse(row.capabilities), epoch: row.epoch,
      online: Boolean(row.connection_hash && row.lease_until > this.now()) };
  }
  listEndpoints(admin) { this._admin(admin); return this.db.prepare('SELECT id FROM endpoints ORDER BY id').all().map(r => this.endpoint(r.id)); }
  register(admin, input) {
    this._admin(admin); object(input, 'endpoint');
    for (const k of ['id', 'harness', 'native_session_id']) text(input[k], k, 256);
    const domains = choices(input.work_domains ?? ['text'], ['text', 'engineering'], 'work_domains');
    insist(!this.db.prepare('SELECT 1 FROM endpoints WHERE id=? OR (harness=? AND native_session_id=?)').get(input.id, input.harness, input.native_session_id), 'ALREADY_REGISTERED', 'This address or native session is already registered');
    const credential = input.credential ?? token();
    insist(typeof credential === 'string' && /^[A-Za-z0-9_-]{32,256}$/.test(credential), 'INVALID_INPUT', 'Endpoint credential must be a sufficiently long local secret');
    this.db.prepare('INSERT INTO endpoints(id,harness,native_session_id,work_domains,settings,capabilities,credential_hash) VALUES(?,?,?,?,?,?,?)')
      .run(input.id, input.harness, input.native_session_id, canonical(domains), canonical(settings(input.settings ?? {})), canonical(capabilities(input.capabilities)), digest(credential));
    return { ...this.endpoint(input.id), credential };
  }
  connect(id, credential) {
    const now = this.now();
    const result = this._tx(() => {
      const row = this._endpoint(id); insist(same(credential, row.credential_hash), 'UNAUTHORIZED', 'Invalid endpoint credential');
      const connectionToken = token(), epoch = row.epoch + 1;
      this.db.prepare('UPDATE endpoints SET epoch=?,connection_hash=?,lease_until=? WHERE id=?').run(epoch, digest(connectionToken), now + this.leaseMs, id);
      this.db.prepare("UPDATE messages SET claim_epoch=NULL,claim_hash=NULL,claim_until=NULL WHERE to_id=? AND processing!='handled'").run(id);
      return { endpoint_id: id, epoch, token: connectionToken, lease_until: now + this.leaseMs };
    });
    this._notify(id); return result;
  }
  authenticateEndpoint(id, credential) {
    const row = this._endpoint(id); insist(same(credential, row.credential_hash), 'UNAUTHORIZED', 'Invalid endpoint credential');
    return this.endpoint(id);
  }
  disconnect(connection) {
    const row = this._actor(connection);
    this.db.prepare('UPDATE endpoints SET connection_hash=NULL,lease_until=0,epoch=epoch+1 WHERE id=?').run(row.id);
    this._notify(row.id); return { disconnected: true };
  }
  _actor(connection, now = this.now()) {
    object(connection, 'connection'); text(connection.endpoint_id, 'endpoint_id', 256);
    const row = this._endpoint(connection.endpoint_id);
    insist(connection.epoch === row.epoch && same(connection.token, row.connection_hash) && row.lease_until > now, 'STALE_CONNECTION', 'Connection is expired or superseded');
    return row;
  }
  heartbeat(connection) {
    const now = this.now(), row = this._actor(connection, now);
    this.db.prepare('UPDATE endpoints SET lease_until=? WHERE id=?').run(now + this.leaseMs, row.id);
    return { lease_until: now + this.leaseMs };
  }
  sessionInfo(connection) { return this.endpoint(this._actor(connection).id); }
  setSettings(admin, id, patch) {
    this._admin(admin); const row = this._endpoint(id), updated = settings(patch, parse(row.settings));
    this.db.prepare('UPDATE endpoints SET settings=? WHERE id=?').run(canonical(updated), id);
    this._notify(id); return updated;
  }
  setCapabilities(admin, id, value) {
    this._admin(admin); const updated = capabilities(value); this._endpoint(id);
    this.db.prepare('UPDATE endpoints SET capabilities=? WHERE id=?').run(canonical(updated), id);
    this._notify(id); return updated;
  }
  setWorkDomains(admin, id, domains) {
    this._admin(admin); this._endpoint(id); choices(domains, ['text', 'engineering'], 'work_domains');
    this.db.prepare('UPDATE endpoints SET work_domains=? WHERE id=?').run(canonical(domains), id);
    this._notify(id);
  }
  grant(admin, input) {
    this._admin(admin); object(input, 'grant');
    for (const k of ['id', 'from', 'to', 'author_ref']) text(input[k], k, k === 'author_ref' ? 1024 : 256);
    this._endpoint(input.from); this._endpoint(input.to);
    choices(input.content_types, CONTENT_TYPES, 'content_types');
    const kinds = choices(input.kinds ?? ['event', 'request', 'result', 'cancel'], ['event', 'request', 'result', 'cancel'], 'kinds');
    const domains = choices(input.work_domains ?? ['text'], ['text', 'engineering'], 'work_domains');
    insist(typeof input.allow_wake === 'boolean', 'INVALID_INPUT', 'allow_wake must be explicit');
    insist(!this.db.prepare('SELECT 1 FROM grants WHERE id=?').get(input.id), 'ALREADY_REGISTERED', 'Grant already exists');
    this.db.prepare('INSERT INTO grants(id,from_id,to_id,content_types,kinds,work_domains,allow_wake,author_ref) VALUES(?,?,?,?,?,?,?,?)')
      .run(input.id, input.from, input.to, canonical(input.content_types), canonical(kinds), canonical(domains), Number(input.allow_wake), input.author_ref);
    return { id: input.id };
  }
  revoke(admin, id) { this._admin(admin); this.db.prepare('UPDATE grants SET revoked=1 WHERE id=?').run(id); }
  registerArchive(admin, input) {
    this._admin(admin); for (const k of ['id', 'owner', 'generation']) text(input[k], k, 256); this._endpoint(input.owner);
    insist(!this.db.prepare('SELECT 1 FROM archives WHERE id=?').get(input.id), 'ALREADY_REGISTERED', 'Archive already registered');
    this.db.prepare('INSERT INTO archives VALUES(?,?,?)').run(input.id, input.owner, input.generation);
  }
  _grantFor(sender, envelope) {
    const g = this.db.prepare('SELECT * FROM grants WHERE id=?').get(envelope.grant_id);
    insist(g && !g.revoked && g.from_id === sender && g.to_id === envelope.to && parse(g.content_types).includes(envelope.content_type), 'NOT_AUTHORIZED', 'No active grant for this direction and content type');
    if (envelope.content_type === 'message') {
      const p = envelope.payload;
      insist(parse(g.kinds).includes(p.kind), 'NOT_AUTHORIZED', 'Message kind is outside the grant');
      if (p.authority_ref) insist(p.authority_ref.source_ref === g.author_ref, 'NOT_AUTHORIZED', 'Authority source does not match the operator grant');
      if (p.work_domain) insist(parse(g.work_domains).includes(p.work_domain), 'WORK_DOMAIN_MISMATCH', 'Work domain is outside the grant');
    }
    return g;
  }
  publish(connection, input) {
    const now = this.now(), sender = this._actor(connection, now);
    return this._publishAs(sender.id, input, now);
  }
  _publishAs(senderId, input, now = this.now()) {
    const sender = this._endpoint(senderId);
    const { envelope: e, fingerprint, payloadBytes } = validateEnvelope(input);
    const result = this._tx(() => {
      const old = this.db.prepare('SELECT * FROM messages WHERE id=?').get(e.id);
      if (old) {
        insist(old.from_id === sender.id && old.fingerprint === fingerprint, 'ID_CONFLICT', 'Message id cannot be reused with a different sender or body');
        return { ...this._view(old), duplicate: true };
      }
      const recipient = this._endpoint(e.to); this._grantFor(sender.id, e);
      const p = e.payload;
      if (e.content_type === 'history_sync') {
        const archive = this.db.prepare('SELECT * FROM archives WHERE id=?').get(p.archive_id);
        insist(archive && archive.owner_id === sender.id && archive.generation === p.generation, 'SOURCE_MISMATCH', 'History source is not registered to this sender and generation');
        for (const record of p.records) {
          const parts = record.fragment?.count ?? 1, part = record.fragment?.index ?? 0, fullSha = record.fragment?.sha256 ?? digest(record.text);
          const metadataSha = digest(canonical({ role: record.role, source_ref: record.source_ref }));
          const existing = this.db.prepare('SELECT * FROM history_source_meta WHERE archive_id=? AND generation=? AND seq=?').get(p.archive_id, p.generation, record.seq);
          insist(!existing || existing.parts === parts && existing.full_sha === fullSha && existing.metadata_sha === metadataSha, 'SOURCE_CONFLICT', 'An indexed source record changed without a new generation');
          const oldPart = this.db.prepare('SELECT part_sha FROM history_source_parts WHERE archive_id=? AND generation=? AND seq=? AND part=?').get(p.archive_id, p.generation, record.seq, part);
          insist(!oldPart || oldPart.part_sha === digest(record.text), 'SOURCE_CONFLICT', 'A source fragment changed');
          this.db.prepare('INSERT OR IGNORE INTO history_source_meta VALUES(?,?,?,?,?,?)').run(p.archive_id, p.generation, record.seq, parts, fullSha, metadataSha);
          this.db.prepare('INSERT OR IGNORE INTO history_source_parts VALUES(?,?,?,?,?,?)').run(p.archive_id, p.generation, record.seq, part, record.text, digest(record.text));
          const collected = this.db.prepare('SELECT text FROM history_source_parts WHERE archive_id=? AND generation=? AND seq=? ORDER BY part').all(p.archive_id, p.generation, record.seq);
          if (collected.length === parts) insist(digest(collected.map(r => r.text).join('')) === fullSha, 'SOURCE_CONFLICT', 'Complete source record does not match its checksum');
        }
      } else if (p.kind === 'request') {
        insist(parse(recipient.work_domains).includes(p.work_domain), 'WORK_DOMAIN_MISMATCH', 'Recipient does not accept this work domain');
      } else if (['result', 'cancel'].includes(p.kind)) {
        const parent = this.db.prepare('SELECT * FROM messages WHERE id=?').get(p.reply_to);
        insist(parent?.content_type === 'message', 'INVALID_REFERENCE', 'Related request not found');
        const original = parse(parent.payload);
        insist(original.kind === 'request' && original.correlation_id === p.correlation_id && original.work_domain === p.work_domain, 'INVALID_REFERENCE', 'Response must retain the request correlation and work domain');
        const validDirection = p.kind === 'result' ? parent.to_id === sender.id && parent.from_id === e.to : parent.from_id === sender.id && parent.to_id === e.to;
        insist(validDirection, 'INVALID_REFERENCE', 'Response direction does not match its request');
      }
      const expiry = e.expires_at === undefined ? null : Date.parse(e.expires_at);
      insist(expiry === null || expiry > now, 'EXPIRED', 'Message has already expired');
      const seq = this.db.prepare('SELECT COALESCE(MAX(queue_seq),0)+1 AS n FROM messages WHERE to_id=?').get(e.to).n;
      this.db.prepare('INSERT INTO messages(id,from_id,to_id,queue_seq,content_type,payload,payload_bytes,grant_id,fingerprint,created_at,expires_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)')
        .run(e.id, sender.id, e.to, seq, e.content_type, canonical(p), payloadBytes, e.grant_id, fingerprint, now, expiry);
      return { ...this._view(this.db.prepare('SELECT * FROM messages WHERE id=?').get(e.id)), duplicate: false };
    });
    if (!result.duplicate) this._notify(e.to);
    return result;
  }
  attachSource(admin, input) {
    this._admin(admin); object(input, 'source'); text(input.archive_id, 'archive_id', 256); text(input.path, 'path', 4096);
    insist(isAbsolute(input.path), 'INVALID_INPUT', 'Archive path must be explicitly registered as an absolute path');
    insist(['codex-jsonl', 'dsh-zstd'].includes(input.format), 'INVALID_INPUT', 'Unknown archive format');
    const budget = input.max_decoded_bytes ?? 64 * 1024 * 1024; integer(budget, 'max_decoded_bytes', 1, 512 * 1024 * 1024);
    insist(this.db.prepare('SELECT 1 FROM archives WHERE id=?').get(input.archive_id), 'NOT_FOUND', 'Archive is not registered');
    insist(!this.db.prepare('SELECT 1 FROM archive_sources WHERE archive_id=?').get(input.archive_id), 'ALREADY_REGISTERED', 'Source already attached; replacement requires explicit reindexing');
    this.db.prepare('INSERT INTO archive_sources(archive_id,path,format,max_decoded_bytes,updated_at) VALUES(?,?,?,?,?)').run(input.archive_id, resolve(input.path), input.format, budget, this.now());
    return this.sourceState(admin, input.archive_id);
  }
  sourceState(admin, archiveId) {
    this._admin(admin);
    const row = this.db.prepare('SELECT s.*,a.owner_id,a.generation FROM archive_sources s JOIN archives a ON a.id=s.archive_id WHERE s.archive_id=?').get(archiveId);
    insist(row, 'NOT_FOUND', 'Attached source not found');
    return { ...row, checkpoint: row.checkpoint ? parse(row.checkpoint) : null };
  }
  sourceError(admin, archiveId, reason) {
    this._admin(admin); text(reason, 'reason', 256);
    this.db.prepare('UPDATE archive_sources SET status=?,last_error=?,updated_at=? WHERE archive_id=?').run(reason === 'SOURCE_REPLACED' ? 'needs_reindex' : 'error', reason, this.now(), archiveId);
  }
  subscribeHistory(admin, input) {
    this._admin(admin); for (const k of ['id', 'archive_id', 'to', 'grant_id']) text(input[k], k, 256);
    return this._tx(() => {
      const source = this.db.prepare('SELECT * FROM archives WHERE id=?').get(input.archive_id); insist(source, 'NOT_FOUND', 'Archive not found');
      this._grantFor(source.owner_id, { grant_id: input.grant_id, to: input.to, content_type: 'history_sync' });
      this.db.prepare('INSERT INTO history_subscriptions(id,archive_id,target_id,grant_id) VALUES(?,?,?,?)').run(input.id, input.archive_id, input.to, input.grant_id);
      return { id: input.id, queued: this._fanoutIndexed(input.archive_id) };
    });
  }
  _fanoutIndexed(archiveId) {
    const archive = this.db.prepare('SELECT * FROM archives WHERE id=?').get(archiveId), queued = [];
    for (const subscription of this.db.prepare('SELECT * FROM history_subscriptions WHERE archive_id=? AND enabled=1').all(archiveId)) {
      try { this._grantFor(archive.owner_id, { to: subscription.target_id, content_type: 'history_sync', grant_id: subscription.grant_id }); }
      catch (error) {
        if (!(error instanceof BridgeError)) throw error;
        this.db.prepare('UPDATE history_subscriptions SET last_error=? WHERE id=?').run(error.code, subscription.id); continue;
      }
      const rows = this.db.prepare('SELECT r.* FROM indexed_records r WHERE r.archive_id=? AND r.generation=? AND NOT EXISTS(SELECT 1 FROM history_fanout f WHERE f.target_id=? AND f.archive_id=r.archive_id AND f.generation=r.generation AND f.seq=r.seq) ORDER BY r.seq').all(archiveId, archive.generation, subscription.target_id);
      let run = [];
      const emit = () => {
        if (!run.length) return;
        for (const packet of historyEnvelopes(run, { archive_id: archiveId, generation: archive.generation })) {
          const id = 'history-' + digest(canonical({ from: archive.owner_id, to: subscription.target_id, packet: packet.id }));
          const result = this._publishAs(archive.owner_id, { protocol: PROTOCOL, ...packet, id, to: subscription.target_id, grant_id: subscription.grant_id });
          if (!result.duplicate) queued.push(result.id);
        }
        for (const record of run) this.db.prepare('INSERT INTO history_fanout VALUES(?,?,?,?)').run(subscription.target_id, archiveId, archive.generation, record.seq);
        run = [];
      };
      for (const row of rows) { if (run.length && row.seq !== run.at(-1).seq + 1) emit(); run.push(parse(row.record)); }
      emit(); this.db.prepare('UPDATE history_subscriptions SET last_error=NULL WHERE id=?').run(subscription.id);
    }
    return queued;
  }
  commitIndexedPage(admin, archiveId, expectedCheckpoint, page) {
    this._admin(admin); object(page, 'page');
    return this._tx(() => {
      const source = this.sourceState(admin, archiveId), next = object(page.checkpoint, 'checkpoint');
      insist(canonical(source.checkpoint) === canonical(expectedCheckpoint ?? null), 'CHECKPOINT_CONFLICT', 'Another index operation already advanced this source');
      insist(next.archive_id === archiveId && next.generation === source.generation, 'SOURCE_MISMATCH', 'Page belongs to another source generation');
      const old = source.checkpoint ?? { byte_offset: 0, line_offset: 0, visible_next: 0 };
      for (const key of ['byte_offset', 'line_offset', 'visible_next']) integer(next[key], key, old[key]);
      insist(/^[a-f0-9]{64}$/.test(next.prefix_sha256), 'INVALID_INPUT', 'Missing checkpoint prefix checksum');
      insist(Array.isArray(page.records) && next.visible_next === old.visible_next + page.records.length, 'INVALID_INPUT', 'Page does not cover its visible sequence');
      for (let i = 0; i < page.records.length; i++) {
        const record = object(page.records[i], 'record');
        insist(record.seq === old.visible_next + i && ['user', 'assistant'].includes(record.role), 'INVALID_INPUT', 'Indexed records must be contiguous visible messages');
        text(record.text, 'record.text', 1024 * 1024);
        const ref = object(record.source_ref, 'source_ref');
        insist(ref.archive_id === archiveId && Number.isSafeInteger(ref.start_line) && ref.start_line > old.line_offset && ref.end_line === ref.start_line && ref.end_line <= next.line_offset && /^[a-f0-9]{64}$/.test(ref.sha256), 'SOURCE_MISMATCH', 'Indexed source reference is outside this page');
        const encoded = canonical(record), fingerprint = digest(encoded);
        const previous = this.db.prepare('SELECT fingerprint FROM indexed_records WHERE archive_id=? AND generation=? AND seq=?').get(archiveId, source.generation, record.seq);
        insist(!previous || previous.fingerprint === fingerprint, 'SOURCE_CONFLICT', 'Indexed original record changed');
        this.db.prepare('INSERT OR IGNORE INTO indexed_records VALUES(?,?,?,?,?)').run(archiveId, source.generation, record.seq, encoded, fingerprint);
      }
      const queued = this._fanoutIndexed(archiveId);
      this.db.prepare("UPDATE archive_sources SET checkpoint=?,status='ready',last_error=NULL,updated_at=? WHERE archive_id=?").run(canonical(next), this.now(), archiveId);
      return { archive_id: archiveId, indexed_records: page.records.length, queued, checkpoint: next };
    });
  }
  backfillHistory(admin, archiveId) { this._admin(admin); return this._tx(() => this._fanoutIndexed(archiveId)); }
  _receivesHistory(endpoint) { return parse(endpoint.settings).receive_history !== false; }
  _view(row) {
    return { id: row.id, from: row.from_id, to: row.to_id, queue_seq: row.queue_seq, content_type: row.content_type,
      payload: parse(row.payload), created_at: new Date(row.created_at).toISOString(), expires_at: row.expires_at === null ? null : new Date(row.expires_at).toISOString(),
      delivery: row.delivery, processing: row.processing, outcome: row.outcome, reason: row.reason };
  }
  status(connection, id) {
    const actor = this._actor(connection), row = this.db.prepare('SELECT * FROM messages WHERE id=? AND (from_id=? OR to_id=?)').get(id, actor.id, actor.id);
    insist(row, 'NOT_FOUND', 'Message not found');
    const attempts = this.db.prepare('SELECT o.id,o.state,o.wake,o.created_at,o.native_ref,o.error FROM offers o JOIN offer_items i ON i.offer_id=o.id WHERE i.message_id=? ORDER BY o.created_at,o.rowid').all(id);
    const view = this._view(row);
    if (row.to_id === actor.id && row.content_type === 'history_sync' && !this._receivesHistory(actor)) delete view.payload;
    return { ...view, attempts };
  }
  _settle(target, now) {
    const recipient = this._endpoint(target);
    for (const row of this.db.prepare("SELECT m.*,g.revoked FROM messages m JOIN grants g ON g.id=m.grant_id WHERE m.to_id=? AND delivery!='delivery_failed' AND processing!='handled'").all(target)) {
      let reason = row.revoked ? 'GRANT_REVOKED' : row.expires_at !== null && row.expires_at <= now && row.delivery !== 'received' ? 'EXPIRED' : null;
      const p = parse(row.payload);
      if (row.content_type === 'message' && p.kind === 'request' && !parse(recipient.work_domains).includes(p.work_domain)) reason = 'WORK_DOMAIN_MISMATCH';
      if (!reason) continue;
      if (row.delivery === 'received') this.db.prepare("UPDATE messages SET processing='handling',outcome='needs_input',reason=? WHERE id=?").run(reason, row.id);
      else this.db.prepare("UPDATE messages SET delivery='delivery_failed',reason=?,claim_hash=NULL,claim_until=NULL WHERE id=?").run(reason, row.id);
    }
  }
  historySourcesForOffer(target, rows) {
    const sources = new Map();
    const count = this.db.prepare(`SELECT count(*) AS n FROM messages WHERE ${unreadHistory}`);
    for (const row of rows) {
      if (row.content_type !== 'history_sync') continue;
      const { archive_id, generation } = parse(row.payload);
      if (!sources.has(archive_id)) sources.set(archive_id, { archive_id, unread_packets: count.get(target, archive_id, generation).n });
    }
    return [...sources.values()];
  }
  _receiveHistoryBatch(connection, input) {
    keys(input, ['archive_id', 'budgetBytes', 'request_id'], 'history receive');
    const { archive_id, budgetBytes = DEFAULT_HISTORY_BATCH_BYTES, request_id } = input;
    text(archive_id, 'archive_id', 256); integer(budgetBytes, 'budgetBytes', 1, MAX_HISTORY_BATCH_BYTES);
    const now = this.now(), actor = this._actor(connection, now);
    insist(this._receivesHistory(actor), 'NOT_AUTHORIZED', 'History receiving is disabled');
    const syncKey = request_id === undefined ? null : syncRequestKey(actor.id, request_id);
    if (syncKey) insist(!parse(actor.settings).paused, 'NOT_AUTHORIZED', 'History synchronization is paused');
    return this._tx(() => {
      const archive = this.db.prepare(`SELECT a.generation,s.status,s.checkpoint FROM archives a
        LEFT JOIN history_subscriptions h ON h.archive_id=a.id AND h.target_id=? AND h.enabled=1
        LEFT JOIN grants sub_grant ON sub_grant.id=h.grant_id AND sub_grant.revoked=0
        LEFT JOIN archive_sources s ON s.archive_id=a.id AND sub_grant.id IS NOT NULL
        WHERE a.id=? AND EXISTS (
          SELECT 1 FROM grants g WHERE g.from_id=a.owner_id AND g.to_id=? AND g.revoked=0
          AND EXISTS (SELECT 1 FROM json_each(g.content_types) WHERE value='history_sync'))`).get(actor.id, archive_id, actor.id);
      insist(archive, 'NOT_AUTHORIZED', 'No active history grant for this source');
      this._settle(actor.id, now);
      const checkpoint = archive.checkpoint && parse(archive.checkpoint);
      const prior = syncKey && syncOperation(this.db, syncKey);
      if (prior) {
        insist(prior.forget_at > now, 'CLAIM_EXPIRED', 'History synchronization request has expired');
        insist(prior.archive_id === archive_id && prior.generation === archive.generation && prior.budgetBytes === budgetBytes,
          'CALL_CONFLICT', 'History synchronization request parameters changed');
        insist(!prior.confirmed, 'SYNC_ALREADY_CONFIRMED', 'This history synchronization request was already confirmed');
        insist(prior.epoch === actor.epoch && prior.expires_at > now, 'CLAIM_EXPIRED', 'History synchronization request has expired');
        const rows = this.db.prepare("SELECT payload FROM messages WHERE to_id=? AND content_type='history_sync' AND claim_epoch=? AND claim_hash=? AND claim_until>? AND delivery IN ('queued','offered')")
          .all(actor.id, actor.epoch, historyClaimHash(prior.receipt), now);
        insist(rows.length === prior.packets, 'INVALID_CLAIM', 'The claimed history synchronization batch changed');
        return { ...prior.view, ...historyRuns(rows.map(row => parse(row.payload))), receipt: prior.receipt, expires_at: prior.expires_at };
      }
      if (syncKey) this.db.prepare("DELETE FROM metadata WHERE key GLOB 'history-sync:*' AND json_extract(value,'$.forget_at')<=?").run(now);
      const rows = this.db.prepare(`SELECT id,payload,claim_until FROM messages WHERE ${unreadHistory}
        ORDER BY json_extract(payload,'$.start_seq'), coalesce(json_extract(payload,'$.records[0].fragment.index'),0), queue_seq`)
        .all(actor.id, archive_id, archive.generation);
      const busy = rows.findIndex(row => row.claim_until > now);
      const available = busy < 0 ? rows.length : busy;
      const packets = rows.slice(0, available).map(row => parse(row.payload));
      const salt = token(), expires = now + this.historyClaimMs;
      const view = length => ({ archive_id, generation: archive.generation,
        ...historyRuns(packets.slice(0, length)), remaining_packets: rows.length - length,
        source_status: archive.status ?? 'manual', indexed_through: checkpoint ? checkpoint.visible_next - 1 : null,
        ...(busy >= 0 && length === available ? { blocked_until: rows[busy].claim_until } : {}),
        ...(length ? { expires_at: expires, receipt: `${length}.${salt}` } : {}),
      });
      // Each additional packet contributes at least one entry, so the serialized view grows with the prefix.
      let low = available ? 1 : 0, high = available, selected = 0, result;
      while (low <= high) {
        const length = Math.floor((low + high) / 2), candidate = view(length);
        if (Buffer.byteLength(JSON.stringify(candidate)) > budgetBytes) high = length - 1;
        else { selected = length; result = candidate; low = length + 1; }
      }
      insist(result, 'BUDGET_TOO_SMALL', 'The next history result exceeds the receive budget');
      if (selected) {
        const claim = this.db.prepare('UPDATE messages SET claim_epoch=?,claim_hash=?,claim_until=? WHERE id=?');
        const hash = historyClaimHash(result.receipt);
        for (const row of rows.slice(0, selected)) claim.run(actor.epoch, hash, expires, row.id);
        if (syncKey) {
          const { runs, range, receipt, expires_at, ...view } = result;
          saveSyncOperation(this.db, syncKey, { archive_id, generation: archive.generation, budgetBytes,
            epoch: actor.epoch, receipt, expires_at, packets: selected, view, forget_at: now + SYNC_REQUEST_TTL_MS });
        }
      }
      return result;
    });
  }
  receive(connection, input = {}) {
    if (input?.archive_id !== undefined) return this._receiveHistoryBatch(connection, input);
    const { limit = MAX_BATCH_COUNT, budgetBytes = MAX_BATCH_BYTES, ids } = input;
    integer(limit, 'limit', 1, MAX_BATCH_COUNT); integer(budgetBytes, 'budgetBytes', 1, MAX_BATCH_BYTES);
    if (ids !== undefined) insist(Array.isArray(ids) && ids.length > 0 && ids.length <= MAX_BATCH_COUNT && ids.every(id => typeof id === 'string' && id.length <= 256) && new Set(ids).size === ids.length, 'INVALID_INPUT', 'Invalid requested message IDs');
    const now = this.now(), actor = this._actor(connection, now);
    return this._tx(() => {
      this._settle(actor.id, now);
      const rows = this.db.prepare("SELECT * FROM messages WHERE to_id=? AND (? OR content_type!='history_sync') AND delivery!='delivery_failed' AND processing!='handled' AND (outcome IS NULL OR outcome='accepted') AND (claim_until IS NULL OR claim_until<=?) ORDER BY queue_seq")
        .all(actor.id, Number(this._receivesHistory(actor)), now);
      if (ids) for (const id of ids) insist(this.db.prepare('SELECT 1 FROM messages WHERE id=? AND to_id=?').get(id, actor.id), 'NOT_FOUND', 'Requested message not found');
      const items = []; let used = 0;
      for (const row of rows) {
        if (ids && !ids.includes(row.id)) continue;
        if (items.length >= limit) break;
        if (used + row.payload_bytes > budgetBytes) {
          insist(items.length > 0, 'BUDGET_TOO_SMALL', 'First pending message exceeds this receive budget'); break;
        }
        const claimToken = token();
        this.db.prepare('UPDATE messages SET claim_epoch=?,claim_hash=?,claim_until=? WHERE id=?').run(actor.epoch, digest(claimToken), now + this.leaseMs, row.id);
        items.push({ ...this._view(row), claim: { token: claimToken, epoch: actor.epoch, expires_at: now + this.leaseMs } }); used += row.payload_bytes;
      }
      return { items, payload_bytes: used, remaining: rows.length - items.length };
    });
  }
  acknowledge(connection, acknowledgements) {
    if (acknowledgements?.receipt !== undefined) return this._acknowledgeHistoryBatch(connection, acknowledgements);
    const now = this.now(), actor = this._actor(connection, now);
    insist(Array.isArray(acknowledgements) && acknowledgements.length > 0 && acknowledgements.length <= MAX_BATCH_COUNT, 'INVALID_INPUT', 'A bounded acknowledgement batch is required');
    const results = acknowledgements.map(ack => {
      try {
        return this._tx(() => {
          object(ack, 'ack'); text(ack.id, 'id', 256);
          const row = this.db.prepare('SELECT * FROM messages WHERE id=? AND to_id=?').get(ack.id, actor.id);
          insist(row, 'NOT_FOUND', 'Message not found');
          insist(row.claim_epoch === actor.epoch && same(ack.claim_token, row.claim_hash), 'INVALID_CLAIM', 'Message was not returned to this connection');
          insist(row.claim_until > now, 'CLAIM_EXPIRED', 'Claim has expired');
          insist(row.delivery !== 'delivery_failed', 'DELIVERY_FAILED', 'Message is not eligible for acknowledgement');
          this._settle(actor.id, now);
          const live = this.db.prepare('SELECT * FROM messages WHERE id=?').get(row.id);
          return this._acknowledgeRow(live, ack.outcome, now);
        });
      } catch (e) { if (!(e instanceof BridgeError)) throw e; return { id: ack?.id ?? null, ok: false, error: e.code }; }
    });
    return results;
  }
  _acknowledgeHistoryBatch(connection, input) {
    keys(input, ['receipt', 'request_id'], 'history acknowledgement');
    text(input.receipt, 'receipt', 64);
    const parts = /^([1-9]\d{0,5})\.[A-Za-z0-9_-]{43}$/.exec(input.receipt);
    insist(parts, 'INVALID_CLAIM', 'Invalid history receipt');
    const now = this.now(), actor = this._actor(connection, now);
    const syncKey = input.request_id === undefined ? null : syncRequestKey(actor.id, input.request_id);
    return this._tx(() => {
      const prior = syncKey && syncOperation(this.db, syncKey);
      if (syncKey) {
        insist(prior?.receipt === input.receipt, 'INVALID_CLAIM', 'History receipt does not match this synchronization request');
        insist(prior.forget_at > now, 'CLAIM_EXPIRED', 'History synchronization request has expired');
        if (prior.confirmed) return { ...prior.confirmed, duplicate: true };
        insist(this._receivesHistory(actor) && !parse(actor.settings).paused, 'NOT_AUTHORIZED', 'History synchronization is disabled or paused');
      }
      this._settle(actor.id, now);
      const rows = this.db.prepare("SELECT * FROM messages WHERE to_id=? AND content_type='history_sync' AND claim_epoch=? AND claim_hash=?")
        .all(actor.id, actor.epoch, historyClaimHash(input.receipt));
      insist(rows.length === Number(parts[1]), 'INVALID_CLAIM', 'The complete history batch is not claimed by this connection');
      insist(rows.every(row => row.claim_until > now), 'CLAIM_EXPIRED', 'History receipt has expired');
      const { archive_id, generation } = parse(rows[0].payload);
      const duplicate = rows.every(row => row.delivery === 'received');
      for (const row of rows) this._acknowledgeRow(row, undefined, now, false);
      const result = { archive_id, generation, acknowledged_packets: rows.length, read_through: this._advanceHistory(actor.id, archive_id, generation), duplicate };
      if (prior) saveSyncOperation(this.db, syncKey, { ...prior, confirmed: result, forget_at: now + SYNC_REQUEST_TTL_MS });
      return result;
    });
  }
  _acknowledgeRow(row, requestedOutcome, now, advanceHistory = true) {
    insist(row.delivery !== 'delivery_failed' && row.outcome !== 'needs_input', 'NOT_AUTHORIZED', 'Message is no longer eligible for processing');
    if (requestedOutcome !== undefined) insist(OUTCOMES.includes(requestedOutcome), 'INVALID_INPUT', 'Invalid processing outcome');
    if (row.processing === 'handled') {
      insist(requestedOutcome === undefined || requestedOutcome === row.outcome, 'TERMINAL_STATE', 'A terminal outcome cannot change');
      return { id: row.id, ok: true, duplicate: true, delivery: row.delivery, outcome: row.outcome };
    }
    const outcome = requestedOutcome ?? row.outcome;
    const processing = TERMINAL.includes(outcome) ? 'handled' : outcome ? 'handling' : row.processing;
    this.db.prepare("UPDATE messages SET delivery='received',received_at=COALESCE(received_at,?),processing=?,outcome=?,handled_at=? WHERE id=?")
      .run(now, processing, outcome ?? null, processing === 'handled' ? now : null, row.id);
    if (row.content_type === 'history_sync') {
      const p = parse(row.payload);
      const fragment = p.records[0].fragment;
      let complete = !fragment;
      if (fragment) {
        this.db.prepare('INSERT OR IGNORE INTO history_read_parts VALUES(?,?,?,?,?)').run(row.to_id, p.archive_id, p.generation, p.start_seq, fragment.index);
        const count = this.db.prepare('SELECT COUNT(*) AS n FROM history_read_parts WHERE target_id=? AND archive_id=? AND generation=? AND seq=?').get(row.to_id, p.archive_id, p.generation, p.start_seq).n;
        complete = count === fragment.count;
      }
      if (complete) this.db.prepare('INSERT OR IGNORE INTO history_received VALUES(?,?,?,?,?,?)').run(row.id, row.to_id, p.archive_id, p.generation, p.start_seq, p.end_seq);
      if (advanceHistory) this._advanceHistory(row.to_id, p.archive_id, p.generation);
    }
    // A valid receive acknowledgement resolves an earlier uncertain host call.
    for (const offer of this.db.prepare("SELECT o.* FROM offers o JOIN offer_items i ON i.offer_id=o.id WHERE i.message_id=? AND o.state IN ('in_flight','uncertain')").all(row.id)) {
      const unread = parse(offer.ids).some(id => this.db.prepare('SELECT delivery FROM messages WHERE id=?').get(id).delivery !== 'received');
      if (!unread) this.db.prepare("UPDATE offers SET state='confirmed',error=NULL WHERE id=?").run(offer.id);
    }
    return { id: row.id, ok: true, delivery: 'received', processing, outcome };
  }
  _advanceHistory(target, archive, generation) {
    let cursor = this.db.prepare('SELECT read_through FROM history_cursors WHERE target_id=? AND archive_id=? AND generation=?').get(target, archive, generation)?.read_through ?? -1;
    const ranges = this.db.prepare('SELECT start_seq,end_seq FROM history_received WHERE target_id=? AND archive_id=? AND generation=? ORDER BY start_seq,end_seq').all(target, archive, generation);
    for (const range of ranges) { if (range.start_seq > cursor + 1) break; cursor = Math.max(cursor, range.end_seq); }
    this.db.prepare('INSERT INTO history_cursors VALUES(?,?,?,?) ON CONFLICT(target_id,archive_id,generation) DO UPDATE SET read_through=excluded.read_through').run(target, archive, generation, cursor);
    return cursor;
  }
  historyCursor(connection, archive, generation) {
    const actor = this._actor(connection); text(archive, 'archive', 256); text(generation, 'generation', 256);
    return this.db.prepare('SELECT read_through FROM history_cursors WHERE target_id=? AND archive_id=? AND generation=?').get(actor.id, archive, generation)?.read_through ?? -1;
  }
  historySyncStatus(connection, archive, generation, requestId) {
    const actor = this._actor(connection);
    const read_through = this.historyCursor(connection, archive, generation);
    const prior = syncOperation(this.db, syncRequestKey(actor.id, requestId));
    if (!prior || prior.forget_at <= this.now() || prior.archive_id !== archive || prior.generation !== generation) {
      return { read_through, batch_state: 'unknown' };
    }
    if (prior.confirmed) return { ...prior.confirmed, read_through, batch_state: 'confirmed' };
    const batch_state = prior.epoch === actor.epoch && prior.expires_at > this.now() ? 'pending' : 'expired';
    return { read_through, batch_state };
  }
  historySourcesForSync(connection) {
    const now = this.now(), endpoint = this._actor(connection, now);
    if (!this._receivesHistory(endpoint) || parse(endpoint.settings).paused) return [];
    return this._tx(() => {
      this._settle(endpoint.id, now);
      return this.db.prepare(`SELECT a.id AS archive_id, count(*) AS unread_packets
        FROM messages m JOIN archives a ON a.id=json_extract(m.payload,'$.archive_id')
          AND a.generation=json_extract(m.payload,'$.generation')
        WHERE m.to_id=? AND m.content_type='history_sync' AND m.delivery IN ('queued','offered')
          AND m.processing!='handled' AND (m.outcome IS NULL OR m.outcome='accepted')
        GROUP BY a.id ORDER BY min(m.queue_seq) LIMIT 4`).all(endpoint.id);
    });
  }
  pending(target) {
    const now = this.now(); this._settle(target, now);
    return this.db.prepare("SELECT m.*,g.allow_wake FROM messages m JOIN grants g ON g.id=m.grant_id WHERE m.to_id=? AND (? OR m.content_type!='history_sync') AND delivery='queued' AND NOT EXISTS(SELECT 1 FROM offer_items i JOIN offers o ON o.id=i.offer_id WHERE i.message_id=m.id AND o.state IN ('in_flight','uncertain','offered','confirmed')) ORDER BY queue_seq")
      .all(target, Number(this._receivesHistory(this._endpoint(target))));
  }
  beginOffer(target, ids, wake) {
    const now = this.now(), endpoint = this._endpoint(target);
    insist(endpoint.connection_hash && endpoint.lease_until > now, 'OFFLINE', 'Endpoint is offline');
    insist(Array.isArray(ids) && ids.length > 0 && ids.length <= MAX_BATCH_COUNT && new Set(ids).size === ids.length, 'INVALID_INPUT', 'Invalid offer batch');
    insist(typeof wake === 'boolean', 'INVALID_INPUT', 'wake must be boolean');
    const id = randomUUID();
    this._tx(() => {
      const rows = this.pending(target), candidates = new Map(rows.map(m => [m.id, m]));
      insist(ids.every(value => candidates.has(value)), 'OFFER_CONFLICT', 'Message already offered or no longer deliverable');
      const policy = parse(endpoint.settings), caps = parse(endpoint.capabilities);
      insist(!policy.paused && caps.can_enqueue, 'CAPABILITY_MISSING', 'Endpoint cannot accept an offer');
      if (wake) insist(caps.can_wake && ids.some(value => candidates.get(value).allow_wake && policy[candidates.get(value).content_type] === 'idle_wake'), 'WAKE_NOT_AUTHORIZED', 'No message in this batch is permitted to wake this endpoint');
      this.db.prepare("INSERT INTO offers(id,target_id,epoch,state,ids,wake,created_at) VALUES(?,?,?,'in_flight',?,?,?)").run(id, target, endpoint.epoch, canonical(ids), Number(wake), now);
      for (const message of ids) this.db.prepare('INSERT INTO offer_items VALUES(?,?)').run(id, message);
    });
    return { id, target, epoch: endpoint.epoch, ids: [...ids], wake };
  }
  finishOffer(id, result) {
    return this._tx(() => {
      const row = this.db.prepare('SELECT * FROM offers WHERE id=?').get(id);
      insist(row, 'NOT_FOUND', 'Offer not found');
      if (row.state === 'confirmed') return { state: 'confirmed' };
      insist(row.state === 'in_flight', 'TERMINAL_STATE', 'Offer has already been resolved');
      const currentEpoch = this._endpoint(row.target_id).epoch;
      if (currentEpoch !== row.epoch) result = { error: 'Connection epoch changed during host call' };
      const accepted = result?.accepted === true, rejected = result?.accepted === false;
      const state = accepted ? 'offered' : rejected ? 'rejected' : 'uncertain';
      this.db.prepare('UPDATE offers SET state=?,native_ref=?,error=? WHERE id=?').run(state, result?.native_ref ?? null, accepted ? null : String(result?.error ?? 'Host response is unknown'), id);
      if (accepted) for (const message of parse(row.ids)) this.db.prepare("UPDATE messages SET delivery='offered' WHERE id=? AND delivery='queued'").run(message);
      return { state };
    });
  }
}

export { PROTOCOL, BridgeError };
