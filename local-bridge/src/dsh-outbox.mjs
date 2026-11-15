import { open } from 'node:fs/promises';
import { BridgeError, PROTOCOL, canonical, digest, insist, keys, object, validateEnvelope } from './protocol.mjs';

const transportErrors = new Map([
  ['ECONNREFUSED', 'The local bridge service refused the connection'],
  ['ECONNRESET', 'The local bridge connection was reset'],
  ['ETIMEDOUT', 'The local bridge request timed out'],
  ['ENETUNREACH', 'The local bridge network is unreachable'],
  ['EHOSTUNREACH', 'The local bridge host is unreachable'],
  ['EPIPE', 'The local bridge connection closed during the request'],
  ['UND_ERR_SOCKET', 'The local bridge connection closed before a complete response'],
  ['UND_ERR_CONNECT_TIMEOUT', 'The local bridge connection timed out'],
  ['UND_ERR_HEADERS_TIMEOUT', 'The local bridge response timed out'],
  ['UND_ERR_BODY_TIMEOUT', 'The local bridge response timed out'],
  ['BRIDGE_UNREACHABLE', 'The local bridge request could not reach the service'],
  ['INVALID_BRIDGE_RESPONSE', 'The local bridge did not return a verifiable response'],
]);
const retryable = new Set([...transportErrors.keys(), 'HOST_BUSY', 'STALE_CONNECTION', 'QUEUE_PRESSURE', 'INTERNAL_ERROR']);
const committedStates = new Set(['queued', 'offered', 'received', 'delivery_failed']);
const limit = 256;

export function dshToolError(error) {
  const ownCode = typeof error?.code === 'string' ? error.code : undefined;
  let code = ownCode ?? error?.cause?.code;
  if (!code && (error?.name === 'TimeoutError' || error?.name === 'AbortError')) code = 'ETIMEDOUT';
  if (!code && error?.name === 'SyntaxError') code = 'INVALID_BRIDGE_RESPONSE';
  if (!code && error?.name === 'TypeError' && error.message === 'fetch failed') code = 'BRIDGE_UNREACHABLE';
  if (typeof code !== 'string' || !/^[A-Z0-9_]+$/.test(code)) return { code: 'TOOL_FAILED', message: 'Bridge tool failed' };
  return { code, message: transportErrors.get(code) ?? (ownCode === code && typeof error.message === 'string' ? error.message : 'Local bridge request failed') };
}

export function dshOutboxScope(service, sessionId, binding) {
  return digest(canonical({ service, sessionId, endpoint: binding.endpoint_id, credential: digest(binding.credential) }));
}

/** Persist the validated invocation before the first network attempt. */
export class DshOutbox {
  static async open(file, { now = Date.now } = {}) {
    const { DatabaseSync } = await import('node:sqlite');
    const handle = await open(file, 'a', 0o600);
    await handle.close();
    return new DshOutbox(new DatabaseSync(file), now);
  }

  constructor(db, now) {
    this.now = now;
    this.inFlight = new Map();
    this.db = db;
    try {
      this.db.exec(`PRAGMA busy_timeout=1000;
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=FULL;
        CREATE TABLE IF NOT EXISTS outbox(
          scope TEXT NOT NULL, id TEXT NOT NULL, args TEXT NOT NULL,
          created_at INTEGER NOT NULL, expires_at INTEGER,
          attempts INTEGER NOT NULL DEFAULT 0, next_attempt_at INTEGER NOT NULL,
          error TEXT, failed INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY(scope,id)
        );`);
    } catch (error) { this.db.close(); throw error; }
  }

  close() { this.db.close(); }

  row(scope, id) { return this.db.prepare('SELECT * FROM outbox WHERE scope=? AND id=?').get(scope, id); }

  status(scope, id) {
    const row = this.row(scope, id);
    if (!row) return null;
    return { id, delivery: row.failed ? 'local_failed' : 'pending_local', stored_locally: true,
      attempts: row.attempts, error: row.error, next_attempt_at: row.failed ? null : new Date(row.next_attempt_at).toISOString() };
  }

  enqueue(scope, args) {
    object(args, 'publish arguments');
    keys(args, ['id', 'to', 'grant_id', 'payload', 'expires_at'], 'publish arguments');
    const { envelope } = validateEnvelope({ ...args, protocol: PROTOCOL, content_type: 'message' });
    const { protocol, content_type, ...publishArgs } = envelope;
    const serialized = canonical(publishArgs), now = this.now();
    const expires = envelope.expires_at === undefined ? null : Date.parse(envelope.expires_at);
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const current = this.row(scope, envelope.id);
      if (current) insist(current.args === serialized, 'ID_CONFLICT', 'A locally stored message ID cannot be reused with different content');
      else {
        insist(expires === null || expires > now, 'EXPIRED', 'Message has already expired');
        insist(this.db.prepare('SELECT count(*) AS n FROM outbox').get().n < limit, 'OUTBOX_FULL', 'The local DSH outbox is full');
        this.db.prepare('INSERT INTO outbox(scope,id,args,created_at,expires_at,next_attempt_at) VALUES(?,?,?,?,?,?)')
          .run(scope, envelope.id, serialized, now, expires, now);
      }
      this.db.exec('COMMIT');
    } catch (error) { this.db.exec('ROLLBACK'); throw error; }
    return envelope.id;
  }

  async publish(scope, args, dispatch) {
    const id = this.enqueue(scope, args);
    return this.send(scope, id, dispatch);
  }

  send(scope, id, dispatch) {
    const key = canonical([scope, id]);
    if (!this.inFlight.has(key)) {
      const work = this.attempt(scope, id, dispatch).finally(() => this.inFlight.delete(key));
      this.inFlight.set(key, work);
    }
    return this.inFlight.get(key);
  }

  async attempt(scope, id, dispatch) {
    const row = this.row(scope, id);
    if (!row) return null;
    insist(!row.failed, row.error ?? 'OUTBOX_FAILED', 'The locally stored message was rejected');
    if (row.expires_at !== null && row.expires_at <= this.now()) {
      this.db.prepare("UPDATE outbox SET failed=1,error='EXPIRED' WHERE scope=? AND id=?").run(scope, id);
      throw new BridgeError('EXPIRED', 'The locally stored message has expired');
    }
    if (row.next_attempt_at > this.now()) return this.status(scope, id);
    const args = JSON.parse(row.args);
    let result;
    try {
      result = await dispatch(args);
      insist(result?.id === id && result.to === args.to && result.content_type === 'message' && committedStates.has(result.delivery)
        && typeof result.duplicate === 'boolean' && result.payload && canonical(result.payload) === canonical(args.payload),
        'INVALID_BRIDGE_RESPONSE', 'Missing a matching durable publish receipt');
    } catch (error) {
      const failure = dshToolError(error), attempts = row.attempts + 1;
      const retry = retryable.has(failure.code);
      const next = this.now() + Math.min(30_000, 1000 * 2 ** Math.min(attempts - 1, 5));
      this.db.prepare('UPDATE outbox SET attempts=?,next_attempt_at=?,error=?,failed=? WHERE scope=? AND id=? AND args=?')
        .run(attempts, next, failure.code, retry ? 0 : 1, scope, id, row.args);
      if (retry) return this.status(scope, id);
      throw new BridgeError(failure.code, failure.message);
    }
    this.db.prepare('DELETE FROM outbox WHERE scope=? AND id=? AND args=?').run(scope, id, row.args);
    return result;
  }

  async flush(scope, dispatch) {
    const rows = this.db.prepare('SELECT id FROM outbox WHERE scope=? AND failed=0 AND (next_attempt_at<=? OR expires_at<=?) ORDER BY created_at,rowid LIMIT 4')
      .all(scope, this.now(), this.now());
    for (const { id } of rows) {
      try {
        const result = await this.send(scope, id, dispatch);
        if (result?.delivery === 'pending_local') break;
      } catch (error) { if (!this.row(scope, id)?.failed) throw error; }
    }
  }
}
