import { createHash } from 'node:crypto';

export const PROTOCOL = 'local-bridge.session-transport/0.1';
export const CONTENT_TYPES = ['history_sync', 'message'];
export const DEFAULT_SETTINGS = Object.freeze({ history_sync: 'next_turn', message: 'idle_wake', paused: false });
export const DEFAULT_CAPABILITIES = Object.freeze({ can_enqueue: false, can_wake: false, can_confirm_read: false });
export const MAX_PAYLOAD_BYTES = 2048;
export const MAX_BATCH_BYTES = 8192;
export const MAX_BATCH_COUNT = 4;
export const DEFAULT_HISTORY_BATCH_BYTES = 16 * 1024;
export const MAX_HISTORY_BATCH_BYTES = 64 * 1024;
export const OUTCOMES = ['accepted', 'completed', 'declined', 'needs_input', 'needs_verification', 'failed', 'cancelled'];

export class BridgeError extends Error {
  constructor(code, message, details) { super(message); this.name = 'BridgeError'; this.code = code; this.details = details; }
}
export function insist(condition, code, message) { if (!condition) throw new BridgeError(code, message); }
export function object(value, name) { insist(value && typeof value === 'object' && !Array.isArray(value), 'INVALID_INPUT', `${name} must be an object`); return value; }
export function text(value, name, max = 1024) { insist(typeof value === 'string' && value.length > 0 && value.length <= max, 'INVALID_INPUT', `${name} must be a nonempty string of at most ${max} characters`); return value; }
export function integer(value, name, min = 0, max = Number.MAX_SAFE_INTEGER) { insist(Number.isSafeInteger(value) && value >= min && value <= max, 'INVALID_INPUT', `${name} is out of range`); return value; }
export function keys(value, allowed, name) { for (const key of Object.keys(value)) insist(allowed.includes(key), 'INVALID_INPUT', `${name}.${key} is unsupported`); }
export function choices(values, allowed, name, nonempty = true) {
  insist(Array.isArray(values) && (!nonempty || values.length > 0) && values.every(v => allowed.includes(v)) && new Set(values).size === values.length, 'INVALID_INPUT', `${name} has invalid choices`);
  return [...values];
}
export function canonical(value) {
  if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
  if (value && typeof value === 'object') return '{' + Object.keys(value).sort().filter(k => value[k] !== undefined).map(k => JSON.stringify(k) + ':' + canonical(value[k])).join(',') + '}';
  insist(value !== undefined && (typeof value !== 'number' || Number.isFinite(value)), 'INVALID_INPUT', 'Value is not JSON');
  return JSON.stringify(value);
}
export const digest = value => createHash('sha256').update(value).digest('hex');
export function settings(patch, previous = DEFAULT_SETTINGS) {
  object(patch, 'settings'); keys(patch, ['history_sync', 'message', 'paused', 'receive_history'], 'settings');
  const result = { ...previous, ...patch };
  for (const type of CONTENT_TYPES) insist(['next_turn', 'idle_wake'].includes(result[type]), 'INVALID_INPUT', `Invalid ${type} policy`);
  insist(typeof result.paused === 'boolean', 'INVALID_INPUT', 'paused must be boolean');
  insist(result.receive_history === undefined || typeof result.receive_history === 'boolean', 'INVALID_INPUT', 'receive_history must be boolean');
  return result;
}
export function capabilities(patch = {}) {
  object(patch, 'capabilities'); keys(patch, Object.keys(DEFAULT_CAPABILITIES), 'capabilities');
  const result = { ...DEFAULT_CAPABILITIES, ...patch };
  insist(Object.values(result).every(v => typeof v === 'boolean'), 'INVALID_INPUT', 'Capabilities must be boolean');
  return result;
}
function sourceRef(ref) {
  object(ref, 'source_ref'); keys(ref, ['archive_id', 'record_id', 'start_line', 'end_line', 'sha256'], 'source_ref');
  text(ref.archive_id, 'archive_id', 256);
  insist(ref.record_id !== undefined || ref.start_line !== undefined, 'INVALID_INPUT', 'A source record or line range is required');
  if (ref.record_id !== undefined) text(ref.record_id, 'record_id', 256);
  if (ref.start_line !== undefined) { integer(ref.start_line, 'start_line', 1); integer(ref.end_line, 'end_line', ref.start_line); }
  if (ref.sha256 !== undefined) insist(/^[a-f0-9]{64}$/.test(ref.sha256), 'INVALID_INPUT', 'Invalid source checksum');
}
export function validateEnvelope(input) {
  object(input, 'envelope'); keys(input, ['protocol', 'id', 'to', 'content_type', 'grant_id', 'payload', 'expires_at'], 'envelope');
  insist(input.protocol === PROTOCOL, 'UNSUPPORTED_PROTOCOL', 'Unsupported transport protocol');
  text(input.id, 'id', 256); text(input.to, 'to', 256); text(input.grant_id, 'grant_id', 256);
  insist(CONTENT_TYPES.includes(input.content_type), 'INVALID_INPUT', 'Unknown content type');
  const p = object(input.payload, 'payload');
  if (input.expires_at !== undefined) insist(typeof input.expires_at === 'string' && Number.isFinite(Date.parse(input.expires_at)), 'INVALID_INPUT', 'Invalid expiration');
  if (input.content_type === 'history_sync') {
    keys(p, ['archive_id', 'generation', 'start_seq', 'end_seq', 'records'], 'history_sync');
    text(p.archive_id, 'archive_id', 256); text(p.generation, 'generation', 256);
    integer(p.start_seq, 'start_seq'); integer(p.end_seq, 'end_seq', p.start_seq);
    insist(Array.isArray(p.records) && p.records.length === p.end_seq - p.start_seq + 1, 'INVALID_INPUT', 'History batch must cover its complete visible sequence range');
    p.records.forEach((r, i) => {
      object(r, 'record'); keys(r, ['seq', 'role', 'text', 'source_ref', 'fragment'], 'record');
      insist(r.seq === p.start_seq + i && ['user', 'assistant'].includes(r.role), 'INVALID_INPUT', 'History records must be ordered visible messages');
      text(r.text, 'record.text', 2048); if (r.source_ref !== undefined) sourceRef(r.source_ref);
      if (r.fragment !== undefined) {
        object(r.fragment, 'fragment'); keys(r.fragment, ['index', 'count', 'sha256'], 'fragment');
        integer(r.fragment.count, 'fragment.count', 2, 256); integer(r.fragment.index, 'fragment.index', 0, r.fragment.count - 1);
        insist(/^[a-f0-9]{64}$/.test(r.fragment.sha256), 'INVALID_INPUT', 'Invalid complete-record checksum');
        insist(p.records.length === 1, 'INVALID_INPUT', 'A fragmented record must have its own envelope');
      }
    });
  } else {
    keys(p, ['kind', 'topic', 'correlation_id', 'text', 'source_refs', 'work_domain', 'authority_ref', 'reply_to'], 'message');
    insist(['event', 'request', 'result', 'cancel'].includes(p.kind), 'INVALID_INPUT', 'Unknown message kind');
    text(p.topic, 'topic', 256); text(p.correlation_id, 'correlation_id', 256); text(p.text, 'message.text', 2048);
    insist(Array.isArray(p.source_refs) && p.source_refs.length <= 16, 'INVALID_INPUT', 'source_refs must be a bounded array');
    p.source_refs.forEach(sourceRef);
    if (p.work_domain !== undefined) insist(['text', 'engineering'].includes(p.work_domain), 'INVALID_INPUT', 'Invalid work domain');
    if (['request', 'cancel'].includes(p.kind)) {
      object(p.authority_ref, 'authority_ref'); keys(p.authority_ref, ['grant_id', 'source_ref'], 'authority_ref');
      insist(p.authority_ref.grant_id === input.grant_id, 'INVALID_INPUT', 'Authority must reference the granted direction');
      text(p.authority_ref.source_ref, 'authority source', 1024);
    }
    if (p.kind === 'request') insist(p.work_domain !== undefined, 'INVALID_INPUT', 'Requests must declare a work domain');
    if (['result', 'cancel'].includes(p.kind)) text(p.reply_to, 'reply_to', 256);
    else insist(p.reply_to === undefined, 'INVALID_INPUT', 'reply_to is supported for result and cancel only');
  }
  const encoded = canonical(input);
  const payloadBytes = Buffer.byteLength(canonical(p));
  insist(payloadBytes <= MAX_PAYLOAD_BYTES, 'PAYLOAD_TOO_LARGE', 'Payload exceeds the 2 KiB limit');
  return { envelope: JSON.parse(encoded), fingerprint: digest(encoded), payloadBytes };
}
