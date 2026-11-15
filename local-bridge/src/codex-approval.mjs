import { insist, object, keys, text } from './protocol.mjs';

const PREFIX = 'local-bridge-approval-v1.';
const secret = value => typeof value === 'string' && /^[A-Za-z0-9_-]{43}$/.test(value);

function validate(value) {
  object(value, 'native approval proof');
  keys(value, ['endpoint_id', 'native_session_id', 'proof'], 'native approval proof');
  text(value.endpoint_id, 'endpoint_id', 256); text(value.native_session_id, 'native_session_id', 256);
  insist(secret(value.proof), 'HOST_CONTEXT_UNVERIFIED', 'Invalid native approval proof');
  return value;
}

// Routing hints select private local configuration; they never establish identity.
export function encodeApprovalProof(value) {
  return PREFIX + Buffer.from(JSON.stringify(validate(value))).toString('base64url');
}
export function decodeApprovalProof(value) {
  insist(typeof value === 'string' && value.startsWith(PREFIX) && value.length <= 2048,
    'HOST_CONTEXT_UNVERIFIED', 'A native hook approval proof is required');
  let decoded;
  try { decoded = JSON.parse(Buffer.from(value.slice(PREFIX.length), 'base64url').toString('utf8')); }
  catch { insist(false, 'HOST_CONTEXT_UNVERIFIED', 'Invalid native approval proof'); }
  return validate(decoded);
}
export const isLegacyTicket = secret;
