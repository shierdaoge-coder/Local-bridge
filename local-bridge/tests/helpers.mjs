import { randomUUID } from 'node:crypto';
import { TransportCore, PROTOCOL } from '../src/core.mjs';

export const ADMIN = 'fixture-admin-secret-which-is-long-enough';
export function fixture(options = {}) {
  const clock = { now: 10_000 };
  const core = new TransportCore({ adminToken: ADMIN, now: () => clock.now, ...options });
  const a = core.register(ADMIN, { id: 'codex-a', harness: 'codex', native_session_id: 'native-a', work_domains: ['text', 'engineering'] });
  const b = core.register(ADMIN, { id: 'dsh-b', harness: 'dsh', native_session_id: 'native-b', work_domains: ['text'] });
  core.register(ADMIN, { id: 'other', harness: 'dsh', native_session_id: 'other' });
  core.grant(ADMIN, { id: 'a-b', from: a.id, to: b.id, content_types: ['history_sync', 'message'], work_domains: ['text', 'engineering'], allow_wake: true, author_ref: 'fixture-user-instruction' });
  core.grant(ADMIN, { id: 'b-a', from: b.id, to: a.id, content_types: ['history_sync', 'message'], work_domains: ['text', 'engineering'], allow_wake: true, author_ref: 'fixture-user-instruction' });
  core.registerArchive(ADMIN, { id: 'archive-a', owner: a.id, generation: 'g1' });
  const ca = core.connect(a.id, a.credential), cb = core.connect(b.id, b.credential);
  return { core, clock, a, b, ca, cb };
}
export function message(patch = {}, payload = {}) {
  return { protocol: PROTOCOL, id: randomUUID(), to: 'dsh-b', content_type: 'message', grant_id: 'a-b',
    payload: { kind: 'event', topic: 'test.result', correlation_id: 'work-one', text: 'A committed result.', source_refs: [], ...payload }, ...patch };
}
export function history(start = 0, end = start, patch = {}) {
  return { protocol: PROTOCOL, id: randomUUID(), to: 'dsh-b', content_type: 'history_sync', grant_id: 'a-b',
    payload: { archive_id: 'archive-a', generation: 'g1', start_seq: start, end_seq: end,
      records: Array.from({ length: end - start + 1 }, (_, i) => ({ seq: start + i, role: (start + i) % 2 ? 'assistant' : 'user', text: `Visible record ${start + i}` })) }, ...patch };
}
export const ack = (item, extra = {}) => ({ id: item.id, claim_token: item.claim.token, ...extra });
export const code = expected => error => error.code === expected;
