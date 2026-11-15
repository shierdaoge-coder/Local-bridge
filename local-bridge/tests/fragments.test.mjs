import test from 'node:test';
import assert from 'node:assert/strict';
import { historyEnvelopes } from '../src/history.mjs';
import { PROTOCOL, MAX_PAYLOAD_BYTES, canonical } from '../src/protocol.mjs';
import { fixture, ack, code } from './helpers.mjs';

const encode = text => historyEnvelopes([{ seq: 0, role: 'assistant', text }], { archive_id: 'archive-a', generation: 'g1' });
const envelope = packet => ({ protocol: PROTOCOL, to: 'dsh-b', grant_id: 'a-b', ...packet });

test('large Chinese and emoji records fragment losslessly into bounded, deterministic packets', () => {
  const text = '波浪与自由表面🐉'.repeat(350), parts = encode(text);
  assert.ok(parts.length > 1); assert.ok(parts.every(p => Buffer.byteLength(canonical(p.payload)) <= MAX_PAYLOAD_BYTES));
  assert.equal(parts.map(p => p.payload.records[0].text).join(''), text);
  assert.deepEqual(encode(text).map(p => p.id), parts.map(p => p.id));
  assert.ok(parts.every(p => !p.payload.records[0].text.includes('\ufffd')));
});

test('a history record remains unread until every fragment has been acknowledged', t => {
  const f = fixture(); t.after(() => f.core.close()); const parts = encode('长消息🐉'.repeat(500));
  parts.forEach(p => f.core.publish(f.ca, envelope(p)));
  const all = []; for (;;) { const batch = f.core.receive(f.cb).items; if (!batch.length) break; all.push(...batch); }
  const missing = all[1];
  for (const item of all.filter(m => m.id !== missing.id)) { assert.equal(f.core.acknowledge(f.cb, [ack(item, { outcome: 'completed' })])[0].ok, true); }
  assert.equal(f.core.historyCursor(f.cb, 'archive-a', 'g1'), -1);
  f.core.acknowledge(f.cb, [ack(missing, { outcome: 'completed' })]);
  assert.equal(f.core.historyCursor(f.cb, 'archive-a', 'g1'), 0);
});

test('a corrupted source fragment cannot be committed as a complete original record', t => {
  const f = fixture(); t.after(() => f.core.close()); const parts = encode('Original. '.repeat(500));
  for (let i = 0; i < parts.length - 1; i++) f.core.publish(f.ca, envelope(parts[i]));
  const damaged = structuredClone(parts.at(-1)); damaged.payload.records[0].text += 'x';
  assert.throws(() => f.core.publish(f.ca, envelope(damaged)), code('SOURCE_CONFLICT'));
  assert.equal(f.core.publish(f.ca, envelope(parts.at(-1))).duplicate, false);
});

test('ordinary records batch without fragments while retaining visible sequence ranges', () => {
  const records = Array.from({ length: 5 }, (_, i) => ({ seq: i, role: 'user', text: String(i) }));
  const packets = historyEnvelopes(records, { archive_id: 'a', generation: 'g' });
  assert.equal(packets.length, 1); assert.equal(packets[0].payload.start_seq, 0); assert.equal(packets[0].payload.end_seq, 4);
  assert.ok(packets[0].payload.records.every(r => !r.fragment));
});
