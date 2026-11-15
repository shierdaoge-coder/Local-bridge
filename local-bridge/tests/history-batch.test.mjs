import test from 'node:test';
import assert from 'node:assert/strict';
import { historyEnvelopes } from '../src/history.mjs';
import { PROTOCOL } from '../src/protocol.mjs';
import { ADMIN, fixture, history, message, code } from './helpers.mjs';

const source = { archive_id: 'archive-a' };
const messages = batch => batch.runs.flatMap(run => run.messages);

test('one history batch preserves alternating roles and isolates the selected source and snapshot', t => {
  const f = fixture(), { core, ca, cb } = f;
  t.after(() => core.close());
  const roles = ['user', 'user', 'assistant', 'assistant', 'user', 'assistant'];
  const packets = roles.map((role, seq) => {
    const packet = history(seq);
    packet.payload.records[0] = { seq, role, text: '原文 ' + seq + ' 🐉\n第二行' };
    return packet;
  });
  for (const seq of [3, 0, 5, 1, 4, 2]) core.publish(ca, packets[seq]);
  core.registerArchive(ADMIN, { id: 'other-archive', owner: f.a.id, generation: 'g2' });
  const others = [0, 1, 2].map(seq => {
    const packet = history(seq);
    Object.assign(packet.payload, { archive_id: 'other-archive', generation: 'g2' });
    core.publish(ca, packet);
    return packet;
  });
  core.registerArchive(ADMIN, { id: 'unauthorized-archive', owner: 'other', generation: 'g1' });
  const business = message();
  core.publish(ca, business);

  assert.throws(() => core.receive(cb, { archive_id: 'unauthorized-archive' }), code('NOT_AUTHORIZED'));
  assert.throws(() => core.receive(cb, { ...source, ids: [packets[0].id] }), code('INVALID_INPUT'));
  core.setSettings(ADMIN, f.b.id, { receive_history: false });
  assert.throws(() => core.receive(cb, source), code('NOT_AUTHORIZED'));
  core.setSettings(ADMIN, f.b.id, { receive_history: true });
  const batch = core.receive(cb, source);
  assert.deepEqual(batch.range, [0, 5]);
  assert.deepEqual(batch.runs.map(run => [run.role, run.messages.length]), [['user', 2], ['assistant', 2], ['user', 1], ['assistant', 1]]);
  assert.deepEqual(messages(batch), packets.map(packet => {
    const { seq, text } = packet.payload.records[0];
    return [seq, text];
  }));
  assert.equal(batch.remaining_packets, 0);
  assert.equal(core.acknowledge(cb, [{ id: packets[0].id, claim_token: batch.receipt }])[0].error, 'INVALID_CLAIM');
  assert.ok(!JSON.stringify(batch).includes(packets[0].id));
  assert.equal(core.historyCursor(cb, source.archive_id, 'g1'), -1);

  core.publish(ca, history(6));
  const busy = core.receive(cb, source);
  assert.equal(busy.receipt, undefined);
  assert.equal(busy.remaining_packets, 7);
  assert.equal(busy.blocked_until, batch.expires_at);
  const confirmed = core.acknowledge(cb, { receipt: batch.receipt });
  assert.equal(confirmed.acknowledged_packets, 6);
  assert.equal(confirmed.read_through, 5);
  assert.equal(core.acknowledge(cb, { receipt: batch.receipt }).duplicate, true);
  assert.deepEqual(core.receive(cb, source).range, [6, 6]);
  const [middle] = core.receive(cb, { ids: [others[1].id] }).items;
  assert.ok(core.acknowledge(cb, [{ id: middle.id, claim_token: middle.claim.token }])[0].ok);
  const otherBatch = core.receive(cb, { archive_id: 'other-archive' });
  assert.deepEqual(otherBatch.range, [0, 2]);
  assert.deepEqual(otherBatch.runs.map(run => [run.role, run.messages[0][0]]), [['user', 0], ['user', 2]]);
  assert.equal(core.historyCursor(cb, 'other-archive', 'g2'), -1);
  assert.equal(core.receive(cb, { ids: [business.id] }).items[0].id, business.id);
});

test('history batch receipts outlast ordinary claims on a live connection', t => {
  const { core, ca, cb, clock } = fixture();
  t.after(() => core.close());
  const event = message();
  core.publish(ca, event);
  core.publish(ca, history());
  const [item] = core.receive(cb, { ids: [event.id] }).items;
  const batch = core.receive(cb, source);
  assert.equal(item.claim.expires_at - clock.now, 30_000);
  assert.equal(batch.expires_at - clock.now, 180_000);
  clock.now += 20_000;
  core.heartbeat(cb);
  clock.now += 20_000;
  assert.equal(core.acknowledge(cb, [{ id: item.id, claim_token: item.claim.token }])[0].error, 'CLAIM_EXPIRED');
  assert.equal(core.acknowledge(cb, { receipt: batch.receipt }).read_through, 0);
});

test('small budgets and expired receipts allow lossless fragment continuation without advancing early', t => {
  const f = fixture({ historyClaimMs: 45_000 }), { core, ca, cb } = f;
  t.after(() => core.close());
  const text = '长消息 🐉 "原文"\n'.repeat(420);
  for (const packet of historyEnvelopes([{ seq: 0, role: 'assistant', text }], { ...source, generation: 'g1' })) {
    core.publish(ca, { protocol: PROTOCOL, to: f.b.id, grant_id: 'a-b', ...packet });
  }
  assert.throws(() => core.receive(cb, { ...source, budgetBytes: 100 }), code('BUDGET_TOO_SMALL'));
  const budgetBytes = 2400;
  const first = core.receive(cb, { ...source, budgetBytes });
  assert.ok(Buffer.byteLength(JSON.stringify(first)) <= budgetBytes);
  assert.ok(first.remaining_packets > 0);
  assert.equal(messages(first)[0][2][0], 1);
  assert.equal(first.expires_at - f.clock.now, 45_000);
  f.clock.now += 20_000;
  core.heartbeat(cb);
  f.clock.now = first.expires_at;
  assert.throws(() => core.acknowledge(cb, { receipt: first.receipt }), code('CLAIM_EXPIRED'));
  const retry = core.receive(cb, { ...source, budgetBytes });
  assert.notEqual(retry.receipt, first.receipt);
  assert.deepEqual(retry.runs, first.runs);
  assert.equal(core.acknowledge(cb, { receipt: retry.receipt }).read_through, -1);
  const rest = core.receive(cb, source);
  assert.equal(rest.remaining_packets, 0);
  assert.equal([...messages(retry), ...messages(rest)].map(entry => entry[1]).join(''), text);
  assert.equal(core.acknowledge(cb, { receipt: rest.receipt }).read_through, 0);
});

test('a changed grant or endpoint epoch cannot confirm any part of a previously claimed batch', t => {
  const f = fixture(), { core, ca, cb } = f;
  t.after(() => core.close());
  core.grant(ADMIN, { id: 'additional-grant', from: f.a.id, to: f.b.id, content_types: ['history_sync'], allow_wake: false, author_ref: 'isolated-test' });
  const first = history(), second = history(1, 1, { grant_id: 'additional-grant' });
  core.publish(ca, first); core.publish(ca, second);
  const earlier = core.receive(cb, source);
  const current = core.connect(f.b.id, f.b.credential);
  assert.throws(() => core.acknowledge(current, { receipt: earlier.receipt }), code('INVALID_CLAIM'));
  const batch = core.receive(current, source);
  core.revoke(ADMIN, 'additional-grant');
  assert.throws(() => core.acknowledge(current, { receipt: batch.receipt }), code('INVALID_CLAIM'));
  assert.equal(core.status(current, first.id).delivery, 'queued');
  assert.equal(core.historyCursor(current, source.archive_id, 'g1'), -1);
});
