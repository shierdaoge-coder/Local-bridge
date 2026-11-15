import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { basename, dirname, join, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { TransportCore } from '../src/core.mjs';
import { DeliveryCoordinator } from '../src/delivery.mjs';
import { ADMIN, fixture, history, message, ack, code } from './helpers.mjs';

function delivery(t, options = {}) {
  const f = fixture(), calls = [], state = { value: 'idle' };
  const adapter = { state: async () => state.value, offer: async request => { calls.push(request); return { accepted: true, native_ref: 'fixture-inbox' }; }, ...options.adapter };
  const pump = new DeliveryCoordinator({ core: f.core, adapters: new Map([['dsh', adapter]]), auto: false, timeoutMs: 25 });
  t.after(async () => { await pump.close(); f.core.close(); });
  return { ...f, calls, state, adapter, pump };
}
const enable = f => f.core.setCapabilities(ADMIN, 'dsh-b', { can_enqueue: true, can_wake: true, can_confirm_read: true });

test('capability defaults prevent a simulated adapter from being treated as a verified host', async t => {
  const f = delivery(t); f.core.publish(f.ca, message());
  assert.equal((await f.pump.pump('dsh-b')).state, 'capability_missing'); assert.equal(f.calls.length, 0);
});

test('history and communication enter the same native batch; offer is distinct from model acknowledgement', async t => {
  const f = delivery(t); enable(f); const h = f.core.publish(f.ca, history()), m = f.core.publish(f.ca, message());
  assert.equal((await f.pump.pump('dsh-b')).state, 'offered');
  assert.equal(f.calls.length, 1); assert.deepEqual(f.calls[0].ids, [h.id, m.id]);
  assert.deepEqual(f.calls[0].content_types, ['history_sync', 'message']); assert.equal(f.calls[0].wake, true);
  assert.equal(f.calls[0].source.kind, 'local-bridge');
  assert.equal(f.core.status(f.cb, h.id).delivery, 'offered'); assert.equal(f.core.historyCursor(f.cb, 'archive-a', 'g1'), -1);
  await f.pump.pump('dsh-b'); assert.equal(f.calls.length, 1, 'Host acceptance must not be offered again while awaiting receipt');
  f.core.acknowledge(f.cb, f.core.receive(f.cb).items.map(m => ack(m, { outcome: 'completed' })));
  assert.equal(f.core.historyCursor(f.cb, 'archive-a', 'g1'), 0);
});

test('default history waits for a turn and never independently wakes the host', async t => {
  const f = delivery(t); enable(f); f.core.publish(f.ca, history());
  assert.equal((await f.pump.pump('dsh-b')).state, 'waiting_turn'); assert.equal(f.calls.length, 0);
  assert.equal((await f.pump.pump('dsh-b', { naturalTurn: true })).state, 'offered'); assert.equal(f.calls[0].wake, false);
});

test('busy hosts queue events; pause and each content policy are respected', async t => {
  const f = delivery(t); enable(f); f.core.publish(f.ca, message()); f.state.value = 'busy';
  assert.equal((await f.pump.pump('dsh-b')).state, 'busy'); assert.equal(f.calls.length, 0);
  f.state.value = 'idle'; f.core.setSettings(ADMIN, 'dsh-b', { paused: true });
  assert.equal((await f.pump.pump('dsh-b')).state, 'paused');
  f.core.setSettings(ADMIN, 'dsh-b', { paused: false, message: 'next_turn' });
  assert.equal((await f.pump.pump('dsh-b')).state, 'waiting_turn');
  await f.pump.pump('dsh-b', { naturalTurn: true }); assert.equal(f.calls.length, 1); assert.equal(f.calls[0].wake, false);
});

test('a settings change cannot invent a wake permission absent from the operator grant', async t => {
  const f = delivery(t); enable(f);
  f.core.grant(ADMIN, { id: 'no-wake', from: 'codex-a', to: 'dsh-b', content_types: ['message'], allow_wake: false, author_ref: 'fixture-user-instruction' });
  const msg = message({ grant_id: 'no-wake' }); f.core.publish(f.ca, msg);
  assert.equal((await f.pump.pump('dsh-b')).state, 'waiting_turn');
  assert.throws(() => f.core.beginOffer('dsh-b', [msg.id], true), code('WAKE_NOT_AUTHORIZED'));
  assert.equal(f.calls.length, 0);
});

test('turning off history reading keeps its backlog while communication is offered and read', async t => {
  const f = delivery(t);
  enable(f);
  f.core.setSettings(ADMIN, f.b.id, { receive_history: false });
  const h = f.core.publish(f.ca, history());
  const m = f.core.publish(f.ca, message());

  await f.pump.pump(f.b.id, { naturalTurn: true });
  assert.deepEqual(f.calls[0].ids, [m.id]);
  assert.equal(f.core.status(f.cb, h.id).payload, undefined);
  assert.equal(f.core.status(f.ca, h.id).delivery, 'queued');
  const batch = f.core.receive(f.cb, { ids: [h.id, m.id] });
  assert.deepEqual(batch.items.map(item => item.id), [m.id]);
  f.core.acknowledge(f.cb, batch.items.map(item => ack(item, { outcome: 'completed' })));
  assert.equal(f.core.historyCursor(f.cb, 'archive-a', 'g1'), -1);

  f.core.setSettings(ADMIN, f.b.id, { receive_history: true });
  await f.pump.pump(f.b.id, { naturalTurn: true });
  assert.deepEqual(f.calls[1].ids, [h.id]);
  const [original] = f.core.receive(f.cb).items;
  assert.equal(original.id, h.id);
  f.core.acknowledge(f.cb, [ack(original)]);
  assert.equal(f.core.historyCursor(f.cb, 'archive-a', 'g1'), 0);
});

test('a timed-out host call remains uncertain and is not blindly retried', async t => {
  const f = delivery(t, { adapter: { offer: async () => new Promise(() => {}) } }); enable(f);
  const msg = f.core.publish(f.ca, message());
  assert.equal((await f.pump.pump('dsh-b')).state, 'uncertain');
  assert.equal(f.core.status(f.ca, msg.id).delivery, 'queued');
  assert.equal(f.core.status(f.ca, msg.id).attempts[0].state, 'uncertain');
  assert.equal((await f.pump.pump('dsh-b')).state, 'empty');
  f.core.acknowledge(f.cb, f.core.receive(f.cb).items.map(m => ack(m)));
  assert.equal(f.core.status(f.ca, msg.id).attempts[0].state, 'confirmed');
});

test('concurrent delivery triggers share one attempt; endpoint epoch changes cannot claim current acceptance', async t => {
  let finish; const f = delivery(t, { adapter: { offer: request => { f.calls.push(request); return new Promise(resolve => { finish = resolve; }); } } }); enable(f);
  const msg = f.core.publish(f.ca, message());
  const first = f.pump.pump('dsh-b'), second = f.pump.pump('dsh-b'); assert.equal(first, second);
  await new Promise(resolve => setImmediate(resolve)); assert.equal(f.calls.length, 1);
  f.core.connect(f.b.id, f.b.credential); finish({ accepted: true, native_ref: 'old-epoch' });
  assert.equal((await first).state, 'uncertain'); assert.equal(f.core.status(f.ca, msg.id).delivery, 'queued');
});

test('restart converts interrupted host attempts to uncertain without repeating them', t => {
  const directory = mkdtempSync(join(tmpdir(), 'local-bridge-offer-')), database = join(directory, 'bridge.sqlite');
  const f = fixture({ database }); let restarted;
  t.after(() => { restarted?.close(); f.core.close(); assert.equal(dirname(directory), resolve(tmpdir())); assert.ok(basename(directory).startsWith('local-bridge-offer-')); rmSync(directory, { recursive: true, force: true }); });
  enable(f); const msg = f.core.publish(f.ca, message()); f.core.beginOffer('dsh-b', [msg.id], true); f.core.close();
  restarted = new TransportCore({ database, adminToken: ADMIN, now: () => f.clock.now });
  const actor = restarted.connect(f.a.id, f.a.credential); restarted.connect(f.b.id, f.b.credential);
  assert.equal(restarted.status(actor, msg.id).attempts[0].state, 'uncertain'); assert.equal(restarted.pending('dsh-b').length, 0);
});

test('deferred history backlog cannot block a permitted communication wake', async t => {
  const f = delivery(t); enable(f);
  for (let i = 0; i < 6; i++) f.core.publish(f.ca, history(i, i));
  const urgent = f.core.publish(f.ca, message());
  assert.equal((await f.pump.pump('dsh-b')).state, 'offered');
  assert.ok(f.calls[0].ids.includes(urgent.id)); assert.equal(f.calls[0].ids.length, 4);
  const selected = f.core.receive(f.cb, { ids: f.calls[0].ids }).items;
  assert.ok(selected.some(m => m.id === urgent.id)); assert.equal(selected.length, 4);
  const info = selected.find(m => m.id === urgent.id); f.core.acknowledge(f.cb, [ack(info, { outcome: 'completed' })]);
  assert.equal(f.core.historyCursor(f.cb, 'archive-a', 'g1'), -1);
});

test('a hanging status probe is bounded and never starts a host offer', async t => {
  const f = delivery(t, { adapter: { state: () => new Promise(() => {}) } }); enable(f);
  const msg = f.core.publish(f.ca, message());
  assert.equal((await f.pump.pump('dsh-b')).state, 'state_unknown');
  assert.equal(f.core.status(f.ca, msg.id).attempts.length, 0);
});
