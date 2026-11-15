import test from 'node:test';
import assert from 'node:assert/strict';
import { DshGateway } from '../src/dsh-gateway.mjs';
import { DeliveryCoordinator } from '../src/delivery.mjs';
import { createBridgeServer } from '../src/server.mjs';
import { BridgeClient } from '../src/client.mjs';
import { fixture, ADMIN, message, history, ack, code } from './helpers.mjs';

const host = { endpoint_id: 'dsh-b', native_session_id: 'native-b', host_instance: 'isolated-host-a' };
function setup(t) {
  const f = fixture(), gateway = new DshGateway({ core: f.core, now: () => f.clock.now });
  const coordinator = new DeliveryCoordinator({ core: f.core, adapters: new Map([['dsh', gateway]]), auto: false });
  gateway.coordinator = coordinator;
  t.after(async () => { await coordinator.close(); gateway.close(); f.core.close(); });
  return { ...f, gateway, coordinator };
}

test('DSH host pulse and tools share identity, connection and live claims', async t => {
  const f = setup(t), g = f.gateway;
  await g.pulse(f.b.credential, { ...host, state: 'idle' });
  const call = (name, args = {}) => g.call(f.b.credential, { ...host, name, arguments: args });
  const first = await call('bridge_context');
  f.core.publish(f.ca, message({ id: 'read-one' }));
  const received = await call('bridge_receive');
  await g.pulse(f.b.credential, { ...host, state: 'busy' });
  assert.equal((await call('bridge_context')).endpoint.epoch, first.endpoint.epoch);
  await call('bridge_ack', { acknowledgements: received.items.map(i => ack(i)) });
  assert.equal(f.core.status(f.ca, 'read-one').delivery, 'received');
  assert.equal(f.core.status(f.ca, 'read-one').processing, 'unread');
  assert.ok(!JSON.stringify(first).includes(f.b.credential));
  await assert.rejects(g.call(f.b.credential, { ...host, native_session_id: 'other', name: 'bridge_context' }), code('CALLER_MISMATCH'));
  await assert.rejects(g.pulse(f.b.credential, { ...host, host_instance: 'competing-host', state: 'idle' }), code('HOST_BUSY'));
});

test('DSH HTTP natural turn carries both types; host acceptance and actual read remain separate', async t => {
  const f = setup(t), server = createBridgeServer({ core: f.core, adminToken: ADMIN, dsh: f.gateway, port: 0 });
  t.after(() => server.close()); const base = await server.listen(), client = new BridgeClient(base);
  const request = (path, body) => client.request('/api/dsh/' + path, { method: 'POST', credential: f.b.credential, body: { ...host, ...body } });
  assert.equal((await client.request('/api/health')).features.dsh_host, true);
  await request('pulse', { state: 'idle' });
  f.core.setCapabilities(ADMIN, f.b.id, { can_enqueue: true });
  f.core.publish(f.ca, history(0, 0, { id: 'history-one' }));
  f.core.publish(f.ca, message({ id: 'event-one' }));
  const response = await request('pulse', { state: 'busy', natural_turn: true }), command = response.command;
  assert.deepEqual(command.content_types.sort(), ['history_sync', 'message']); assert.equal(command.wake, false);
  assert.deepEqual(command.ids, ['history-one', 'event-one']);
  assert.equal((await request('pulse', { state: 'busy' })).command, null, 'Commands are handed to the host only once');
  await request('host-result', { command_id: command.command_id, accepted: true, native_ref: 'actual-native-message-id' });
  await f.coordinator.running.get(f.b.id);
  assert.equal(f.core.status(f.ca, 'event-one').delivery, 'offered');
  assert.equal(f.core.status(f.ca, 'event-one').processing, 'unread');
  const call = (name, args = {}) => request('tool-call', { name, arguments: args });
  const received = await call('bridge_receive', { ids: command.ids });
  await call('bridge_ack', { acknowledgements: [ack(received.items.find(i => i.id === 'event-one'))] });
  assert.equal((await call('bridge_history_cursor', { archive_id: 'archive-a', generation: 'g1' })).read_through, -1);
  await call('bridge_ack', { acknowledgements: [ack(received.items.find(i => i.id === 'history-one'))] });
  assert.equal((await call('bridge_history_cursor', { archive_id: 'archive-a', generation: 'g1' })).read_through, 0);
});

test('DSH channel timeouts do not resend and stale hosts cannot finish another connection', async t => {
  const f = setup(t), g = f.gateway;
  await g.pulse(f.b.credential, { ...host, state: 'idle' });
  f.core.setCapabilities(ADMIN, f.b.id, { can_enqueue: true, can_wake: true });
  f.core.publish(f.ca, message({ id: 'uncertain' }));
  const abort = new AbortController(), endpoint = f.core.endpoint(f.b.id);
  const work = g.offer({ endpoint, batch_id: 'batch-one', ids: ['uncertain'], content_types: ['message'], wake: true, signal: abort.signal });
  const command = (await g.pulse(f.b.credential, { ...host, state: 'idle' })).command;
  abort.abort(); assert.equal((await work).accepted, undefined);
  assert.equal((await g.pulse(f.b.credential, { ...host, state: 'idle' })).command, null);
  assert.throws(() => g.result(f.b.credential, { ...host, command_id: command.command_id, accepted: true, native_ref: 'late' }), code('STALE_CONNECTION'));
  const stale = g.sessions.get(f.b.id).connection;
  await g.pulse(f.b.credential, { ...host, state: 'offline' });
  assert.throws(() => f.core.sessionInfo(stale), code('STALE_CONNECTION'));
  assert.equal(g.state(f.core.endpoint(f.b.id)), 'offline');
});

test('DSH missing capabilities never offers, and grants can be revoked via admin HTTP', async t => {
  const f = setup(t), server = createBridgeServer({ core: f.core, adminToken: ADMIN, dsh: f.gateway, port: 0 });
  t.after(() => server.close()); const client = new BridgeClient(await server.listen(), { adminToken: ADMIN });
  await f.gateway.pulse(f.b.credential, { ...host, state: 'idle' });
  f.core.publish(f.ca, message({ id: 'never-wake' }));
  assert.equal((await f.coordinator.pump(f.b.id)).state, 'capability_missing');
  await assert.rejects(client.request('/api/grants/a-b/revoke', { method: 'POST', body: {} }), code('UNAUTHORIZED'));
  await client.request('/api/grants/a-b/revoke', { method: 'POST', body: {}, admin: true });
  assert.equal(f.core.pending(f.b.id).length, 0);
});
