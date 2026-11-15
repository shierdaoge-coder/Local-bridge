import test from 'node:test';
import assert from 'node:assert/strict';
import { SessionToolBridge } from '../src/session-tools.mjs';
import { BridgeClient } from '../src/client.mjs';
import { createBridgeServer } from '../src/server.mjs';
import { dshHostContext, dshToolDefinitions } from '../src/adapters/dsh.mjs';
import { ADMIN, fixture, message, code } from './helpers.mjs';

async function setup(t) {
  const f = fixture(), service = createBridgeServer({ core: f.core, adminToken: ADMIN, port: 0 });
  const base = await service.listen(), client = new BridgeClient(base);
  const bridge = new SessionToolBridge({ client, endpointId: f.b.id, credential: f.b.credential, binding: { harness: 'dsh', native_session_id: f.b.native_session_id }, getHostContext: dshHostContext });
  t.after(async () => { await service.close(); f.core.close(); });
  return { ...f, client, bridge, exec: { agent: { session: { id: 'native-b' } } } };
}

test('session tools reject missing and different native callers before connecting or claiming', async t => {
  const f = await setup(t), epoch = f.core.endpoint('dsh-b').epoch;
  await assert.rejects(f.bridge.execute('bridge_receive', {}, {}), code('HOST_CONTEXT_UNVERIFIED'));
  await assert.rejects(f.bridge.execute('bridge_receive', {}, { agent: { session: { id: 'other-native' } } }), code('CALLER_MISMATCH'));
  assert.equal(f.core.endpoint('dsh-b').epoch, epoch);
  assert.equal(f.client.connection, undefined);
});

test('bound tools read real HTTP entries and keep acknowledgement separate from receipt', async t => {
  const f = await setup(t), sent = f.core.publish(f.ca, message());
  const context = await f.bridge.execute('bridge_context', {}, f.exec); assert.equal(context.endpoint.id, 'dsh-b');
  const batch = await f.bridge.execute('bridge_receive', { ids: [sent.id] }, f.exec);
  assert.equal(f.core.status(f.ca, sent.id).delivery, 'queued');
  const result = await f.bridge.execute('bridge_ack', { acknowledgements: [{ id: sent.id, claim_token: batch.items[0].claim.token, outcome: 'completed' }] }, f.exec);
  assert.equal(result[0].ok, true); assert.equal(f.core.status(f.ca, sent.id).outcome, 'completed');
});

test('model arguments cannot forge native identity, source, or gain administrator tools', async t => {
  const f = await setup(t);
  await assert.rejects(f.bridge.execute('bridge_receive', { native_session_id: 'native-b' }, f.exec), code('INVALID_INPUT'));
  await assert.rejects(f.bridge.execute('register_grant', {}, f.exec), code('NOT_FOUND'));
  const definition = f.bridge.definitions().find(x => x.name === 'bridge_publish');
  assert.equal(definition.inputSchema.additionalProperties, false);
  assert.ok(!Object.hasOwn(definition.inputSchema.properties, 'from'));
});

test('tool publishing uses the host-bound sender and restricts the channel to business messages', async t => {
  const f = await setup(t), value = message({ to: 'codex-a', grant_id: 'b-a' });
  const args = { id: value.id, to: value.to, grant_id: value.grant_id, payload: value.payload };
  const result = await f.bridge.execute('bridge_publish', args, f.exec);
  assert.equal(result.from, 'dsh-b'); assert.equal(result.content_type, 'message');
  assert.equal((await f.bridge.execute('bridge_status', { id: result.id }, f.exec)).id, result.id);
  assert.equal((await f.bridge.execute('bridge_history_cursor', { archive_id: 'archive-a', generation: 'g1' }, f.exec)).read_through, -1);
});

test('stale transport connections reconnect without accepting the old claim as current', async t => {
  const f = await setup(t), msg = f.core.publish(f.ca, message());
  const batch = await f.bridge.execute('bridge_receive', {}, f.exec);
  f.core.connect(f.b.id, f.b.credential);
  const result = await f.bridge.execute('bridge_ack', { acknowledgements: [{ id: msg.id, claim_token: batch.items[0].claim.token }] }, f.exec);
  assert.equal(result[0].ok, false); assert.equal(result[0].error, 'INVALID_CLAIM');
});

test('DSH native tool definitions route exclusively through the real execution session', async t => {
  const f = await setup(t), tools = dshToolDefinitions(new Map([['native-b', f.bridge]]));
  const context = tools.find(t => t.name === 'bridge_context');
  assert.equal(context.parameters.additionalProperties, false);
  assert.equal(JSON.parse(await context.execute({}, f.exec)).result.endpoint.id, 'dsh-b');
  const result = JSON.parse(await context.execute({}, { agent: { session: { id: 'unregistered' } } }));
  assert.equal(result.ok, false); assert.equal(result.error, 'NOT_REGISTERED');
  const missing = JSON.parse(await context.execute({}, {})); assert.equal(missing.error, 'HOST_CONTEXT_UNVERIFIED');
  assert.equal(context.output.render({}, 'result')[0].text, 'result');
});
