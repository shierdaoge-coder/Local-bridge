import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname, basename, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import { apply } from '../src/dsh-plugin.mjs';
import { DshGateway } from '../src/dsh-gateway.mjs';
import { DeliveryCoordinator } from '../src/delivery.mjs';
import { createBridgeServer } from '../src/server.mjs';
import { fixture, ADMIN, message, history, ack } from './helpers.mjs';

test('loaded DSH plugin admits a shared notification into the current native pre-step without extra injection', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'local-bridge-dsh-chain-'));
  const f = fixture(), gateway = new DshGateway({ core: f.core }), coordinator = new DeliveryCoordinator({ core: f.core, adapters: new Map([['dsh', gateway]]), auto: false });
  gateway.coordinator = coordinator;
  const service = createBridgeServer({ core: f.core, adminToken: ADMIN, dsh: gateway, port: 0 }), base = await service.listen();
  let unload = async () => {};
  t.after(async () => {
    await unload(); await coordinator.close(); gateway.close(); await service.close(); f.core.close();
    assert.equal(dirname(resolve(dir)), resolve(tmpdir())); assert.ok(basename(dir).startsWith('local-bridge-dsh-chain-')); await rm(dir, { recursive: true, force: true });
  });
  const bindingFile = join(dir, 'bridge.json');
  await writeFile(bindingFile, JSON.stringify({ service_url: base, bindings: { 'native-b': { endpoint_id: f.b.id, credential: f.b.credential } } }));
  const tools = new Map(), listeners = new Map(); let injected = 0, woke = 0, validated = 0;
  const agent = { session: { id: 'native-b', log: [] }, status: 'idle',
    inject: () => { injected++; }, followup: () => { woke++; },
    ctx: { on: (event, listener) => { listeners.set(event, listener); return () => listeners.delete(event); } } };
  const native = {
    createUserMessage: input => Object.freeze({ role: 'user', id: randomUUID(), ...input }),
    adoptSessionEvent: event => { assert.equal(event.type, 'user/message'); assert.equal(event.data.role, 'user'); assert.ok(event.data.id); validated++; return event; },
  };
  unload = await apply({ agents: new Map([['native-b', agent]]), tools: { register: tool => { tools.set(tool.name, tool); return () => tools.delete(tool.name); } } }, { binding_file: bindingFile, loadNative: async () => native });
  for (let i = 0; i < 30 && !f.core.endpoint(f.b.id).online; i++) await new Promise(resolve => setTimeout(resolve, 10));
  assert.equal(f.core.endpoint(f.b.id).online, true); assert.equal(woke, 0); assert.equal(injected, 0);
  f.core.setCapabilities(ADMIN, f.b.id, { can_enqueue: true });
  f.core.publish(f.ca, history(0, 0, { id: 'shared-history' }));
  f.core.publish(f.ca, message({ id: 'shared-message' }));
  agent.status = 'running';
  const original = { role: 'user', id: 'author-message', source: { kind: 'user' }, content: [{ type: 'text', text: 'natural conversation' }] };
  const decision = await listeners.get('agent/pre-step')({ signal: new AbortController().signal }, async () => ({ kind: 'enter', messages: [original] }));
  await coordinator.running.get(f.b.id);
  assert.equal(decision.messages.length, 2); assert.equal(decision.messages[0], original);
  const notice = decision.messages[1]; assert.equal(notice.source.kind, 'local-bridge'); assert.equal(validated, 1);
  assert.match(notice.content[0].text, /shared-history/); assert.match(notice.content[0].text, /shared-message/);
  assert.equal(injected, 0, 'Natural notices must not leave work in the next-step inbox'); assert.equal(woke, 0);
  assert.equal(f.core.status(f.ca, 'shared-message').delivery, 'offered');
  assert.equal(f.core.status(f.ca, 'shared-message').processing, 'unread');
  const execute = async (name, args = {}) => {
    const response = JSON.parse(await tools.get(name).execute(args, { agent })); assert.equal(response.ok, true); return response.result;
  };
  const received = await execute('bridge_receive', { ids: ['shared-history', 'shared-message'] });
  await execute('bridge_ack', { acknowledgements: received.items.map(item => ack(item)) });
  assert.equal((await execute('bridge_history_cursor', { archive_id: 'archive-a', generation: 'g1' })).read_through, 0);
  await unload(); assert.equal(f.core.endpoint(f.b.id).online, false); assert.equal(tools.size, 0); assert.equal(listeners.size, 0);
});
