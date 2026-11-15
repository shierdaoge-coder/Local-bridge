import test from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
import { mkdtempSync, writeFileSync, readFileSync, rmSync } from 'node:fs';
import { zstdCompressSync, zstdDecompressSync } from 'node:zlib';
import { join, dirname, basename, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { DshDeliveryAdapter } from '../src/adapters/dsh.mjs';
import { dshHostContext } from '../src/adapters/dsh.mjs';
import { DeliveryCoordinator } from '../src/delivery.mjs';
import { SourceIndexer } from '../src/indexer.mjs';
import { createBridgeServer } from '../src/server.mjs';
import { BridgeClient } from '../src/client.mjs';
import { SessionToolBridge } from '../src/session-tools.mjs';
import { McpToolSession, MCP_VERSION } from '../src/mcp.mjs';
import { ADMIN, fixture, message } from './helpers.mjs';

// Electron can read app.asar directly. Cached Node may use an explicit copy of
// the installed pure-JavaScript SDK, leaving default tests free of native loading.
const nativeTest = process.env.DSH_NATIVE_PERSISTENCE_ENTRY ? test : test.skip;
let native;
async function sdk() {
  if (native) return native;
  const base = process.env.DSH_NATIVE_PERSISTENCE_ENTRY;
  const requireNative = createRequire(pathToFileURL(base));
  const load = async name => import(pathToFileURL(requireNative.resolve(name)).href);
  native = { ...await import(pathToFileURL(base).href), ...await load('@deepseek-ai/dsh-session'), ...await load('@deepseek-ai/dsh-llm'), ...await load('@deepseek-ai/dsh-session-format-catalog/message-projections') };
  return native;
}
async function setup(sessionId = 'bridge-isolated-native-session', endpointId = 'isolated-dsh') {
  const api = await sdk(), session = api.Session.create(sessionId, [], undefined, 0, api.currentSessionMessageProjections);
  const calls = [], pending = { 'next-step': [], 'next-turn': [] };
  // Model-free test host. Native factory, event append, storage validation and replay are genuine.
  const agent = { session, status: 'idle' };
  for (const [method, target] of [['followup', 'next-turn'], ['inject', 'next-step']]) agent[method] = message => {
    calls.push(method); session.append('agent/inbox/spliced', { target, start: pending[target].length, inserted: [message] }); pending[target].push(message);
  };
  const endpoint = { id: endpointId, harness: 'dsh', native_session_id: session.id };
  const adapter = new DshDeliveryAdapter({ agents: new Map([[session.id, agent]]), createUserMessage: api.createUserMessage, adoptSessionEvent: api.adoptSessionEvent });
  const offer = { endpoint, batch_id: 'batch-native-1', ids: ['transport-one'], content_types: ['history_sync', 'message'], wake: true };
  return { api, session, agent, adapter, calls, pending, endpoint, offer };
}

nativeTest('native DSH notification persists and replays both inbox and admitted user messages', async t => {
  const f = await setup(), directory = mkdtempSync(join(tmpdir(), 'local-bridge-native-dsh-'));
  t.after(() => { assert.equal(dirname(directory), resolve(tmpdir())); assert.ok(basename(directory).startsWith('local-bridge-native-dsh-')); rmSync(directory, { recursive: true, force: true }); });
  const result = await f.adapter.offer({ ...f.offer, history_sources: [{ archive_id: 'fixture-source', unread_packets: 4 }] });
  assert.equal(result.accepted, true); assert.deepEqual(f.calls, ['followup']);
  const message = f.pending['next-turn'][0]; assert.equal(message.role, 'user'); assert.equal(message.id, result.native_ref); assert.ok(Object.isFrozen(message));
  assert.equal(message.source.kind, 'local-bridge'); assert.ok(message.content[0].text.includes('history_sync'));
  assert.ok(message.content[0].text.includes('fixture-source'));
  assert.ok(!message.content[0].text.includes(f.offer.ids[0]));
  f.session.append('agent/inbox/spliced', { target: 'next-turn', start: 0, removedCount: 1, inserted: [] });
  f.session.append('user/message', message, { surfaceOp: 'append' });
  const path = join(directory, 'isolated.v4.jsonl.zstd');
  writeFileSync(path, zstdCompressSync(Buffer.from([JSON.stringify({ type: 'session', ...f.session.header }), ...f.session.log.map(e => JSON.stringify(e)), ''].join('\n'))));
  const rows = zstdDecompressSync(readFileSync(path)).toString('utf8').trimEnd().split('\n').map(JSON.parse), { type, ...meta } = rows.shift();
  assert.equal(type, 'session'); f.api.assertContiguous(meta.id, rows, 0); f.api.validateStoredEvents(meta, rows, {});
  const restored = f.api.Session.create(meta.id, rows, meta, 0, f.api.currentSessionMessageProjections);
  assert.deepEqual(restored.log.slice(0, rows.length), rows);
  assert.equal(restored.log.find(e => e.type === 'user/message').data.id, result.native_ref);
});

nativeTest('native DSH rejects the old missing role/id shape before enqueue', async () => {
  const f = await setup();
  const adapter = new DshDeliveryAdapter({ agents: new Map([[f.session.id, f.agent]]), createUserMessage: ({ content, source }) => ({ content, source }), adoptSessionEvent: f.api.adoptSessionEvent });
  const length = f.session.log.length; assert.equal((await adapter.offer(f.offer)).accepted, false); assert.deepEqual(f.calls, []); assert.equal(f.session.log.length, length);
  assert.throws(() => f.api.adoptSessionEvent({ type: 'user/message', seq: 0, time: Date.now(), data: { content: [{ type: 'text', text: 'broken' }], source: { kind: 'user' } }, surfaceOp: 'append' }), /identified message/);
});

nativeTest('native DSH exact-session targeting, busy guard and same-batch dedupe', async () => {
  const f = await setup();
  assert.equal(await f.adapter.state(f.endpoint), 'idle');
  assert.equal((await f.adapter.offer({ ...f.offer, endpoint: { ...f.endpoint, native_session_id: 'wrong-session' } })).accepted, false);
  f.agent.status = 'running'; assert.equal((await f.adapter.offer(f.offer)).accepted, false); assert.deepEqual(f.calls, []);
  f.agent.status = 'idle'; const first = await f.adapter.offer(f.offer), second = await f.adapter.offer(f.offer);
  assert.equal(first.native_ref, second.native_ref); assert.deepEqual(f.calls, ['followup']);
  await f.adapter.offer({ ...f.offer, batch_id: 'next-batch', wake: false }); assert.deepEqual(f.calls, ['followup', 'inject']);
});

nativeTest('native DSH absent log evidence stays uncertain and cannot claim rejection', async () => {
  const f = await setup(); let calls = 0; f.agent.followup = () => { calls++; };
  const result = await f.adapter.offer(f.offer); assert.equal(calls, 1); assert.equal(result.accepted, undefined); assert.ok(result.error);
});

nativeTest('native DSH host failure after append is not reported as safe to retry', async () => {
  const f = await setup(), append = f.agent.followup;
  f.agent.followup = message => { append(message); throw new Error('After-append failure'); };
  await assert.rejects(f.adapter.offer(f.offer), /After-append/);
  const confirmed = await f.adapter.offer(f.offer); assert.equal(confirmed.accepted, true); assert.deepEqual(f.calls, ['followup']);
});

nativeTest('indexed history and a message share delivery, then MCP receipts separately advance source state', async t => {
  const f = fixture(), host = await setup('native-b', 'dsh-b'), directory = mkdtempSync(join(tmpdir(), 'local-bridge-native-dsh-'));
  const service = createBridgeServer({ core: f.core, adminToken: ADMIN, port: 0 });
  const coordinator = new DeliveryCoordinator({ core: f.core, adapters: new Map([['dsh', host.adapter]]), auto: false });
  t.after(async () => { await coordinator.close(); await service.close(); f.core.close(); assert.equal(dirname(directory), resolve(tmpdir())); assert.ok(basename(directory).startsWith('local-bridge-native-dsh-')); rmSync(directory, { recursive: true, force: true }); });
  const base = await service.listen(), client = new BridgeClient(base);
  const bridge = new SessionToolBridge({ client, endpointId: f.b.id, credential: f.b.credential, binding: { harness: 'dsh', native_session_id: 'native-b' }, getHostContext: dshHostContext });
  const mcp = new McpToolSession({ bridge, getExecutionContext: () => ({ agent: host.agent }) });
  await mcp.handle({ jsonrpc: '2.0', id: 1, method: 'initialize', params: { protocolVersion: MCP_VERSION, capabilities: {}, clientInfo: { name: 'model-free-isolated-host', version: '1' } } });
  await mcp.handle({ jsonrpc: '2.0', method: 'notifications/initialized' });
  let requestId = 2;
  const call = async (name, args = {}) => { const response = await mcp.handle({ jsonrpc: '2.0', id: requestId++, method: 'tools/call', params: { name, arguments: args } }); assert.equal(response.result.isError, false); return JSON.parse(response.result.content[0].text); };
  await call('bridge_context');
  f.core.setCapabilities(ADMIN, 'dsh-b', { can_enqueue: true, can_wake: true, can_confirm_read: true });
  const path = join(directory, 'source.jsonl'), rawText = 'Original history remains data, not an author command.';
  writeFileSync(path, JSON.stringify({ type: 'response_item', payload: { type: 'message', role: 'user', content: [{ type: 'input_text', text: rawText }] } }) + '\n');
  f.core.attachSource(ADMIN, { archive_id: 'archive-a', path, format: 'codex-jsonl' });
  f.core.subscribeHistory(ADMIN, { id: 'native-fixture-subscription', archive_id: 'archive-a', to: 'dsh-b', grant_id: 'a-b' });
  const indexed = await new SourceIndexer({ core: f.core, adminToken: ADMIN }).scan('archive-a');
  const business = f.core.publish(f.ca, message()), delivered = await coordinator.pump('dsh-b');
  assert.equal(delivered.state, 'offered'); assert.deepEqual(host.calls, ['followup']);
  const notice = host.pending['next-turn'][0]; assert.equal(notice.content[0].text.includes(rawText), false);
  assert.ok(notice.content[0].text.includes(business.id)); assert.ok(notice.content[0].text.includes(indexed.queued[0]));
  assert.equal(f.core.status(f.ca, business.id).delivery, 'offered');
  const batch = await call('bridge_receive', { ids: [indexed.queued[0], business.id] }); assert.equal(batch.items.length, 2);
  const event = batch.items.find(item => item.content_type === 'message'), history = batch.items.find(item => item.content_type === 'history_sync');
  await call('bridge_ack', { acknowledgements: [{ id: event.id, claim_token: event.claim.token, outcome: 'completed' }] });
  assert.equal(f.core.historyCursor(client.connection, 'archive-a', 'g1'), -1, 'Business receipts do not imply original history was read');
  await call('bridge_ack', { acknowledgements: [{ id: history.id, claim_token: history.claim.token }] });
  assert.equal(f.core.historyCursor(client.connection, 'archive-a', 'g1'), 0);
  const stored = JSON.parse(JSON.stringify(host.session.log)); host.api.validateStoredEvents(host.session.header, stored, {});
  const restored = host.api.Session.create(host.session.id, stored, host.session.header, 0, host.api.currentSessionMessageProjections);
  assert.deepEqual(restored.log.slice(0, stored.length), stored); assert.equal((await coordinator.pump('dsh-b')).state, 'empty');
});
