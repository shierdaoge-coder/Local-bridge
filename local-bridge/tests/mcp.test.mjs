import test from 'node:test';
import assert from 'node:assert/strict';
import { Readable, Writable } from 'node:stream';
import { McpToolSession, serveMcpStdio, MCP_VERSION } from '../src/mcp.mjs';
import { SessionToolBridge } from '../src/session-tools.mjs';
import { dshHostContext } from '../src/adapters/dsh.mjs';
import { BridgeClient } from '../src/client.mjs';
import { createBridgeServer } from '../src/server.mjs';
import { ADMIN, fixture, message } from './helpers.mjs';

const rpc = (id, method, params) => ({ jsonrpc: '2.0', id, method, ...(params ? { params } : {}) });
const hello = () => rpc(1, 'initialize', { protocolVersion: MCP_VERSION, capabilities: {}, clientInfo: { name: 'isolated-client', version: '1' } });
const initialized = { jsonrpc: '2.0', method: 'notifications/initialized' };
async function setup(t, getExecutionContext = () => ({ agent: { session: { id: 'native-b' } } })) {
  const f = fixture(), service = createBridgeServer({ core: f.core, adminToken: ADMIN, port: 0 }), base = await service.listen();
  const bridge = new SessionToolBridge({ client: new BridgeClient(base), endpointId: f.b.id, credential: f.b.credential, binding: { harness: 'dsh', native_session_id: 'native-b' }, getHostContext: dshHostContext });
  const session = new McpToolSession({ bridge, getExecutionContext });
  t.after(async () => { await service.close(); f.core.close(); });
  return { ...f, session };
}
async function ready(session) { const init = await session.handle(hello()); assert.equal(init.result.protocolVersion, MCP_VERSION); assert.equal(await session.handle(initialized), null); }
const toolResult = response => JSON.parse(response.result.content[0].text);

test('MCP negotiates lifecycle and exposes only six endpoint tools', async t => {
  const f = await setup(t);
  assert.equal((await f.session.handle(rpc(0, 'tools/list'))).error.code, -32600);
  assert.deepEqual((await f.session.handle(rpc(0, 'ping'))).result, {});
  const init = await f.session.handle(hello()); assert.deepEqual(Object.keys(init.result.capabilities), ['tools']);
  assert.equal((await f.session.handle(rpc(2, 'tools/list'))).error.code, -32600);
  await f.session.handle(initialized);
  const tools = (await f.session.handle(rpc(3, 'tools/list'))).result.tools;
  assert.equal(tools.length, 6); assert.ok(tools.every(t => !t.name.includes('admin')));
  assert.equal((await f.session.handle(hello())).error.code, -32600);
});

test('MCP calls traverse real HTTP receive and claim acknowledgement, with separate receipts', async t => {
  const f = await setup(t), sent = f.core.publish(f.ca, message()); await ready(f.session);
  const received = await f.session.handle(rpc(2, 'tools/call', { name: 'bridge_receive', arguments: { ids: [sent.id] } }));
  assert.equal(received.result.isError, false); const item = toolResult(received).items[0];
  assert.equal(f.core.status(f.ca, sent.id).delivery, 'queued');
  const ack = await f.session.handle(rpc(3, 'tools/call', { name: 'bridge_ack', arguments: { acknowledgements: [{ id: item.id, claim_token: item.claim.token, outcome: 'completed' }] } }));
  assert.equal(toolResult(ack)[0].ok, true); assert.equal(f.core.status(f.ca, sent.id).outcome, 'completed');
});

test('MCP caller metadata never supplies host identity and absent identity fails closed', async t => {
  const f = await setup(t, () => undefined), epoch = f.core.endpoint('dsh-b').epoch; await ready(f.session);
  const result = await f.session.handle(rpc(2, 'tools/call', { name: 'bridge_context', arguments: {}, _meta: { harness: 'dsh', native_session_id: 'native-b' } }));
  assert.equal(result.result.isError, true); assert.equal(toolResult(result).error, 'HOST_CONTEXT_UNVERIFIED');
  assert.equal(f.core.endpoint('dsh-b').epoch, epoch);
});

test('MCP separates protocol errors from execution errors without leaking internal failures', async t => {
  const f = await setup(t); await ready(f.session);
  assert.equal((await f.session.handle(rpc(2, 'tools/call', { name: 'admin_grant' }))).error.code, -32602);
  assert.equal((await f.session.handle(rpc(3, 'tools/call', { name: 'bridge_receive', arguments: { from: 'forged' } }))).error.code, -32602);
  assert.equal((await f.session.handle(rpc(4, 'resources/list'))).error.code, -32601);
  assert.equal((await f.session.handle([])).error.code, -32600);
  assert.equal(await f.session.handle({ jsonrpc: '2.0', method: 'ignored/notification' }), null);
});

test('stdio parses split UTF-8 frames and emits only newline JSON-RPC responses', async t => {
  const f = await setup(t), bytes = Buffer.from([hello(), initialized, rpc(2, 'tools/call', { name: 'bridge_context', arguments: {}, _meta: { description: '通信' } })].map(JSON.stringify).join('\n') + '\n');
  const split = bytes.indexOf(Buffer.from('通信')) + 1, chunks = [bytes.subarray(0, 8), bytes.subarray(8, split), bytes.subarray(split)]; let text = '';
  const output = new Writable({ write(chunk, encoding, callback) { text += chunk.toString(); callback(); } });
  await serveMcpStdio({ input: Readable.from(chunks), output, session: f.session });
  const rows = text.trimEnd().split('\n').map(JSON.parse); assert.equal(rows.length, 2); assert.deepEqual(rows.map(row => row.id), [1, 2]);
  assert.equal(toolResult(rows[1]).endpoint.native_session_id, 'native-b');
});

test('stdio bounds incomplete/oversized input and rejects malformed frames without tool calls', async t => {
  const f = await setup(t), before = f.core.endpoint('dsh-b').epoch;
  for (const [bytes, budget, code] of [[Buffer.from('{broken}\n'), 100, -32700], [Buffer.from('x'.repeat(101)), 100, -32600], [Buffer.from('{"jsonrpc":'), 100, -32700], [Buffer.from([255, 10]), 100, -32700]]) {
    let text = ''; const output = new Writable({ write(chunk, encoding, callback) { text += chunk.toString(); callback(); } });
    await serveMcpStdio({ input: Readable.from([bytes]), output, session: f.session, maxFrameBytes: budget });
    assert.equal(JSON.parse(text).error.code, code);
  }
  assert.equal(f.core.endpoint('dsh-b').epoch, before);
});
