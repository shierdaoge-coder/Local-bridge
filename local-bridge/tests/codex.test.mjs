import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { resolve, join, dirname, basename } from 'node:path';
import { tmpdir } from 'node:os';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { spawn } from 'node:child_process';
import { fixture, ADMIN, message, history, code } from './helpers.mjs';
import { CodexGateway } from '../src/codex-gateway.mjs';
import { CodexDeliveryAdapter, CodexQueue, CODEX_TOOL_PREFIX, codexHostContext } from '../src/adapters/codex.mjs';
import { DeliveryCoordinator } from '../src/delivery.mjs';
import { createBridgeServer } from '../src/server.mjs';
import { BridgeClient } from '../src/client.mjs';
import { runCodexHook } from '../src/codex-hook.mjs';
import { CodexMcpProxy } from '../src/codex-mcp.mjs';
import { McpToolSession, MCP_VERSION } from '../src/mcp.mjs';

const rpc = (id, method, params) => ({ jsonrpc: '2.0', id, method, ...(params ? { params } : {}) });
const hello = rpc(1, 'initialize', { protocolVersion: MCP_VERSION, capabilities: {}, clientInfo: { name: 'isolated-codex', version: '1' } });
const initialized = { jsonrpc: '2.0', method: 'notifications/initialized' };
async function setup(t) {
  const f = fixture(), gateway = new CodexGateway({ core: f.core, now: () => f.clock.now }), adapter = new CodexDeliveryAdapter({ gateway });
  const coordinator = new DeliveryCoordinator({ core: f.core, adapters: new Map([['codex', adapter]]), auto: false }); gateway.coordinator = coordinator;
  const service = createBridgeServer({ core: f.core, adminToken: ADMIN, codex: gateway, port: 0 }); const base = await service.listen(); gateway.settingsUrl = base;
  t.after(async () => { await coordinator.close(); await service.close(); f.core.close(); });
  const cwd = resolve(tmpdir()), transcript_path = join(cwd, 'local-bridge-fixture-transcript.jsonl');
  const config = { service_url: base, bindings: { 'native-a': { endpoint_id: 'codex-a', credential: f.a.credential, cwd, transcript_path } } };
  const event = patch => ({ session_id: 'native-a', cwd, transcript_path, hook_event_name: 'PreToolUse', turn_id: 'fixture-turn', tool_use_id: randomUUID(), tool_name: CODEX_TOOL_PREFIX + 'bridge_context', tool_input: {}, ...patch });
  const proxy = new CodexMcpProxy({ client: new BridgeClient(base), config });
  const mcp = new McpToolSession({ bridge: proxy }); await mcp.handle(hello); await mcp.handle(initialized); let id = 2;
  const call = async (name, args = {}) => {
    const hook = await runCodexHook(event({ tool_name: CODEX_TOOL_PREFIX + name, tool_input: args }), { config });
    assert.equal(hook.hookSpecificOutput.permissionDecision, 'allow');
    const result = await mcp.handle(rpc(id++, 'tools/call', { name, arguments: hook.hookSpecificOutput.updatedInput }));
    assert.equal(result.result.isError, false, JSON.stringify(result)); return JSON.parse(result.result.content[0].text);
  };
  const issue = async patch => { const input = event(patch); return { input, result: await gateway.hostEvent(f.a.credential, { endpoint_id: 'codex-a', event: input }) }; };
  return { ...f, gateway, adapter, coordinator, service, base, config, event, mcp, proxy, call, issue };
}

test('Codex MCP needs a fresh native hook ticket; model _meta cannot establish identity', async t => {
  const f = await setup(t), epoch = f.core.endpoint('codex-a').epoch;
  const response = await f.mcp.handle(rpc(2, 'tools/call', { name: 'bridge_context', arguments: {}, _meta: { session_id: 'native-a' } }));
  assert.equal(response.result.isError, true); assert.equal(JSON.parse(response.result.content[0].text).error, 'HOST_CONTEXT_UNVERIFIED');
  assert.equal(f.core.endpoint('codex-a').epoch, epoch); assert.equal(codexHostContext({ harness: 'codex', native_session_id: 'native-a' }), null);
  const context = await f.call('bridge_context'); assert.equal(context.endpoint.id, 'codex-a'); assert.equal(context.settings_url, f.base);
  assert.ok(!JSON.stringify(context).includes(f.a.credential));
});

test('unbound bridge_context denial provides only native setup metadata and refuses missing transcript', async () => {
  const native = { session_id: 'native-setup', cwd: resolve(tmpdir()), transcript_path: join(tmpdir(), 'rollout.jsonl'), hook_event_name: 'PreToolUse', tool_name: CODEX_TOOL_PREFIX + 'bridge_context', tool_use_id: 'setup-hint', tool_input: {} };
  const output = await runCodexHook(native, { config: { bindings: {} }, env: { PLUGIN_DATA: join(tmpdir(), 'private-plugin-data') } });
  const reason = output.hookSpecificOutput.permissionDecisionReason;
  assert.equal(output.hookSpecificOutput.permissionDecision, 'deny');
  assert.match(reason, /native-setup/); assert.match(reason, /transcript_path/); assert.match(reason, /private-plugin-data/);
  assert.doesNotMatch(reason, /prompt|token|credential|tool_input/i);
  const noTranscript = await runCodexHook({ ...native, transcript_path: null }, { config: { bindings: {} }, env: { PLUGIN_DATA: 'private-path' } });
  assert.match(noTranscript.hookSpecificOutput.permissionDecisionReason, /不支持/);
  assert.equal((await runCodexHook({ ...native, tool_name: CODEX_TOOL_PREFIX + 'bridge_receive' }, { config: { bindings: {} } })).hookSpecificOutput.permissionDecisionReason.includes('metadata='), false);
});

test('Codex hook credentials, native ID, workspace, transcript and subagent guards fail before a connection', async t => {
  const f = await setup(t), epoch = f.core.endpoint('codex-a').epoch;
  await assert.rejects(f.gateway.hostEvent('bad-secret', { endpoint_id: 'codex-a', event: f.event() }), code('UNAUTHORIZED'));
  await assert.rejects(f.gateway.hostEvent(f.a.credential, { endpoint_id: 'codex-a', event: f.event({ session_id: 'other' }) }), code('CALLER_MISMATCH'));
  for (const patch of [{ cwd: join(tmpdir(), 'wrong') }, { transcript_path: join(tmpdir(), 'wrong.jsonl') }, { agent_id: 'child' }]) {
    const output = await runCodexHook(f.event(patch), { config: f.config }); assert.equal(output.hookSpecificOutput.permissionDecision, 'deny');
  }
  assert.equal(f.core.endpoint('codex-a').epoch, epoch);
});

test('Codex one-use tickets bind the exact tool and arguments and dedupe the native call ID', async t => {
  const f = await setup(t), { input, result } = await f.issue();
  const same = await f.gateway.hostEvent(f.a.credential, { endpoint_id: 'codex-a', event: input }); assert.equal(same.ticket, result.ticket);
  await assert.rejects(f.gateway.call(result.ticket, { name: 'bridge_status', arguments: { id: 'other' } }), code('CALL_CONFLICT'));
  await assert.rejects(f.gateway.hostEvent(f.a.credential, { endpoint_id: 'codex-a', event: { ...input, tool_input: { forged: true } } }), code('CALL_CONFLICT'));
  assert.equal((await f.gateway.call(result.ticket, { name: 'bridge_context', arguments: {} })).endpoint.id, 'codex-a');
  await assert.rejects(f.gateway.call(result.ticket, { name: 'bridge_context', arguments: {} }), code('TICKET_USED'));
  await assert.rejects(f.gateway.hostEvent(f.a.credential, { endpoint_id: 'codex-a', event: input }), code('TICKET_USED'));
});

test('Codex expired tickets and tickets from an invalidated epoch cannot execute', async t => {
  const f = await setup(t), first = await f.issue(); f.clock.now += 30_001;
  await assert.rejects(f.gateway.call(first.result.ticket, { name: 'bridge_context', arguments: {} }), code('UNAUTHORIZED'));
  const second = await f.issue(); f.core.connect('codex-a', f.a.credential);
  await assert.rejects(f.gateway.call(second.result.ticket, { name: 'bridge_context', arguments: {} }), code('STALE_CONNECTION'));
  await assert.rejects(f.gateway.hostEvent(f.a.credential, { endpoint_id: 'codex-a', event: second.input }), code('STALE_CONNECTION'));
});

test('Codex MCP liveness keeps its established connection online without exposing credentials or changing host state', async t => {
  const f = await setup(t), result = await f.call('bridge_context'), epoch = f.core.endpoint('codex-a').epoch;
  const token = f.proxy.liveness.get('codex-a'); assert.ok(token); assert.ok(!JSON.stringify(result).includes(token));
  f.clock.now += 20_000; await f.proxy.sendHeartbeats(); f.clock.now += 20_000; await f.proxy.sendHeartbeats();
  assert.equal(f.core.endpoint('codex-a').online, true); assert.equal(f.core.endpoint('codex-a').epoch, epoch); assert.equal(f.gateway.hostState(f.core.endpoint('codex-a')), 'busy');
  await assert.rejects(f.gateway.call(token, { name: 'bridge_context', arguments: {} }), code('UNAUTHORIZED'));
  await runCodexHook(f.event({ hook_event_name: 'Stop' }), { config: f.config });
  await f.proxy.sendHeartbeats(); assert.equal(f.gateway.hostState(f.core.endpoint('codex-a')), 'idle');
  await runCodexHook(f.event({ hook_event_name: 'SessionEnd' }), { config: f.config });
  const heartbeat = await f.proxy.sendHeartbeats(); assert.equal(heartbeat[0].status, 'rejected'); assert.equal(f.proxy.liveness.size, 0);
});

test('Codex successive tool hooks preserve the same connection and its receive claims', async t => {
  const f = await setup(t), sent = f.core.publish(f.cb, message({ to: 'codex-a', grant_id: 'b-a' }));
  const batch = await f.call('bridge_receive', { ids: [sent.id] }), epoch = f.core.endpoint('codex-a').epoch;
  const ack = await f.call('bridge_ack', { acknowledgements: [{ id: sent.id, claim_token: batch.items[0].claim.token, outcome: 'completed' }] });
  assert.equal(ack[0].ok, true); assert.equal(f.core.endpoint('codex-a').epoch, epoch);
  assert.equal((await f.call('bridge_status', { id: sent.id })).outcome, 'completed');
});

test('Codex hook-injected ticket cannot be supplied as a model argument', async t => {
  const f = await setup(t), output = await runCodexHook(f.event({ tool_input: { _bridge_ticket: 'forged' } }), { config: f.config });
  assert.equal(output.hookSpecificOutput.permissionDecision, 'deny'); assert.equal(f.gateway.tickets.size, 0);
});

test('Codex concurrent native sessions get independent tool identities', async t => {
  const f = await setup(t), c = f.core.register(ADMIN, { id: 'codex-c', harness: 'codex', native_session_id: 'native-c' });
  const second = f.event({ session_id: 'native-c' });
  const [a, b] = await Promise.all([f.issue(), f.gateway.hostEvent(c.credential, { endpoint_id: c.id, event: second })]);
  const [ar, br] = await Promise.all([f.gateway.call(a.result.ticket, { name: 'bridge_context', arguments: {} }), f.gateway.call(b.ticket, { name: 'bridge_context', arguments: {} })]);
  assert.equal(ar.endpoint.id, 'codex-a'); assert.equal(br.endpoint.id, 'codex-c');
});

test('Codex natural-turn hook delivers shared history/message notification without waking a model', async t => {
  const f = await setup(t); f.core.setCapabilities(ADMIN, 'codex-a', { can_enqueue: true });
  f.core.registerArchive(ADMIN, { id: 'archive-b', owner: 'dsh-b', generation: 'g1' });
  const packet = history(0, 0, { to: 'codex-a', grant_id: 'b-a' }); packet.payload.archive_id = 'archive-b';
  const original = f.core.publish(f.cb, packet), sent = f.core.publish(f.cb, message({ to: 'codex-a', grant_id: 'b-a' }));
  const output = await runCodexHook(f.event({ hook_event_name: 'UserPromptSubmit', prompt: 'This prompt is not forwarded to the bridge.' }), { config: f.config });
  const context = output.hookSpecificOutput.additionalContext; assert.ok(context.includes(sent.id)); assert.ok(context.includes(original.id)); assert.ok(!context.includes(packet.payload.records[0].text));
  assert.equal(f.core.status(f.cb, sent.id).delivery, 'offered');
  const batch = await f.call('bridge_receive', { ids: [original.id, sent.id] });
  const event = batch.items.find(m => m.content_type === 'message'), hist = batch.items.find(m => m.content_type === 'history_sync');
  await f.call('bridge_ack', { acknowledgements: [{ id: event.id, claim_token: event.claim.token, outcome: 'completed' }] });
  assert.equal((await f.call('bridge_history_cursor', { archive_id: 'archive-b', generation: 'g1' })).read_through, -1);
  await f.call('bridge_ack', { acknowledgements: [{ id: hist.id, claim_token: hist.claim.token }] });
  assert.equal((await f.call('bridge_history_cursor', { archive_id: 'archive-b', generation: 'g1' })).read_through, 0);
});

test('Codex pause and unverified enqueue capability prevent natural-turn delivery', async t => {
  const f = await setup(t), sent = f.core.publish(f.cb, message({ to: 'codex-a', grant_id: 'b-a' }));
  const event = f.event({ hook_event_name: 'UserPromptSubmit' });
  assert.deepEqual(await runCodexHook(event, { config: f.config }), {});
  f.core.setCapabilities(ADMIN, 'codex-a', { can_enqueue: true }); f.core.setSettings(ADMIN, 'codex-a', { paused: true });
  assert.deepEqual(await runCodexHook(event, { config: f.config }), {}); assert.equal(f.core.status(f.cb, sent.id).delivery, 'queued');
});

test('Codex end and interruption invalidate unused hook tickets without creating continuation turns', async t => {
  const f = await setup(t), first = await f.issue();
  assert.deepEqual(await runCodexHook(f.event({ hook_event_name: 'Stop' }), { config: f.config }), {});
  const endpoint = f.core.endpoint('codex-a'); assert.equal(f.gateway.hostState(endpoint), 'idle');
  assert.deepEqual(await runCodexHook(f.event({ hook_event_name: 'SessionEnd' }), { config: f.config }), {});
  assert.equal(f.core.endpoint('codex-a').online, false);
  await assert.rejects(f.gateway.call(first.result.ticket, { name: 'bridge_context', arguments: {} }), code('STALE_CONNECTION'));
  const second = await f.issue(); await runCodexHook(f.event({ hook_event_name: 'Interrupt' }), { config: f.config });
  await assert.rejects(f.gateway.call(second.result.ticket, { name: 'bridge_context', arguments: {} }), code('STALE_CONNECTION'));
});

test('Codex missing configuration is quiet, unrelated tools are unchanged, and unavailable bridge denies only its tools', async t => {
  const f = await setup(t);
  assert.deepEqual(await runCodexHook(f.event({ hook_event_name: 'SessionStart' })), {});
  assert.deepEqual(await runCodexHook(f.event({ tool_name: 'Bash' })), {});
  assert.equal((await runCodexHook(f.event())).hookSpecificOutput.permissionDecision, 'deny');
  const client = { request: async () => { throw new Error('Private credential must not leak'); } };
  const result = await runCodexHook(f.event(), { config: f.config, client }); assert.ok(!JSON.stringify(result).includes('Private credential'));
});

test('Codex native queue is disabled by default and exact UUID execution never treats exit zero as a receipt', async () => {
  let calls = 0; const input = { endpoint: { harness: 'codex', native_session_id: '00000000-0000-4000-8000-000000000002' }, batch_id: 'fixture-batch', ids: ['fixture-id'], content_types: ['message'], wake: true };
  assert.equal((await new CodexQueue({ run: async () => { calls++; } }).offer(input)).accepted, false); assert.equal(calls, 0);
  const queue = new CodexQueue({ enabled: true, executable: process.execPath, run: async (file, args, options) => {
    calls++; assert.equal(file, process.execPath); assert.deepEqual(args.slice(0, 4), ['queue', '--thread', input.endpoint.native_session_id, '--message']);
    assert.equal(args.at(-1), '--resume'); assert.match(args[4], /^<local_bridge_transport>/); assert.equal(options.shell, false); assert.equal(options.windowsHide, true);
    return { stdout: 'success', stderr: 'resume warning' };
  } });
  const result = await queue.offer(input); assert.equal(result.accepted, undefined); assert.ok(result.error); assert.equal(calls, 1);
  assert.equal((await queue.offer({ ...input, endpoint: { ...input.endpoint, native_session_id: 'ambiguous name' } })).accepted, false); assert.equal(calls, 1);
});

test('Codex queue timeouts and post-launch failures remain uncertain while missing executable is safely rejected', async () => {
  const input = { endpoint: { native_session_id: '00000000-0000-4000-8000-000000000000' }, batch_id: 'batch', ids: ['id'], content_types: ['message'], wake: true };
  for (const code of ['ENOENT', 'EACCES', 'ETIMEDOUT', 2]) {
    const queue = new CodexQueue({ enabled: true, executable: process.execPath, run: async () => { const error = new Error('private'); error.code = code; throw error; } });
    const result = await queue.offer(input); assert.equal(result.accepted, ['ENOENT', 'EACCES'].includes(code) ? false : undefined); assert.ok(!result.error.includes('private'));
  }
});

test('Codex coordinated uncertain wake is not retried and a target receipt resolves it', async t => {
  const f = fixture(), nativeId = '00000000-0000-4000-8000-000000000099'; let executions = 0;
  const target = f.core.register(ADMIN, { id: 'codex-target', harness: 'codex', native_session_id: nativeId, capabilities: { can_enqueue: true, can_wake: true } });
  f.core.grant(ADMIN, { id: 'b-target', from: 'dsh-b', to: target.id, content_types: ['message'], allow_wake: true, author_ref: 'isolated-author-instruction' });
  const gateway = new CodexGateway({ core: f.core, now: () => f.clock.now });
  const queue = new CodexQueue({ enabled: true, executable: process.execPath, run: async () => { executions++; return { stdout: 'success' }; } });
  const coordinator = new DeliveryCoordinator({ core: f.core, adapters: new Map([['codex', new CodexDeliveryAdapter({ gateway, queue })]]), auto: false });
  t.after(async () => { await coordinator.close(); f.core.close(); });
  await gateway.hostEvent(target.credential, { endpoint_id: target.id, event: { session_id: nativeId, hook_event_name: 'Stop' } });
  const sent = f.core.publish(f.cb, message({ to: target.id, grant_id: 'b-target' }));
  assert.equal((await coordinator.pump(target.id)).state, 'uncertain');
  assert.equal((await coordinator.pump(target.id)).state, 'empty'); assert.equal(executions, 1);
  const call = async (name, args) => {
    const result = await gateway.hostEvent(target.credential, { endpoint_id: target.id, event: { session_id: nativeId, hook_event_name: 'PreToolUse', tool_use_id: randomUUID(), tool_name: CODEX_TOOL_PREFIX + name, tool_input: args } });
    return gateway.call(result.ticket, { name, arguments: args });
  };
  const batch = await call('bridge_receive', { ids: [sent.id] });
  await call('bridge_ack', { acknowledgements: [{ id: sent.id, claim_token: batch.items[0].claim.token, outcome: 'completed' }] });
  assert.equal(f.core.status(f.cb, sent.id).attempts[0].state, 'confirmed'); assert.equal(executions, 1);
});

test('Codex host and tool routes require distinct native credentials or one-use tickets', async t => {
  const f = await setup(t), client = new BridgeClient(f.base);
  await assert.rejects(client.request('/api/codex/host-event', { method: 'POST', body: { endpoint_id: 'codex-a', event: f.event() }, credential: f.a.credential + 'bad' }), code('UNAUTHORIZED'));
  await assert.rejects(client.request('/api/codex/tool-call', { method: 'POST', body: { name: 'bridge_context' }, credential: f.a.credential }), code('UNAUTHORIZED'));
  assert.equal(f.gateway.sessions.size, 0);
});

test('Codex MCP executable accepts a native-hook ticket over real stdio without using startup thread identity', async t => {
  const f = await setup(t), { result } = await f.issue(), directory = mkdtempSync(join(tmpdir(), 'local-bridge-codex-stdio-'));
  t.after(() => { assert.equal(dirname(directory), resolve(tmpdir())); assert.ok(basename(directory).startsWith('local-bridge-codex-stdio-')); rmSync(directory, { recursive: true, force: true }); });
  const config = join(directory, 'client.json'); writeFileSync(config, JSON.stringify({ service_url: f.base }));
  const child = spawn(process.execPath, [resolve('src/codex-mcp.mjs')], { windowsHide: true, env: { ...process.env, LOCAL_BRIDGE_CONFIG: config, CODEX_THREAD_ID: 'unrelated-startup-thread' }, stdio: ['pipe', 'pipe', 'pipe'] });
  let stdout = '', stderr = ''; child.stdout.on('data', chunk => { stdout += chunk; }); child.stderr.on('data', chunk => { stderr += chunk; });
  child.stdin.end([hello, initialized, rpc(2, 'tools/call', { name: 'bridge_context', arguments: { _bridge_ticket: result.ticket } })].map(JSON.stringify).join('\n') + '\n');
  const exit = await new Promise((resolve, reject) => { child.once('error', reject); child.once('close', resolve); });
  assert.equal(exit, 0, stderr); const frames = stdout.trimEnd().split('\n').map(JSON.parse); assert.equal(frames.length, 2);
  assert.equal(JSON.parse(frames[1].result.content[0].text).endpoint.native_session_id, 'native-a');
});
