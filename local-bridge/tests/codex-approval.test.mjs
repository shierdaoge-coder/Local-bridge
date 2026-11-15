import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { resolve, join, dirname, basename } from 'node:path';
import { tmpdir } from 'node:os';
import { mkdtempSync, writeFileSync, readFileSync, mkdirSync, rmSync } from 'node:fs';
import { spawn } from 'node:child_process';
import { createInterface } from 'node:readline';
import { fixture, ADMIN, code, message } from './helpers.mjs';
import { CodexGateway } from '../src/codex-gateway.mjs';
import { CodexMcpProxy } from '../src/codex-mcp.mjs';
import { runCodexHook } from '../src/codex-hook.mjs';
import { decodeApprovalProof, encodeApprovalProof } from '../src/codex-approval.mjs';
import { createBridgeServer } from '../src/server.mjs';
import { BridgeClient } from '../src/client.mjs';
import { CODEX_TOOL_PREFIX } from '../src/adapters/codex.mjs';
import { MCP_VERSION } from '../src/mcp.mjs';
import { build } from '../scripts/build-codex.mjs';

async function setup(t) {
  const f = fixture(), gateway = new CodexGateway({ core: f.core, now: () => f.clock.now });
  const service = createBridgeServer({ core: f.core, adminToken: ADMIN, codex: gateway, port: 0 });
  const base = await service.listen(), client = new BridgeClient(base), cwd = resolve(tmpdir());
  t.after(async () => { await service.close(); f.core.close(); });
  const config = { service_url: base, bindings: { 'native-a': {
    endpoint_id: 'codex-a', credential: f.a.credential, cwd, transcript_path: join(cwd, 'fixture.jsonl'),
  } } };
  const event = patch => ({ session_id: 'native-a', cwd, transcript_path: config.bindings['native-a'].transcript_path,
    hook_event_name: 'PreToolUse', turn_id: 'review-turn', tool_use_id: randomUUID(),
    tool_name: CODEX_TOOL_PREFIX + 'bridge_context', tool_input: {}, ...patch });
  const issue = async patch => {
    const input = event(patch), output = await runCodexHook(input, { config });
    assert.equal(output.hookSpecificOutput.permissionDecision, 'allow');
    const args = output.hookSpecificOutput.updatedInput, proof = decodeApprovalProof(args._bridge_ticket);
    return { input, args, proof };
  };
  const exchange = proof => gateway.exchange(f.a.credential, { ...proof, name: 'bridge_context', arguments: {} });
  return { ...f, gateway, service, base, client, config, event, issue, exchange,
    proxy: new CodexMcpProxy({ client, config }) };
}

test('human review lasting 52 minutes can redeem exact native proof over HTTP after its lease expires', async t => {
  const f = await setup(t), pending = await f.issue(), epoch = f.core.endpoint('codex-a').epoch;
  f.clock.now += 52 * 60_000;
  assert.equal(f.core.endpoint('codex-a').online, false);
  const result = await f.proxy.execute('bridge_context', pending.args);
  assert.equal(result.endpoint.id, 'codex-a'); assert.equal(result.endpoint.native_session_id, 'native-a');
  assert.equal(result.endpoint.epoch, epoch + 1);
  assert.doesNotMatch(JSON.stringify(result), new RegExp(pending.proof.proof));
  assert.ok(!JSON.stringify(result).includes(f.a.credential));
  assert.ok(!JSON.stringify(result).includes(f.proxy.liveness.get('codex-a')));
  await assert.rejects(f.proxy.execute('bridge_context', pending.args), code('TICKET_USED'));
});

test('pending approval alone is not a tool ticket and needs the private matching endpoint credential', async t => {
  const f = await setup(t), pending = await f.issue(); f.clock.now += 60_000;
  await assert.rejects(f.gateway.call(pending.proof.proof, { name: 'bridge_context', arguments: {} }), code('UNAUTHORIZED'));
  assert.throws(() => f.gateway.exchange(f.b.credential, { ...pending.proof, name: 'bridge_context', arguments: {} }), code('UNAUTHORIZED'));
  await assert.rejects(new CodexMcpProxy({ client: f.client }).execute('bridge_context', pending.args), code('HOST_CONTEXT_UNVERIFIED'));
  assert.equal((await f.proxy.execute('bridge_context', pending.args)).endpoint.id, 'codex-a');
});

test('approval redemption binds exact tool, arguments, endpoint and native session', async t => {
  const f = await setup(t), pending = await f.issue();
  for (const patch of [{ name: 'bridge_status', arguments: { id: 'other' } }, { arguments: { extra: true } }]) {
    assert.throws(() => f.gateway.exchange(f.a.credential, { ...pending.proof, name: 'bridge_context', arguments: {}, ...patch }), code('CALL_CONFLICT'));
  }
  assert.throws(() => f.gateway.exchange(f.a.credential, { ...pending.proof, native_session_id: 'other', name: 'bridge_context', arguments: {} }), code('CALLER_MISMATCH'));
  assert.throws(() => f.gateway.exchange(f.b.credential, { ...pending.proof, endpoint_id: 'dsh-b', native_session_id: 'native-b', name: 'bridge_context', arguments: {} }), code('CALLER_MISMATCH'));
  const swapped = encodeApprovalProof({ ...pending.proof, native_session_id: 'other' });
  await assert.rejects(f.proxy.execute('bridge_context', { _bridge_ticket: swapped }), code('HOST_CONTEXT_UNVERIFIED'));
  assert.equal((await f.proxy.execute('bridge_context', pending.args)).endpoint.id, 'codex-a');
});

test('redemption issues a fresh one-use 30-second ticket and cannot extend it twice', async t => {
  const f = await setup(t), pending = await f.issue(); f.clock.now += 52 * 60_000;
  const exchanged = f.exchange(pending.proof);
  assert.equal(exchanged.expires_at - f.clock.now, 30_000);
  assert.throws(() => f.exchange(pending.proof), code('TICKET_USED'));
  f.clock.now += 30_001;
  await assert.rejects(f.gateway.call(exchanged.ticket, { name: 'bridge_context', arguments: {} }), code('UNAUTHORIZED'));
  assert.throws(() => f.exchange(pending.proof), code('TICKET_USED'));
});

test('a host cancellation between redemption and execution invalidates the short ticket', async t => {
  const f = await setup(t), pending = await f.issue(), exchanged = f.exchange(pending.proof);
  await f.gateway.hostEvent(f.a.credential, { endpoint_id: 'codex-a', event: f.event({ hook_event_name: 'Stop' }) });
  await assert.rejects(f.gateway.call(exchanged.ticket, { name: 'bridge_context', arguments: {} }), code('UNAUTHORIZED'));
});

test('approval redemption preserves an established live epoch and unexpired receive claim', async t => {
  const f = await setup(t), sent = f.core.publish(f.cb, message({ to: 'codex-a', grant_id: 'b-a' }));
  const receive = await f.issue({ tool_name: CODEX_TOOL_PREFIX + 'bridge_receive', tool_input: { ids: [sent.id] } });
  const batch = await f.proxy.execute('bridge_receive', receive.args), epoch = f.core.endpoint('codex-a').epoch;
  const args = { acknowledgements: [{ id: sent.id, claim_token: batch.items[0].claim.token }] };
  const pending = await f.issue({ tool_name: CODEX_TOOL_PREFIX + 'bridge_ack', tool_input: args });
  f.clock.now += 10_000; await f.proxy.sendHeartbeats(); f.clock.now += 10_000;
  const result = await f.proxy.execute('bridge_ack', pending.args);
  assert.equal(result[0].ok, true); assert.equal(f.core.endpoint('codex-a').epoch, epoch);
});

test('long approval with MCP heartbeats keeps the existing epoch instead of replacing it', async t => {
  const f = await setup(t), initial = await f.issue(); await f.proxy.execute('bridge_context', initial.args);
  const pending = await f.issue(), epoch = f.core.endpoint('codex-a').epoch;
  for (let i = 0; i < 156; i++) { f.clock.now += 20_000; await f.proxy.sendHeartbeats(); }
  assert.equal((await f.proxy.execute('bridge_context', pending.args)).endpoint.epoch, epoch);
});

test('approval-proof redemption does not extend an independently expired read claim', async t => {
  const f = await setup(t), sent = f.core.publish(f.cb, message({ to: 'codex-a', grant_id: 'b-a' }));
  const receive = await f.issue({ tool_name: CODEX_TOOL_PREFIX + 'bridge_receive', tool_input: { ids: [sent.id] } });
  const batch = await f.proxy.execute('bridge_receive', receive.args);
  const pending = await f.issue({ tool_name: CODEX_TOOL_PREFIX + 'bridge_ack', tool_input: {
    acknowledgements: [{ id: sent.id, claim_token: batch.items[0].claim.token }],
  } });
  f.clock.now += 20_000; await f.proxy.sendHeartbeats(); f.clock.now += 20_000;
  const result = await f.proxy.execute('bridge_ack', pending.args);
  assert.equal(result[0].ok, false); assert.equal(result[0].error, 'CLAIM_EXPIRED');
  assert.equal(f.core.db.prepare('SELECT received_at FROM messages WHERE id=?').get(sent.id).received_at, null);
});

test('approval proof expires at two hours and cannot renew a disconnected session', async t => {
  const f = await setup(t), pending = await f.issue(); f.clock.now += 7_200_000;
  await assert.rejects(f.proxy.execute('bridge_context', pending.args), code('UNAUTHORIZED'));
  assert.equal(f.core.endpoint('codex-a').online, false);
});

test('interrupt, end, stop or a new native turn cancels pending approval', async t => {
  const f = await setup(t);
  for (const hook_event_name of ['Interrupt', 'SessionEnd', 'Stop', 'UserPromptSubmit', 'SessionStart']) {
    const pending = await f.issue();
    await f.gateway.hostEvent(f.a.credential, { endpoint_id: 'codex-a', event: f.event({ hook_event_name }) });
    await assert.rejects(f.proxy.execute('bridge_context', pending.args), code('UNAUTHORIZED'));
  }
});

test('a replacement connection or service instance cannot revive pending proof', async t => {
  const f = await setup(t), pending = await f.issue();
  f.core.connect('codex-a', f.a.credential);
  assert.throws(() => f.exchange(pending.proof), code('STALE_CONNECTION'));
  const replacement = new CodexGateway({ core: f.core, now: () => f.clock.now });
  assert.throws(() => replacement.exchange(f.a.credential, { ...pending.proof, name: 'bridge_context', arguments: {} }), code('UNAUTHORIZED'));
});

test('concurrent redemption yields exactly one execution and cannot reuse its native call id', async t => {
  const f = await setup(t), pending = await f.issue(); f.clock.now += 52 * 60_000;
  const results = await Promise.allSettled([f.proxy.execute('bridge_context', pending.args), f.proxy.execute('bridge_context', pending.args)]);
  assert.equal(results.filter(r => r.status === 'fulfilled').length, 1);
  assert.equal(results.find(r => r.status === 'rejected').reason.code, 'TICKET_USED');
  const repeated = await runCodexHook(pending.input, { config: f.config });
  assert.equal(repeated.hookSpecificOutput.permissionDecision, 'deny');
});

test('native hook refuses a service lacking approval-proof support without exposing credentials', async t => {
  const f = await setup(t);
  const output = await runCodexHook(f.event(), { config: f.config, client: { request: async () => ({ ticket: 'old' }) } });
  assert.equal(output.hookSpecificOutput.permissionDecision, 'deny');
  assert.match(output.hookSpecificOutput.permissionDecisionReason, /CAPABILITY_MISSING/);
  assert.ok(!JSON.stringify(output).includes(f.a.credential));
});

test('fresh private bindings are read per call and a changed service cannot receive endpoint credentials', async t => {
  const f = await setup(t), pending = await f.issue(); let current = { service_url: f.base, bindings: {} };
  const proxy = new CodexMcpProxy({ client: f.client, getConfig: async () => current });
  await assert.rejects(proxy.execute('bridge_context', pending.args), code('HOST_CONTEXT_UNVERIFIED'));
  current = { ...f.config, service_url: 'http://127.0.0.1:1' };
  await assert.rejects(proxy.execute('bridge_context', pending.args), code('CONFIG_CONFLICT'));
  current = f.config;
  assert.equal((await proxy.execute('bridge_context', pending.args)).endpoint.id, 'codex-a');
});

test('an already-started MCP process observes its first private registration without a restart', async t => {
  const f = await setup(t), pending = await f.issue(), directory = mkdtempSync(join(tmpdir(), 'codex-first-bind-'));
  t.after(() => { assert.equal(dirname(directory), resolve(tmpdir())); assert.ok(basename(directory).startsWith('codex-first-bind-')); rmSync(directory, { recursive: true, force: true }); });
  const configPath = join(directory, 'bridge.json'); writeFileSync(configPath, JSON.stringify({ service_url: f.base, bindings: {} }));
  const child = spawn(process.execPath, [resolve('src/codex-mcp.mjs')], { env: { ...process.env, LOCAL_BRIDGE_CONFIG: configPath, LOCAL_BRIDGE_URL: f.base }, stdio: ['pipe', 'pipe', 'pipe'], windowsHide: true });
  t.after(() => { if (child.exitCode === null) child.kill(); });
  let buffer = ''; const pendingResponses = new Map();
  child.stdout.on('data', b => {
    buffer += b;
    while (buffer.includes('\n')) {
      const index = buffer.indexOf('\n'), row = JSON.parse(buffer.slice(0, index)); buffer = buffer.slice(index + 1);
      pendingResponses.get(row.id)?.(row);
    }
  });
  const exit = new Promise((done, reject) => { child.once('error', reject); child.once('exit', done); });
  const request = value => new Promise(done => { pendingResponses.set(value.id, done); child.stdin.write(JSON.stringify(value) + '\n'); });
  await request({ jsonrpc: '2.0', id: 1, method: 'initialize', params: { protocolVersion: MCP_VERSION, capabilities: {}, clientInfo: { name: 'first-binding', version: '1' } } });
  child.stdin.write(JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' }) + '\n');
  writeFileSync(configPath, JSON.stringify(f.config));
  const response = await request({ jsonrpc: '2.0', id: 2, method: 'tools/call', params: { name: 'bridge_context', arguments: pending.args } });
  assert.equal(response.result.isError, false, JSON.stringify(response));
  assert.equal(JSON.parse(response.result.content[0].text).endpoint.id, 'codex-a');
  child.stdin.end(); assert.equal(await exit, 0);
});

test('MCP stdio redeems delayed native proof using private config, ignoring startup thread identity', async t => {
  const f = await setup(t), pending = await f.issue(), directory = mkdtempSync(join(tmpdir(), 'codex-review-stdio-'));
  t.after(() => { assert.equal(dirname(directory), resolve(tmpdir())); assert.ok(basename(directory).startsWith('codex-review-stdio-')); rmSync(directory, { recursive: true, force: true }); });
  const configPath = join(directory, 'bridge.json'); writeFileSync(configPath, JSON.stringify(f.config));
  f.clock.now += 52 * 60_000;
  const child = spawn(process.execPath, [resolve('src/codex-mcp.mjs')], {
    env: { ...process.env, LOCAL_BRIDGE_CONFIG: configPath, LOCAL_BRIDGE_URL: f.base, CODEX_THREAD_ID: 'wrong-startup-thread' }, stdio: ['pipe', 'pipe', 'pipe'], windowsHide: true,
  });
  t.after(() => { if (child.exitCode === null) child.kill(); });
  let output = '', error = ''; child.stdout.on('data', b => { output += b; }); child.stderr.on('data', b => { error += b; });
  const exit = new Promise((resolveExit, reject) => { child.once('error', reject); child.once('exit', resolveExit); });
  child.stdin.end([
    { jsonrpc: '2.0', id: 1, method: 'initialize', params: { protocolVersion: MCP_VERSION, capabilities: {}, clientInfo: { name: 'review-fixture', version: '1' } } },
    { jsonrpc: '2.0', method: 'notifications/initialized' },
    { jsonrpc: '2.0', id: 2, method: 'tools/call', params: { name: 'bridge_context', arguments: pending.args } },
  ].map(JSON.stringify).join('\n') + '\n');
  assert.equal(await exit, 0, error);
  const response = output.trim().split('\n').map(JSON.parse).find(r => r.id === 2);
  assert.equal(response.result.isError, false, output);
  assert.equal(JSON.parse(response.result.content[0].text).endpoint.id, 'codex-a');
  assert.ok(!output.includes(f.a.credential)); assert.ok(!output.includes(pending.proof.proof));
});

test('a fresh MCP process reads the switched private address and the stale process rejects the new proof', async t => {
  const original = await setup(t), candidate = await setup(t);
  candidate.gateway.settingsUrl = candidate.base;
  const directory = mkdtempSync(join(tmpdir(), 'codex-route-reload-'));
  const path = join(directory, 'bridge.json');
  const children = [];
  t.after(() => {
    for (const child of children) if (child.exitCode === null) child.kill();
    assert.equal(dirname(directory), resolve(tmpdir()));
    assert.ok(basename(directory).startsWith('codex-route-reload-'));
    rmSync(directory, { recursive: true, force: true });
  });
  async function launch() {
    const env = { ...process.env, LOCAL_BRIDGE_CONFIG: path };
    delete env.LOCAL_BRIDGE_URL;
    const child = spawn(process.execPath, [resolve('src/codex-mcp.mjs')], { env, stdio: ['pipe', 'pipe', 'pipe'], windowsHide: true });
    children.push(child);
    const replies = createInterface({ input: child.stdout })[Symbol.asyncIterator]();
    const exited = new Promise((done, reject) => { child.once('error', reject); child.once('exit', done); });
    const write = row => child.stdin.write(JSON.stringify({ jsonrpc: '2.0', ...row }) + '\n');
    const call = async (id, method, params) => {
      write({ id, method, params });
      const { value } = await replies.next();
      const response = JSON.parse(value);
      assert.equal(response.id, id);
      return response.result;
    };
    const initialized = await call(1, 'initialize', { protocolVersion: MCP_VERSION, capabilities: {}, clientInfo: { name: 'reload-fixture', version: '1' } });
    assert.equal(initialized.protocolVersion, MCP_VERSION);
    write({ method: 'notifications/initialized' });
    return { call, async close() { child.stdin.end(); assert.equal(await exited, 0); } };
  }
  writeFileSync(path, JSON.stringify(original.config));
  const stale = await launch();
  const pending = await candidate.issue();
  writeFileSync(path, JSON.stringify(candidate.config));
  const request = { name: 'bridge_context', arguments: pending.args };
  const rejected = await stale.call(2, 'tools/call', request);
  assert.equal(rejected.isError, true);
  assert.equal(JSON.parse(rejected.content[0].text).error, 'CONFIG_CONFLICT');
  await stale.close();

  const fresh = await launch();
  const accepted = await fresh.call(2, 'tools/call', request);
  assert.equal(accepted.isError, false);
  const context = JSON.parse(accepted.content[0].text);
  assert.equal(context.endpoint.id, 'codex-a');
  assert.equal(context.settings_url, candidate.base);
  await fresh.close();
});

test('generated MCP manifest supplies private config without inheriting the hook PLUGIN_DATA environment', async t => {
  const f = await setup(t), pending = await f.issue(), directory = mkdtempSync(join(tmpdir(), 'codex-config-wiring-'));
  t.after(() => { assert.equal(dirname(directory), resolve(tmpdir())); assert.ok(basename(directory).startsWith('codex-config-wiring-')); rmSync(directory, { recursive: true, force: true }); });
  const privateData = join(directory, 'private profile'); mkdirSync(privateData);
  writeFileSync(join(privateData, 'bridge.json'), JSON.stringify(f.config));
  const output = join(directory, 'out'); await build(output, { pluginData: privateData });
  const mcp = JSON.parse(readFileSync(join(output, 'plugins/local-bridge/mcp.json'), 'utf8')).mcpServers.local_bridge;
  assert.equal(mcp.env.LOCAL_BRIDGE_CONFIG, join(privateData, 'bridge.json'));
  assert.ok(!JSON.stringify(mcp).includes(f.a.credential));
  const env = { ...process.env, ...mcp.env, LOCAL_BRIDGE_URL: f.base }; delete env.PLUGIN_DATA;
  const child = spawn(mcp.command, mcp.args, { cwd: mcp.cwd, env, windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'] });
  t.after(() => { if (child.exitCode === null) child.kill(); });
  let stdout = '', stderr = ''; child.stdout.on('data', bytes => { stdout += bytes; }); child.stderr.on('data', bytes => { stderr += bytes; });
  const exit = new Promise((done, reject) => { child.once('error', reject); child.once('exit', done); });
  child.stdin.end([
    { jsonrpc: '2.0', id: 1, method: 'initialize', params: { protocolVersion: MCP_VERSION, capabilities: {}, clientInfo: { name: 'manifest-wiring-fixture', version: '1' } } },
    { jsonrpc: '2.0', method: 'notifications/initialized' },
    { jsonrpc: '2.0', id: 2, method: 'tools/call', params: { name: 'bridge_context', arguments: pending.args } },
  ].map(JSON.stringify).join('\n') + '\n');
  assert.equal(await exit, 0, stderr);
  const response = stdout.trim().split('\n').map(JSON.parse).find(row => row.id === 2);
  assert.equal(response.result.isError, false, stdout);
  assert.equal(JSON.parse(response.result.content[0].text).endpoint.id, 'codex-a');
});
