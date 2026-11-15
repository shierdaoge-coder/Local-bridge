import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, existsSync, readdirSync, rmSync, mkdirSync, writeFileSync } from 'node:fs';
import { join, dirname, basename, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { fixture, ADMIN, code } from './helpers.mjs';
import { provisionCodex } from '../src/codex-setup.mjs';
import { loadCodexConfig } from '../src/codex-config.mjs';
import { CodexGateway } from '../src/codex-gateway.mjs';
import { createBridgeServer } from '../src/server.mjs';
import { BridgeClient } from '../src/client.mjs';
import { runCodexHook } from '../src/codex-hook.mjs';
import { CodexMcpProxy } from '../src/codex-mcp.mjs';
import { spawn } from 'node:child_process';

function directory(t) {
  const path = mkdtempSync(join(tmpdir(), 'local-bridge-codex-setup-'));
  t.after(() => { assert.equal(dirname(path), resolve(tmpdir())); assert.ok(basename(path).startsWith('local-bridge-codex-setup-')); rmSync(path, { recursive: true, force: true }); }); return path;
}
const options = path => ({ session: '00000000-0000-4000-8000-000000000001', pluginData: join(path, 'private profile'), cwd: path, transcript: join(path, 'rollout.jsonl') });

test('Codex setup dry run makes no registration, file, credential or native delivery change', async t => {
  const path = directory(t), args = options(path), result = await provisionCodex(args);
  assert.equal(result.writes, false); assert.equal(result.native_delivery_enabled, false); assert.equal(existsSync(args.pluginData), false);
  assert.equal(Object.hasOwn(result.endpoint, 'credential'), false);
  await assert.rejects(provisionCodex({ ...args, session: 'ambiguous thread name' }), code('INVALID_INPUT'));
  await assert.rejects(provisionCodex({ ...args, pluginData: 'relative' }), code('INVALID_INPUT'));
});

test('Codex operator setup saves private credentials, registers no grant, and supports native-hook/MCP calls', async t => {
  const path = directory(t), args = options(path), f = fixture(), gateway = new CodexGateway({ core: f.core, now: () => f.clock.now });
  const server = createBridgeServer({ core: f.core, adminToken: ADMIN, codex: gateway, port: 0 }); const base = await server.listen();
  t.after(async () => { await server.close(); f.core.close(); });
  const admin = new BridgeClient(base, { adminToken: ADMIN }), grants = f.core.db.prepare('SELECT COUNT(*) AS n FROM grants').get().n;
  const result = await provisionCodex({ ...args, serviceUrl: base, write: true }, { client: admin });
  const config = await loadCodexConfig({ PLUGIN_DATA: args.pluginData }), binding = config.bindings[args.session];
  assert.equal(result.native_delivery_enabled, false); assert.ok(!JSON.stringify(result).includes(binding.credential));
  assert.equal(f.core.authenticateEndpoint(binding.endpoint_id, binding.credential).capabilities.can_enqueue, false);
  assert.equal(f.core.db.prepare('SELECT COUNT(*) AS n FROM grants').get().n, grants);
  assert.equal(readdirSync(args.pluginData).some(name => name.startsWith('.bridge-pending-') || name === '.bridge-setup.lock'), false);
  const hook = await runCodexHook({ session_id: args.session, cwd: args.cwd, transcript_path: args.transcript, hook_event_name: 'PreToolUse', tool_name: 'mcp__local_bridge__bridge_context', tool_use_id: 'setup-fixture-call', tool_input: {} }, { config });
  assert.equal(hook.hookSpecificOutput.permissionDecision, 'allow');
  const context = await new CodexMcpProxy({ client: new BridgeClient(base), config }).execute('bridge_context', hook.hookSpecificOutput.updatedInput); assert.equal(context.endpoint.id, binding.endpoint_id);
  const command = `""${process.execPath}" "${resolve('src/codex-hook.mjs')}""`;
  const child = spawn(process.env.ComSpec ?? 'C:/Windows/System32/cmd.exe', ['/d', '/s', '/c', command], { windowsHide: true, windowsVerbatimArguments: true, env: { ...process.env, PLUGIN_DATA: args.pluginData }, stdio: ['pipe', 'pipe', 'pipe'] });
  let stdout = '', stderr = ''; child.stdout.on('data', chunk => { stdout += chunk; }); child.stderr.on('data', chunk => { stderr += chunk; });
  child.stdin.end(JSON.stringify({ session_id: args.session, cwd: args.cwd, transcript_path: args.transcript, hook_event_name: 'PreToolUse', tool_name: 'mcp__local_bridge__bridge_context', tool_use_id: 'real-hook-stdin', tool_input: {} }));
  const exit = await new Promise((resolve, reject) => { child.once('close', resolve); child.once('error', reject); });
  assert.equal(exit, 0, stderr); assert.equal(JSON.parse(stdout).hookSpecificOutput.permissionDecision, 'allow'); assert.ok(!stdout.includes(binding.credential));
  const before = readFileSync(join(args.pluginData, 'bridge.json'), 'utf8');
  await assert.rejects(provisionCodex({ ...args, serviceUrl: base, write: true }, { client: admin }), code('ALREADY_REGISTERED'));
  assert.equal(readFileSync(join(args.pluginData, 'bridge.json'), 'utf8'), before);
});

test('Codex registration uncertainty preserves a private credential candidate without replacing config', async t => {
  const path = directory(t), args = options(path); let supplied;
  const client = { request: async (path, request) => { if (path === '/api/health') return { features: { codex_hooks: true, codex_approval_redemption: true, provided_endpoint_credentials: true } }; supplied = request.body.credential; throw new Error('response lost'); } };
  await assert.rejects(provisionCodex({ ...args, write: true }, { client }), /response lost/);
  assert.equal(existsSync(join(args.pluginData, 'bridge.json')), false);
  const names = readdirSync(args.pluginData), pending = names.find(name => name.startsWith('.bridge-pending-'));
  assert.ok(pending); assert.equal(names.includes('.bridge-setup.lock'), false);
  const candidate = JSON.parse(readFileSync(join(args.pluginData, pending), 'utf8'));
  assert.equal(candidate.bindings[args.session].credential, supplied); assert.match(supplied, /^[A-Za-z0-9_-]{43}$/);
});

test('Codex setup rejects switching existing bindings to another service before registering', async t => {
  const path = directory(t), args = options(path); mkdirSync(args.pluginData);
  writeFileSync(join(args.pluginData, 'bridge.json'), JSON.stringify({ service_url: 'http://127.0.0.1:4999', bindings: {} }));
  let called = false;
  await assert.rejects(provisionCodex({ ...args, write: true }, { client: { request: async path => { if (path === '/api/health') return { features: { codex_hooks: true, codex_approval_redemption: true, provided_endpoint_credentials: true } }; called = true; } } }), code('CONFIG_CONFLICT'));
  assert.equal(called, false);
});

test('Codex setup refuses the older passive service before saving secrets or registering an endpoint', async t => {
  const path = directory(t), args = options(path); let registrations = 0;
  const client = { request: async path => { if (path === '/api/health') return { ok: true, protocol: 'local-bridge.session-transport/0.1' }; registrations++; } };
  await assert.rejects(provisionCodex({ ...args, write: true }, { client }), code('CAPABILITY_MISSING'));
  assert.equal(registrations, 0); assert.equal(existsSync(args.pluginData), false);
});
