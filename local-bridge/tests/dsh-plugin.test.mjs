import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname, basename, resolve } from 'node:path';
import { apply } from '../src/dsh-plugin.mjs';

test('DSH plugin registers seven tools, binds callers from exec, and leaves unregistered sessions unbound', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'local-bridge-dsh-plugin-'));
  const configPath = join(dir, 'bridge.json');
  await writeFile(configPath, JSON.stringify({ service_url: 'http://127.0.0.1:4776', bindings: {
    session_a: { endpoint_id: 'endpoint-a', credential: 'secret-a' },
  } }));
  const restoreFetch = globalThis.fetch;
  const calls = [];
  globalThis.fetch = async (url, options) => {
    calls.push({ url: String(url), options, body: JSON.parse(options.body) });
    return new Response(JSON.stringify({ ok: true, result: { state: 'idle', command: null } }), { status: 200, headers: { 'content-type': 'application/json' } });
  };
  t.after(async () => { globalThis.fetch = restoreFetch; assert.equal(dirname(resolve(dir)), resolve(tmpdir())); assert.ok(basename(dir).startsWith('local-bridge-dsh-plugin-')); await rm(dir, { recursive: true, force: true }); });

  const tools = new Map();
  const agent = { status: 'idle', session: { id: 'session_a' }, ctx: { on: () => () => {} } };
  const ctx = { agents: { get: id => id === 'session_a' ? agent : undefined }, tools: { register: tool => { tools.set(tool.name, tool); return () => tools.delete(tool.name); } } };
  const unload = await apply(ctx, { binding_file: configPath, loadNative: async () => ({ createUserMessage: () => {}, adoptSessionEvent: () => {} }) });
  t.after(unload);
  await new Promise(resolve => setTimeout(resolve, 20));
  assert.equal(tools.size, 7);
  const unknown = await tools.get('bridge_context').execute({}, { agent: { session: { id: 'session_b' } } });
  assert.deepEqual(JSON.parse(unknown), { ok: false, error: 'NOT_REGISTERED', native_session_id: 'session_b', binding_file: configPath, settings_url: 'http://127.0.0.1:4776' });
  await tools.get('bridge_context').execute({}, { agent: { session: { id: 'session_a' } } });
  const call = calls.find(item => item.url.endsWith('/api/dsh/tool-call'));
  assert.equal(call.body.native_session_id, 'session_a');
  assert.equal(call.body.endpoint_id, 'endpoint-a');
  assert.equal(call.options.headers.Authorization, 'Bearer secret-a');
  await unload();
  assert.equal(tools.size, 0);
  assert.ok(calls.some(item => item.url.endsWith('/api/dsh/pulse') && item.body.state === 'offline'));
});

test('DSH plugin starts with absent private bindings and does not pulse an unbound session', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'local-bridge-dsh-empty-'));
  const restoreFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response(JSON.stringify({ ok: true, result: { state: 'offline', command: null } }), { status: 200 });
  t.after(async () => { globalThis.fetch = restoreFetch; assert.equal(dirname(resolve(dir)), resolve(tmpdir())); assert.ok(basename(dir).startsWith('local-bridge-dsh-empty-')); await rm(dir, { recursive: true, force: true }); });
  const tools = [];
  const ctx = { agents: { get: () => ({ session: { id: 'unbound' }, status: 'idle' }) }, tools: { register: tool => { tools.push(tool); return () => {} } } };
  const unload = await apply(ctx, { binding_file: join(dir, 'missing.json'), loadNative: async () => ({ createUserMessage: () => {}, adoptSessionEvent: () => {} }) });
  t.after(unload);
  assert.equal(tools.length, 7);
  const result = JSON.parse(await tools[0].execute({}, { agent: { session: { id: 'unbound' } } }));
  assert.equal(result.error, 'NOT_REGISTERED');
  await unload();
});
