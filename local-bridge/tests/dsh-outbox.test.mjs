import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { DatabaseSync } from 'node:sqlite';
import { setTimeout as delay } from 'node:timers/promises';
import { buildDsh } from '../scripts/build-dsh.mjs';
import { DshGateway } from '../src/dsh-gateway.mjs';
import { createBridgeServer } from '../src/server.mjs';
import { dshToolError } from '../src/dsh-outbox.mjs';
import { fixture, ADMIN } from './helpers.mjs';

async function until(check) {
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    if (await check()) return;
    await delay(50);
  }
  assert.fail('Outbox did not reach the expected state');
}

test('packaged DSH outbox survives an offline restart and retries a lost receipt without duplicate delivery', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'local-bridge-outbox-'));
  const f = fixture(), originalFetch = globalThis.fetch;
  const tools = new Map();
  const agent = { status: 'idle', session: { id: 'native-b' }, ctx: { on: () => () => {} },
    followup: () => assert.fail('The outbox must not start a model turn') };
  let server, gateway, unload;
  t.after(async () => {
    globalThis.fetch = originalFetch;
    await unload?.(); gateway?.close(); await server?.close(); f.core.close();
    const target = await realpath(directory);
    assert.equal(dirname(target), await realpath(tmpdir()));
    assert.match(basename(target), /^local-bridge-outbox-/);
    await rm(target, { recursive: true, force: true, maxRetries: 3 });
  });
  async function start(port) {
    gateway = new DshGateway({ core: f.core });
    server = createBridgeServer({ core: f.core, adminToken: ADMIN, dsh: gateway, port });
    return server.listen();
  }
  const url = await start(0);
  await server.close(); gateway.close();
  const bundle = await buildDsh(join(directory, 'dsh'), { sdkEntry: join(directory, 'fixture-sdk', 'index.js') });
  const { apply } = await import(pathToFileURL(join(bundle.root, 'src/dsh-plugin.mjs')).href);
  async function load(credential) {
    await writeFile(bundle.binding_file, JSON.stringify({ service_url: url,
      bindings: { 'native-b': { endpoint_id: f.b.id, credential } } }));
    unload = await apply({ agents: new Map([['native-b', agent]]), tools: { register(tool) {
      tools.set(tool.name, tool); return () => tools.delete(tool.name);
    } } }, { binding_file: bundle.binding_file, loadNative: async () => ({ createUserMessage() {}, adoptSessionEvent() {} }) });
  }
  const call = async (name, args = {}) => JSON.parse(await tools.get(name).execute(args, { agent }));
  const publish = id => ({ id, to: f.a.id, grant_id: 'b-a',
    payload: { kind: 'event', topic: 'outbox.test', correlation_id: id, text: 'Synthetic outbox message', source_refs: [] } });
  const committed = id => f.core.db.prepare('SELECT count(*) AS n FROM messages WHERE id=?').get(id).n;
  const outboxFile = join(dirname(bundle.binding_file), 'bridge-outbox.sqlite');
  await load(f.b.credential);
  const contextFailure = await call('bridge_context');
  assert.equal(contextFailure.error, 'ECONNREFUSED');
  const message = publish('persist-through-reopen');
  const pending = await call('bridge_publish', message);
  assert.equal(pending.ok, true);
  assert.equal(pending.result.delivery, 'pending_local');
  assert.equal(pending.result.error, 'ECONNREFUSED');
  assert.equal(committed(message.id), 0);
  const local = new DatabaseSync(outboxFile, { readOnly: true });
  try {
    const record = local.prepare('SELECT * FROM outbox WHERE id=?').get(message.id);
    assert.deepEqual(JSON.parse(record.args), message);
    assert.ok(!JSON.stringify(record).includes(f.b.credential));
  } finally { local.close(); }
  assert.equal((await call('bridge_publish', { ...message, to: 'other' })).error, 'ID_CONFLICT');

  await unload();
  await load('different-operator-binding');
  assert.equal((await call('bridge_status', { id: message.id })).error, 'ECONNREFUSED');
  await unload();
  await load(f.b.credential);
  assert.equal((await call('bridge_status', { id: message.id })).result.delivery, 'pending_local');
  assert.equal(await start(Number(new URL(url).port)), url);
  await until(async () => (await call('bridge_status', { id: message.id })).result?.delivery === 'queued');
  assert.equal(committed(message.id), 1);

  const uncertain = publish('commit-with-lost-reply');
  let sends = 0;
  globalThis.fetch = async (address, options) => {
    const response = await originalFetch(address, options);
    if (String(address) === url + '/api/dsh/tool-call' && options?.body) {
      const request = JSON.parse(options.body);
      if (request.name === 'bridge_publish' && request.arguments.id === uncertain.id && ++sends === 1) {
        await response.arrayBuffer();
        throw new TypeError('fetch failed', { cause: Object.assign(new Error('synthetic lost reply'), { code: 'ECONNRESET' }) });
      }
    }
    return response;
  };
  const lost = await call('bridge_publish', uncertain);
  assert.equal(lost.result.delivery, 'pending_local');
  assert.equal(lost.result.error, 'ECONNRESET');
  assert.equal(committed(uncertain.id), 1);
  await until(async () => (await call('bridge_status', { id: uncertain.id })).result?.delivery === 'queued');
  assert.equal(sends, 2);
  assert.equal(committed(uncertain.id), 1);
  const received = f.core.receive(f.ca, { ids: [message.id, uncertain.id] }).items;
  assert.deepEqual(received.map(item => item.id), [message.id, uncertain.id]);

  const denied = { ...publish('revoked-grant'), grant_id: 'missing-grant' };
  assert.equal((await call('bridge_publish', denied)).error, 'NOT_AUTHORIZED');
  assert.equal((await call('bridge_status', { id: denied.id })).result.delivery, 'local_failed');
  assert.equal(committed(denied.id), 0);
  assert.equal(dshToolError(new DOMException('request timed out', 'TimeoutError')).code, 'ETIMEDOUT');
});
