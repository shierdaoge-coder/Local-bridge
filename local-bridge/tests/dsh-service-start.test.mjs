import test from 'node:test';
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { randomBytes, randomInt } from 'node:crypto';
import { once } from 'node:events';
import { mkdir, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:net';
import { dirname, join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { promisify } from 'node:util';
import { createCandidateService } from '../experimental/source-automation/candidate-service.mjs';
import { apply } from '../src/dsh-plugin.mjs';
import { dailyUseHealth } from '../scripts/daily-use-supervisor.mjs';

const project = resolve(import.meta.dirname, '..');
const runNode = promisify(execFile);
const secret = () => randomBytes(32).toString('base64url');

async function waitFor(check, message, timeout = 10_000) {
  const until = Date.now() + timeout;
  while (!(await check())) {
    assert.ok(Date.now() < until, message);
    await delay(50);
  }
}

function running(pid) {
  try { process.kill(pid, 0); return true; }
  catch (error) { if (error.code === 'ESRCH') return false; throw error; }
}

test('a DSH heartbeat starts the shared service, respects stop after reload, and resumes on request', async t => {
  await mkdir(join(project, '.runtime'), { recursive: true });
  const runtime = await realpath(join(project, '.runtime'));
  const trial = join(runtime, `daily-use-${Date.now()}${randomInt(100000, 1000000)}`);
  await mkdir(trial);
  const command = (...args) => runNode(process.execPath, [join(project, 'scripts', 'daily-use.mjs'), args[0], trial, ...args.slice(1)], { windowsHide: true });
  const pids = new Set();
  let unload;
  t.after(async () => {
    await unload?.();
    if ([...pids].some(running)) {
      await command('stop');
      await waitFor(() => ![...pids].some(running), 'Isolated supervisor did not stop', 20_000);
    }
    const resolved = await realpath(trial);
    assert.equal(resolved, trial);
    assert.equal(dirname(resolved), runtime);
    await rm(resolved, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 });
  });

  const socket = createServer().listen(0, '127.0.0.1');
  await once(socket, 'listening');
  const { port } = socket.address();
  await promisify(socket.close.bind(socket))();
  const url = `http://127.0.0.1:${port}`;
  const directory = join(trial, 'data'), sourcePath = join(trial, 'codex.jsonl'), bindingFile = join(trial, 'bridge.json');
  await mkdir(directory);
  const codex = { id: 'isolated-codex', harness: 'codex', native_session_id: 'isolated-native-codex', credential: secret() };
  const dsh = { id: 'isolated-dsh', harness: 'dsh', native_session_id: 'isolated-native-dsh', credential: secret() };
  const author = 'isolated-dsh-start-test';
  const config = { directory, port, workspace: author, adminToken: secret(), nativeAdapters: true,
    endpoints: [codex, dsh], archives: [{ id: 'isolated-archive', owner: codex.id, generation: 'initial' }],
    bindings: [{ archive_id: 'isolated-archive', path: sourcePath, format: 'codex-jsonl', initial_mode: 'from_now', author_ref: author }],
    grants: [{ id: 'isolated-direction', from: codex.id, to: dsh.id, content_types: ['history_sync'], allow_wake: false, author_ref: author }],
    subscriptions: [{ id: 'isolated-subscription', archive_id: 'isolated-archive', to: dsh.id, grant_id: 'isolated-direction' }],
    pumpOptions: { delayMs: 5, pollMs: 30 } };
  const save = (file, value) => writeFile(join(trial, file), JSON.stringify(value) + '\n', { mode: 0o600 });
  await Promise.all([
    save('config.json', { ...config, endpoints: [] }), save('private-endpoints.json', [codex, dsh]),
    save('codex.jsonl', { type: 'session_meta', payload: { id: codex.native_session_id } }),
    save('bridge.json', { service_url: url, bindings: { [dsh.native_session_id]: { endpoint_id: dsh.id, credential: dsh.credential } } }),
    save('service-start.json', { node_path: process.execPath, trial_directory: trial }),
  ]);
  const preparedService = await createCandidateService({ ...config, port: 0 });
  await preparedService.close();

  const tools = new Map(), agents = new Map(), warnings = [];
  const ctx = { agents, tools: { register(tool) { tools.set(tool.name, tool); return () => tools.delete(tool.name); } },
    logger: { info(_message, { pid }) { pids.add(pid); }, warn(_message, { code } = {}) { warnings.push(code); } } };
  const load = () => apply(ctx, { binding_file: bindingFile,
    loadNative: async () => ({ createUserMessage: () => assert.fail('The heartbeat must not send a model message'), adoptSessionEvent: () => {} }) });
  const events = async () => {
    const log = await readFile(join(trial, 'supervisor.jsonl'), 'utf8').catch(error => { if (error.code !== 'ENOENT') throw error; return ''; });
    return log.split('\n').slice(0, -1).map(line => JSON.parse(line).event);
  };
  const healthy = async () => (await dailyUseHealth(url, 500)).healthy;

  assert.equal(await healthy(), false);
  unload = await load();
  await waitFor(healthy, 'DSH did not start the isolated bridge');
  const agent = { status: 'idle', session: { id: dsh.native_session_id }, ctx: { on: () => () => {} } };
  agents.set(dsh.native_session_id, agent);
  const context = JSON.parse(await tools.get('bridge_context').execute({}, { agent }));
  assert.equal(context.result?.endpoint.id, dsh.id);
  assert.equal((await events()).filter(event => event === 'worker_started').length, 1);

  await command('stop');
  await waitFor(async () => (await events()).includes('supervisor_stopped') && ![...pids].some(running), 'Explicit stop was not completed');
  await unload();
  await command('snapshot');
  await command('supervise', '--autostart');
  assert.ok((await events()).includes('automatic_start_skipped'));
  warnings.length = 0;
  unload = await load();
  await waitFor(() => warnings.includes('ECONNREFUSED'), 'Reloaded DSH did not observe the stopped service');
  await delay(1100);
  assert.equal(await healthy(), false);
  assert.equal(pids.size, 1);

  await command('resume');
  await waitFor(healthy, 'DSH did not restart the bridge after resume');
  assert.equal((await events()).filter(event => event === 'worker_started').length, 2);
});
