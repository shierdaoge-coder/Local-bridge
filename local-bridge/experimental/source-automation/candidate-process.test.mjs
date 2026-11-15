import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, appendFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname, resolve } from 'node:path';
import { fork } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { BridgeClient } from '../../src/client.mjs';

test('a fresh Node candidate process retains cursor and does not default-replay read history', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'local-bridge-candidate-process-'));
  const path = join(directory, 'source.jsonl'), configPath = join(directory, 'private-config.json');
  const children = [];
  async function stop(child) {
    if (child.exitCode !== null) return;
    const exited = new Promise((resolve, reject) => { child.once('exit', resolve); child.once('error', reject); });
    child.send({ command: 'shutdown' });
    const result = await Promise.race([exited.then(() => true), delay(3000).then(() => false)]);
    if (!result) { child.kill(); await exited; assert.fail('Candidate did not close gracefully'); }
  }
  t.after(async () => {
    for (const child of children) await stop(child);
    assert.equal(dirname(directory), resolve(tmpdir())); assert.ok(directory.startsWith(join(resolve(tmpdir()), 'local-bridge-candidate-process-')));
    await rm(directory, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 });
  });
  const a = randomBytes(32).toString('base64url'), b = randomBytes(32).toString('base64url'), adminToken = randomBytes(32).toString('base64url');
  const config = { directory, adminToken,
    endpoints: [{ id: 'a', harness: 'codex', native_session_id: 'process-native', credential: a }, { id: 'b', harness: 'dsh', native_session_id: 'process-target', credential: b }],
    archives: [{ id: 'archive', owner: 'a', generation: 'g1' }],
    bindings: [{ archive_id: 'archive', path, format: 'codex-jsonl', initial_mode: 'from_now', author_ref: 'isolated-process-test' }],
    grants: [{ id: 'a-b', from: 'a', to: 'b', content_types: ['history_sync'], allow_wake: false, author_ref: 'isolated-process-test' }],
    subscriptions: [{ id: 'sub', archive_id: 'archive', to: 'b', grant_id: 'a-b' }], pumpOptions: { delayMs: 5, pollMs: 25 } };
  await writeFile(path, JSON.stringify({ type: 'session_meta', payload: { id: 'process-native' } }) + '\n');
  await writeFile(configPath, JSON.stringify(config), { mode: 0o600 });
  async function start() {
    const child = fork(resolve(import.meta.dirname, 'candidate-main.mjs'), ['--config', configPath],
      { execPath: process.execPath, execArgv: ['--max-old-space-size=96'], stdio: ['ignore', 'pipe', 'pipe', 'ipc'], windowsHide: true });
    children.push(child);
    let stdout = '', stderr = '';
    child.stdout.on('data', b => { stdout = (stdout + b).slice(-2048); });
    child.stderr.on('data', b => { stderr = (stderr + b).slice(-2048); });
    const ready = new Promise((resolve, reject) => {
      child.once('error', reject); child.once('exit', code => reject(new Error(`Candidate exited before ready: ${code}`)));
      child.on('message', value => { if (value?.event === 'ready') resolve(value.url); });
    });
    const url = await Promise.race([ready, delay(5000).then(() => { throw new Error('Candidate startup timeout'); })]);
    assert.ok(!stdout.includes(adminToken) && !stderr.includes(adminToken));
    return { child, url };
  }
  const first = await start(), admin = new BridgeClient(first.url, { adminToken });
  const recipient = new BridgeClient(first.url); await recipient.connect('b', b);
  // Finish the explicit initial boundary before appending the synthetic increment.
  const deadline = Date.now() + 3000;
  for (;;) {
    const state = await admin.request('/api/sources/archive', { admin: true });
    if (state.checkpoint) break;
    assert.ok(Date.now() < deadline); await delay(10);
  }
  await appendFile(path, JSON.stringify({ type: 'response_item', payload: { type: 'message', role: 'user', content: [{ type: 'input_text', text: 'SYNTHETIC process increment' }] } }) + '\n');
  let items;
  for (;;) {
    items = (await recipient.receive()).items; if (items.length) break;
    assert.ok(Date.now() < deadline); await delay(10);
  }
  assert.equal(items[0].payload.records[0].text, 'SYNTHETIC process increment');
  await recipient.acknowledge(items.map(i => ({ id: i.id, claim_token: i.claim.token })));
  await stop(first.child);
  const second = await start(); assert.notEqual(second.child.pid, first.child.pid);
  const restored = new BridgeClient(second.url); await restored.connect('b', b);
  assert.equal((await restored.request('/api/cursor?archive_id=archive&generation=g1')).read_through, 0);
  assert.equal((await restored.receive()).items.length, 0);
  const admin2 = new BridgeClient(second.url, { adminToken });
  const scan = await admin2.request('/api/sources/archive/scan', { admin: true, method: 'POST', body: {} });
  assert.equal(scan.records, 0); assert.equal(scan.queued.length, 0);
  await stop(second.child);
});
