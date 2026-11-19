import test from 'node:test';
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { once } from 'node:events';
import { appendFile, mkdir, mkdtemp, realpath, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { promisify } from 'node:util';
import { zstdCompressSync } from 'node:zlib';
import { createCandidateService } from '../experimental/source-automation/candidate-service.mjs';
import { publishNativeMessage, relaySession, startNativeRelay } from '../scripts/relay-session.mjs';
import { BridgeClient } from '../src/client.mjs';
import { planSessionPairs } from '../src/session-pairs.mjs';
import { PROTOCOL } from '../src/protocol.mjs';
import { code } from './helpers.mjs';

const secret = () => randomBytes(32).toString('base64url');
const exec = promisify(execFile);
const evidence = { queue_ref: 'enqueue', turn_ref: 'turn', native_start: 1, native_complete: 5 };

async function waitFor(probe, description) {
  const end = Date.now() + 5000;
  for (;;) {
    if (await probe()) return;
    assert.ok(Date.now() < end, description);
    await delay(25);
  }
}

async function setup(t, { automatic = false, connectedCodex = false } = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'local-bridge-relay-test-'));
  let service, relay;
  async function stopRelay() { await relay?.close(); relay = undefined; }
  async function stopService() { await service?.close(); service = undefined; }
  t.after(async () => {
    await stopRelay();
    await stopService();
    const actual = await realpath(directory);
    assert.equal(dirname(actual), resolve(tmpdir()));
    assert.match(basename(actual), /^local-bridge-relay-test-/);
    await rm(actual, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 });
  });
  const parking = createServer().listen(0, '127.0.0.1');
  await once(parking, 'listening');
  const { port } = parking.address();
  await new Promise(done => parking.close(done));
  const source = { id: 'first', native_session_id: 'native-first', harness: connectedCodex ? 'dsh' : 'codex', credential: secret() };
  const target = { id: 'second', native_session_id: 'session-second', harness: connectedCodex ? 'codex' : 'dsh', credential: secret() };
  const home = join(directory, 'home'), data = join(directory, 'data');
  const archive = connectedCodex ? join(home, 'desktop.jsonl') : join(home, 'sessions', 'fixture', target.native_session_id, 'session.v4.jsonl.zstd');
  await Promise.all([mkdir(dirname(archive), { recursive: true }), mkdir(data)]);
  const header = connectedCodex ? { type: 'session_meta', payload: { id: target.native_session_id } }
    : { type: 'session', version: 4, id: target.native_session_id };
  const headerLine = JSON.stringify(header) + '\n';
  await writeFile(archive, connectedCodex ? headerLine : zstdCompressSync(Buffer.from(headerLine)));
  const config = { directory: data, port, adminToken: secret(), nativeAdapters: true, endpoints: [source, target],
    sessionPairs: [{ id: 'local-fixture', author_ref: 'operator', directions: [{ from: source.id, to: target.id, native_delivery: true }] }],
    nativeTargets: { [target.id]: connectedCodex
      ? { archive, connection: { url: 'ws://127.0.0.1:1', credential_file: join(directory, 'fixture-token') } }
      : { archive, home, cwd: directory } } };
  const archiveId = 'source-archive', sourcePath = join(directory, 'source.jsonl');
  if (automatic) {
    await writeFile(sourcePath, JSON.stringify({ type: 'session_meta', payload: { id: source.native_session_id } }) + '\n');
    Object.assign(config, { nativeRelay: true, archives: [{ id: archiveId, owner: source.id, generation: 'g1' }],
      bindings: [{ archive_id: archiveId, path: sourcePath, format: 'codex-jsonl', initial_mode: 'from_now', author_ref: 'operator' }] });
    config.sessionPairs[0].directions[0].archive_id = archiveId;
    config.sessionPairs[0].directions.push({ from: target.id, to: source.id });
  }
  await writeFile(join(directory, 'config.json'), JSON.stringify(config));
  const [grant, reverse] = planSessionPairs(config).grants;
  const url = 'http://127.0.0.1:' + port;
  const sender = new BridgeClient(url);
  async function open() {
    service = await createCandidateService(config);
    await sender.connect(source.id, source.credential);
  }
  await open();
  const publish = id => sender.publish({ protocol: PROTOCOL, id, to: target.id, content_type: 'message', grant_id: grant.id,
    payload: { kind: 'event', topic: 'test.relay', correlation_id: id, text: 'fixture ' + id, source_refs: [] } });
  const beginRelay = options => { relay = startNativeRelay(directory, config, { intervalMs: 40, ...options }); };
  return { directory, config, source, target, sourcePath, archiveId, archive, url, grant, reverse, sender, publish, open,
    beginRelay, stopRelay, stopService, get service() { return service; } };
}

test('a message-only pair retains native read evidence and never repeats an uncertain launch', async t => {
  const f = await setup(t);
  const { source, target, directory, grant, sender, publish } = f;
  const never = { deliver: async () => assert.fail('Native dispatch must not be repeated') };

  await publish('uncertain');
  const preview = await exec(process.execPath, [resolve(import.meta.dirname, '../scripts/shared-service.mjs'), 'pairs', directory], { windowsHide: true });
  assert.deepEqual(JSON.parse(preview.stdout).grants, [{ id: grant.id, from: source.id, to: target.id, content_types: ['message'], native_delivery: true }]);
  const listed = await exec(process.execPath, [resolve(import.meta.dirname, '../scripts/relay-session.mjs'), directory, target.id, '--list'], { windowsHide: true });
  assert.deepEqual(JSON.parse(listed.stdout), [{ id: 'uncertain', from: source.id, content_type: 'message', archive_id: null, delivery: 'queued' }]);
  await assert.rejects(relaySession(directory, target.id, 'uncertain', { deliver: async (_target, { dispatch }) => {
    await dispatch(); throw new Error('lost native transport response');
  } }), /lost native transport response/);
  assert.equal((await sender.status('uncertain')).delivery, 'queued');
  await assert.rejects(relaySession(directory, target.id, 'uncertain', never), code('NATIVE_DELIVERY_UNCERTAIN'));

  await publish('read');
  await assert.rejects(relaySession(directory, target.id, 'read', { deliver: async (_target, { dispatch, prompt }) => {
    assert.match(prompt, /fixture read/);
    await dispatch(); await f.stopService();
    return evidence;
  } }));
  await f.open();
  const recovered = await relaySession(directory, target.id, 'read', never);
  assert.deepEqual(recovered, { state: 'confirmed', id: 'read', target: target.id, native: evidence });
  assert.equal((await relaySession(directory, target.id, 'read', never)).state, 'already_received');
});

test('automatic replay honors standby and history policy, recovers receipts and never repeats an uncertain turn', async t => {
  const f = await setup(t, { automatic: true });
  const { config, target, directory, sender, publish, source, reverse, archiveId } = f;
  const admin = new BridgeClient(f.url, { adminToken: config.adminToken });
  const update = body => admin.request('/api/endpoints/' + target.id + '/settings', { method: 'PATCH', admin: true, body });
  const delivered = async id => (await sender.status(id)).delivery === 'received';
  const status = [], dispatched = [];
  let attempts = 0;
  const deliver = async (_profile, { prompt, dispatch }) => {
    attempts++;
    if (attempts === 1) throw Object.assign(new Error('Failed before dispatch'), { code: 'NATIVE_TIMEOUT' });
    await dispatch();
    dispatched.push(prompt);
    if (prompt.includes('fixture recover')) await f.stopService();
    if (prompt.includes('fixture uncertain')) throw Object.assign(new Error('No receipt after dispatch'), { code: 'NATIVE_TIMEOUT' });
    return evidence;
  };
  const begin = () => f.beginRelay({ deliver, onStatus: value => status.push(value) });
  const reverseMessage = { id: 'reverse', to: source.id, grant_id: reverse.id,
    payload: { kind: 'event', topic: 'test.relay', correlation_id: 'reverse', text: 'native side reply', source_refs: [] } };

  await waitFor(() => f.service.core.binding(config.adminToken, archiveId).seeding === 0, 'Source failed to seed');
  await appendFile(f.sourcePath, JSON.stringify({ type: 'response_item', payload: { type: 'message', role: 'user',
    content: [{ type: 'input_text', text: 'synthetic history' }] } }) + '\n');
  await f.service.indexer.scan(archiveId);
  const history = f.service.core.db.prepare("SELECT id FROM messages WHERE content_type='history_sync'").get().id;
  await publish('automatic');
  begin();
  await delay(130);
  assert.equal(attempts, 0, 'next_turn cannot start a native turn');

  const existing = new BridgeClient(f.url);
  await existing.connect(target.id, target.credential);
  await update({ message: 'idle_wake', history_sync: 'idle_wake', receive_history: false });
  await assert.rejects(publishNativeMessage(directory, target.id, reverseMessage), code('ENDPOINT_ONLINE'));
  assert.equal((await existing.sessionInfo()).id, target.id);
  await delay(130);
  assert.equal(attempts, 0, 'The relay cannot replace a connected endpoint');
  await existing.request('/api/disconnect', { method: 'POST' });
  await waitFor(() => delivered('automatic'), 'Automatic event did not recover a pre-dispatch timeout');
  assert.equal(attempts, 2);
  assert.equal(dispatched.length, 1);
  assert.equal((await sender.status(history)).delivery, 'queued');

  await update({ paused: true, receive_history: true });
  await delay(130);
  assert.equal(dispatched.length, 1, 'Paused endpoints cannot start a native turn');
  await update({ paused: false });
  await waitFor(() => delivered(history), 'Enabled original history did not reach the native profile');
  assert.match(dispatched[1], /synthetic history/);
  assert.equal(f.service.core.db.prepare('SELECT read_through FROM history_cursors WHERE target_id=?').get(target.id).read_through, 0);
  await assert.rejects(publishNativeMessage(directory, target.id, { ...reverseMessage, to: 'unpaired' }), code('NOT_AUTHORIZED'));
  await publishNativeMessage(directory, target.id, reverseMessage);
  await publishNativeMessage(directory, target.id, reverseMessage);
  const received = await sender.receive({ ids: [reverseMessage.id] });
  assert.equal(received.items[0].payload.text, reverseMessage.payload.text);
  assert.equal(f.service.core.db.prepare('SELECT count(*) AS n FROM messages WHERE id=?').get(reverseMessage.id).n, 1);

  await publish('recover');
  await waitFor(() => status.some(row => row.id === 'recover' && row.state === 'deferred'), 'Timed-out confirmation was not recorded');
  await f.stopRelay();
  await f.open();
  assert.equal(f.service.core.endpoint(target.id).settings.history_sync, 'idle_wake', 'Saved policy survived restart');
  await update({ message: 'next_turn' });
  begin();
  await waitFor(() => delivered('recover'), 'Durable read receipt was not recovered on restart');
  assert.equal(dispatched.filter(prompt => prompt.includes('fixture recover')).length, 1);

  await publish('uncertain');
  await update({ message: 'idle_wake' });
  await waitFor(() => status.some(row => row.id === 'uncertain' && row.code === 'NATIVE_DELIVERY_UNCERTAIN'), 'Ambiguous dispatch was not held for verification');
  await f.stopRelay();
  await f.stopService();
  await f.open();
  begin();
  await publish('later');
  await delay(150);
  assert.equal(dispatched.filter(prompt => prompt.includes('fixture uncertain')).length, 1);
  assert.equal((await sender.status('later')).delivery, 'queued', 'The profile must wait while a previous native turn is unresolved');
});

test('a connected Codex relay confirms only the exact receipt in its bound rollout', async t => {
  const f = await setup(t, { connectedCodex: true });
  const { archive, directory, target, sender, publish } = f;
  const messageLine = (role, type, text) => JSON.stringify({
    type: 'response_item', payload: { type: 'message', role, content: [{ type, text }] },
  }) + '\n';
  await publish('live-good');
  const result = await relaySession(directory, target.id, 'live-good', { deliver: async (_profile, { dispatch, prompt, expected }) => {
    await dispatch();
    // App Server can persist an exact assistant output_text without a phase.
    await appendFile(archive, messageLine('user', 'input_text', prompt) + messageLine('assistant', 'output_text', expected));
    return evidence;
  } });
  assert.equal(result.state, 'confirmed');
  await publish('live-elsewhere');
  await assert.rejects(relaySession(directory, target.id, 'live-elsewhere', { deliver: async (_profile, { dispatch, expected }) => {
    await dispatch();
    await appendFile(archive, messageLine('user', 'input_text', 'Different prompt') + messageLine('assistant', 'output_text', expected));
    return evidence;
  } }), code('NATIVE_RECEIPT_MISMATCH'));
  assert.equal((await sender.status('live-elsewhere')).delivery, 'queued');
});
