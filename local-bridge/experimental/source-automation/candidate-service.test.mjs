import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, appendFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { randomBytes } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { createCandidateService } from './candidate-service.mjs';
import { BridgeClient } from '../../src/client.mjs';
import { ADMIN } from '../../tests/helpers.mjs';

const row = text => JSON.stringify({ type: 'response_item', payload: { type: 'message', role: 'user', content: [{ type: 'input_text', text }] } }) + '\n';
async function setup(t) {
  const directory = await mkdtemp(join(tmpdir(), 'local-bridge-candidate-service-'));
  const path = join(directory, 'source.jsonl');
  await writeFile(path, JSON.stringify({ type: 'session_meta', payload: { id: 'candidate-native-a' } }) + '\n' + row('Old'));
  const a = randomBytes(32).toString('base64url'), b = randomBytes(32).toString('base64url');
  const config = { directory, adminToken: ADMIN,
    endpoints: [{ id: 'a', harness: 'codex', native_session_id: 'candidate-native-a', credential: a },
      { id: 'b', harness: 'dsh', native_session_id: 'candidate-native-b', credential: b }],
    archives: [{ id: 'archive-candidate', owner: 'a', generation: 'g1' }],
    bindings: [{ archive_id: 'archive-candidate', path, format: 'codex-jsonl', initial_mode: 'from_now', author_ref: 'isolated-test' }],
    grants: [{ id: 'a-b', from: 'a', to: 'b', content_types: ['history_sync', 'message'], allow_wake: false, author_ref: 'isolated-test' }],
    subscriptions: [{ id: 'sub', archive_id: 'archive-candidate', to: 'b', grant_id: 'a-b' }],
    queueLimits: { pending_count: 3, history_count: 1 },
    readerOptions: { maxVisibleRecords: 1 }, pumpOptions: { delayMs: 5, pollMs: 30, retryMaxMs: 5000 } };
  const services = [];
  t.after(async () => {
    for (const service of services) await service.close();
    assert.equal(dirname(directory), resolve(tmpdir())); assert.ok(directory.startsWith(join(resolve(tmpdir()), 'local-bridge-candidate-service-')));
    await rm(directory, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 });
  });
  return { config, directory, path, a, b, services };
}
async function until(predicate, message) {
  const deadline = Date.now() + 3000;
  while (Date.now() < deadline) { if (predicate()) return; await delay(10); }
  assert.fail(message);
}

test('separate candidate HTTP service indexes only the new increment and resumes after queue acknowledgement', async t => {
  const f = await setup(t), service = await createCandidateService(f.config); f.services.push(service);
  const admin = new BridgeClient(service.url, { adminToken: ADMIN });
  await until(() => service.core.binding(ADMIN, 'archive-candidate').seeding === 0 && service.core.sourceState(ADMIN, 'archive-candidate').checkpoint !== null, 'Initial boundary seeding');
  const health = await fetch(service.url + '/api/health').then(r => r.json());
  assert.equal(health.features.codex_hooks, false); assert.equal(health.features.dsh_host, false);
  assert.equal(service.core.db.prepare('SELECT count(*) AS n FROM messages').get().n, 0);
  const recipient = new BridgeClient(service.url);
  await recipient.connect('b', f.b);
  await appendFile(f.path, row('Increment one') + row('Increment two'));
  await until(() => service.core.sourceState(ADMIN, 'archive-candidate').status === 'paused_backpressure', 'Queue pressure pauses automatically');
  const first = await recipient.receive();
  assert.deepEqual(first.items.flatMap(i => i.payload.records.map(r => r.text)), ['Increment one']);
  // Give the retry a long due time; receiving/acknowledging should resume it immediately.
  service.pump.retryAfter.set('archive-candidate', Date.now() + 30000);
  await recipient.acknowledge(first.items.map(i => ({ id: i.id, claim_token: i.claim.token })));
  await until(() => service.core.sourceState(ADMIN, 'archive-candidate').checkpoint.visible_next === 2, 'Capacity event resumes without a new file change');
  const second = await recipient.receive();
  assert.deepEqual(second.items.flatMap(i => i.payload.records.map(r => r.text)), ['Increment two']);
  await recipient.acknowledge(second.items.map(i => ({ id: i.id, claim_token: i.claim.token })));
  assert.equal((await recipient.request('/api/cursor?archive_id=archive-candidate&generation=g1')).read_through, 1);
  assert.equal((await admin.request('/api/sources/archive-candidate', { admin: true })).checkpoint.visible_next, 2);
  assert.equal(service.core.db.prepare('SELECT count(*) AS n FROM offers WHERE wake=1').get().n, 0);
  assert.notEqual(new URL(service.url).port, '4776');
});

test('a history batch receipt frees queue capacity and triggers a blocked source scan', async t => {
  const f = await setup(t), service = await createCandidateService(f.config); f.services.push(service);
  const archive_id = 'archive-candidate';
  await until(() => service.core.binding(ADMIN, archive_id).seeding === 0, 'Initial boundary seeding');
  const recipient = new BridgeClient(service.url);
  await recipient.connect('b', f.b);
  await appendFile(f.path, row('First in snapshot') + row('Later from source scan'));
  await until(() => service.core.sourceState(ADMIN, archive_id).status === 'paused_backpressure', 'Queue pressure pauses automatically');

  const first = await recipient.receive({ archive_id });
  assert.equal(first.source_status, 'paused_backpressure');
  assert.equal(first.indexed_through, 0);
  assert.equal(first.remaining_packets, 0);
  assert.deepEqual(first.runs, [{ role: 'user', messages: [[0, 'First in snapshot']] }]);
  service.pump.retryAfter.set(archive_id, Date.now() + 30000);
  assert.equal((await recipient.acknowledge({ receipt: first.receipt })).read_through, 0);
  await until(() => service.core.sourceState(ADMIN, archive_id).checkpoint.visible_next === 2, 'Batch receipt resumes the source');
  const second = await recipient.receive({ archive_id });
  assert.deepEqual(second.runs, [{ role: 'user', messages: [[1, 'Later from source scan']] }]);
  assert.equal((await recipient.acknowledge({ receipt: second.receipt })).read_through, 1);
});

test('candidate service reopens its own DB and keeps the initial boundary and accepted cursor', async t => {
  const f = await setup(t); const first = await createCandidateService(f.config); f.services.push(first);
  await until(() => first.core.binding(ADMIN, 'archive-candidate').seeding === 0, 'Initial seeding');
  await appendFile(f.path, row('One'));
  await until(() => first.core.sourceState(ADMIN, 'archive-candidate').checkpoint.visible_next === 1, 'First increment');
  const client = new BridgeClient(first.url); await client.connect('b', f.b);
  const items = (await client.receive()).items;
  await client.acknowledge(items.map(i => ({ id: i.id, claim_token: i.claim.token })));
  first.core.setSettings(ADMIN, 'b', { receive_history: false });
  await first.close();
  const second = await createCandidateService(f.config); f.services.push(second);
  const restored = new BridgeClient(second.url); await restored.connect('b', f.b);
  assert.equal((await restored.request('/api/cursor?archive_id=archive-candidate&generation=g1')).read_through, 0);
  assert.equal((await restored.sessionInfo()).settings.receive_history, false);
  assert.equal((await second.indexer.scan('archive-candidate')).records, 0);
  await appendFile(f.path, row('Two'));
  await until(() => second.core.sourceState(ADMIN, 'archive-candidate').checkpoint.visible_next === 2, 'Increment after reopen');
  const pending = second.core.db.prepare("SELECT id FROM messages WHERE to_id='b' AND content_type='history_sync' AND delivery='queued'").get();
  assert.deepEqual((await restored.receive({ ids: [pending.id] })).items, []);
  second.core.setSettings(ADMIN, 'b', { receive_history: true });
  assert.deepEqual((await restored.receive()).items.flatMap(i => i.payload.records.map(r => r.text)), ['Two']);
});

test('history blocked by a full queue resumes after a restart and a fresh acknowledgement', async t => {
  const f = await setup(t);
  const first = await createCandidateService(f.config); f.services.push(first);
  const archive = 'archive-candidate';
  await until(() => first.core.binding(ADMIN, archive).seeding === 0, 'Initial boundary seeding');
  await appendFile(f.path, row('Pending before restart') + row('Blocked before restart'));
  await until(() => first.core.sourceState(ADMIN, archive).status === 'paused_backpressure', 'Queue capacity reached');
  assert.equal(first.core.sourceState(ADMIN, archive).checkpoint.visible_next, 1);
  const recipient = new BridgeClient(first.url);
  const oldConnection = await recipient.connect('b', f.b);
  const [oldItem] = (await recipient.receive()).items;
  await first.close();

  const reopened = await createCandidateService(f.config); f.services.push(reopened);
  const stale = new BridgeClient(reopened.url, { connection: oldConnection });
  await assert.rejects(stale.sessionInfo(), { code: 'STALE_CONNECTION' });
  const fresh = new BridgeClient(reopened.url);
  await fresh.connect('b', f.b);
  const [pending] = (await fresh.receive()).items;
  assert.equal(pending.id, oldItem.id);
  assert.deepEqual(pending.payload.records.map(record => record.text), ['Pending before restart']);
  const staleAck = await fresh.acknowledge([{ id: pending.id, claim_token: oldItem.claim.token }]);
  assert.equal(staleAck[0].ok, false);
  const ack = await fresh.acknowledge([{ id: pending.id, claim_token: pending.claim.token }]);
  assert.equal(ack[0].ok, true);
  await until(() => reopened.core.sourceState(ADMIN, archive).checkpoint.visible_next === 2, 'Capacity resumes the blocked increment');
  const [next] = (await fresh.receive()).items;
  assert.deepEqual(next.payload.records.map(record => record.text), ['Blocked before restart']);
  await fresh.acknowledge([{ id: next.id, claim_token: next.claim.token }]);
  assert.equal((await fresh.request('/api/cursor?archive_id=archive-candidate&generation=g1')).read_through, 1);
  assert.deepEqual((await fresh.receive()).items, []);
  assert.equal(reopened.core.db.prepare('SELECT count(*) AS n FROM messages').get().n, 2);
});

test('candidate factory refuses the active port and a wake-enabled configuration', async t => {
  const f = await setup(t);
  await assert.rejects(createCandidateService({ ...f.config, port: 4776 }), e => e.code === 'INVALID_INPUT');
  const config = { ...f.config, endpoints: f.config.endpoints.map(e => ({ ...e, capabilities: { can_wake: true } })) };
  await assert.rejects(createCandidateService(config), e => e.code === 'WAKE_NOT_AUTHORIZED');
});

test('a stale additional subscription is refused on reopen rather than silently fanning out to an omitted target', async t => {
  const f = await setup(t), service = await createCandidateService(f.config); f.services.push(service);
  await until(() => service.core.binding(ADMIN, 'archive-candidate').seeding === 0, 'Initial seeding');
  const c = service.core.register(ADMIN, { id: 'omitted', harness: 'dsh', native_session_id: 'omitted-native',
    settings: { history_sync: 'next_turn', message: 'next_turn' }, capabilities: { can_confirm_read: true } });
  const extraGrant = { id: 'a-omitted', from: 'a', to: c.id, content_types: ['history_sync'], allow_wake: false, author_ref: 'old-config' };
  service.core.grant(ADMIN, extraGrant);
  service.core.subscribeHistory(ADMIN, { id: 'omitted-sub', archive_id: 'archive-candidate', to: c.id, grant_id: 'a-omitted' });
  await service.close();
  f.config.endpoints.push({ id: c.id, harness: 'dsh', native_session_id: 'omitted-native', credential: c.credential });
  f.config.grants.push(extraGrant);
  await assert.rejects(createCandidateService(f.config), e => e.code === 'NOT_AUTHORIZED');
});

test('a failed startup binding rolls back registrations and permits a corrected configuration', async t => {
  const f = await setup(t);
  const bad = { ...f.config, bindings: f.config.bindings.map(b => ({ ...b, format: 'dsh-zstd' })) };
  await assert.rejects(createCandidateService(bad), e => e.code === 'SOURCE_IDENTITY_MISMATCH');
  const service = await createCandidateService(f.config); f.services.push(service);
  assert.equal(service.core.db.prepare('SELECT count(*) AS n FROM endpoints').get().n, 2);
  assert.equal(service.core.db.prepare('SELECT count(*) AS n FROM candidate_bindings').get().n, 1);
});
