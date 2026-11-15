import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, appendFile, rm, stat } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { zstdCompressSync } from 'node:zlib';
import { CandidateCore } from './candidate-core.mjs';
import { BoundedSourceIndexer } from './source-pump.mjs';
import { inspectSourceHeader, READER_VERSION } from './bounded-reader.mjs';
import { TransportCore } from '../../src/core.mjs';
import { SourceIndexer } from '../../src/indexer.mjs';
import { ADMIN, message, ack, fixture } from '../../tests/helpers.mjs';

const row = text => JSON.stringify({ type: 'response_item', payload: { type: 'message', role: 'user', content: [{ type: 'input_text', text }] } }) + '\n';
const header = id => JSON.stringify({ type: 'session_meta', payload: { id } }) + '\n';
const cleanupCores = new Map();
async function directory(t) {
  const dir = await mkdtemp(join(tmpdir(), 'local-bridge-candidate-'));
  cleanupCores.set(dir, []);
  t.after(async () => {
    for (const core of cleanupCores.get(dir)) core.close(); cleanupCores.delete(dir);
    assert.equal(dirname(dir), resolve(tmpdir())); assert.ok(dir.startsWith(join(resolve(tmpdir()), 'local-bridge-candidate-')));
    await rm(dir, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 });
  });
  return dir;
}
async function setup(t, { bytes = header('native-a') + row('One'), mode = 'from_start', queueLimits = {}, maxVisibleRecords = 16 } = {}) {
  const dir = await directory(t), path = join(dir, 'source.jsonl'), database = join(dir, 'candidate.sqlite');
  await writeFile(path, bytes);
  const clock = { now: 10000 };
  const core = new CandidateCore({ database, adminToken: ADMIN, now: () => clock.now, allowedPaths: [path], queueLimits });
  cleanupCores.get(dir).push(core);
  t.after(() => core.close());
  const a = core.register(ADMIN, { id: 'codex-a', harness: 'codex', native_session_id: 'native-a' });
  const b = core.register(ADMIN, { id: 'dsh-b', harness: 'dsh', native_session_id: 'native-b' });
  const ca = core.connect(a.id, a.credential), cb = core.connect(b.id, b.credential);
  core.grant(ADMIN, { id: 'a-b', from: a.id, to: b.id, content_types: ['history_sync', 'message'], allow_wake: false, author_ref: 'candidate-test' });
  core.registerArchive(ADMIN, { id: 'archive-a', owner: a.id, generation: 'g1' });
  await core.bindSource(ADMIN, { archive_id: 'archive-a', path, format: 'codex-jsonl', author_ref: 'candidate-test', initial_mode: mode });
  core.subscribeHistory(ADMIN, { id: 'history-b', archive_id: 'archive-a', to: b.id, grant_id: 'a-b' });
  const indexer = new BoundedSourceIndexer({ core, adminToken: ADMIN, readerOptions: { maxVisibleRecords } });
  return { core, clock, a, b, ca, cb, path, dir, database, indexer };
}

test('candidate initialization rejects a live legacy database before invalidating its connections', async t => {
  const dir = await directory(t), database = join(dir, 'legacy.sqlite');
  const f = fixture({ database }); t.after(() => f.core.close());
  cleanupCores.get(dir).push(f.core);
  assert.throws(() => new CandidateCore({ database, adminToken: ADMIN }), e => e.code === 'CANDIDATE_DATABASE_REQUIRED');
  assert.ok(f.core.heartbeat(f.cb).lease_until > f.clock.now);
  assert.equal(f.core.db.prepare("SELECT count(*) AS n FROM sqlite_master WHERE name='candidate_bindings'").get().n, 0);
});

test('an existing empty or incomplete database is refused instead of silently repaired', async t => {
  const dir = await directory(t), database = join(dir, 'empty.sqlite'); await writeFile(database, '');
  assert.throws(() => new CandidateCore({ database, adminToken: ADMIN }), e => e.code === 'CANDIDATE_DATABASE_REQUIRED');
  assert.equal((await stat(database)).size, 0);
});

test('a marked candidate missing a cursor table is refused before the base core can recreate it empty', async t => {
  const f = await setup(t); f.core.db.exec('DROP TABLE history_cursors'); f.core.close();
  assert.throws(() => new CandidateCore({ database: f.database, adminToken: ADMIN, allowedPaths: [f.path] }),
    e => e.code === 'CANDIDATE_DATABASE_REQUIRED');
});

test('operator binding requires an explicit mode, allowlisted path, harness format and matching native header', async t => {
  const f = await setup(t); const extra = join(f.dir, 'other.jsonl'); await writeFile(extra, header('native-a'));
  f.core.registerArchive(ADMIN, { id: 'extra', owner: f.a.id, generation: 'g2' });
  const input = { archive_id: 'extra', path: f.path, format: 'codex-jsonl', author_ref: 'candidate-test' };
  await assert.rejects(f.core.bindSource(ADMIN, input), e => e.code === 'INVALID_INPUT');
  await assert.rejects(f.core.bindSource(ADMIN, { ...input, initial_mode: 'from_start', path: extra }), e => e.code === 'SOURCE_NOT_ALLOWED');
  await assert.rejects(f.core.bindSource(ADMIN, { ...input, initial_mode: 'from_start', format: 'dsh-zstd' }), e => e.code === 'SOURCE_IDENTITY_MISMATCH');
  await writeFile(f.path, header('different-native'));
  await assert.rejects(f.core.bindSource(ADMIN, { ...input, initial_mode: 'from_start' }), e => e.code === 'SOURCE_IDENTITY_MISMATCH');
  assert.equal(f.core.db.prepare("SELECT count(*) AS n FROM candidate_bindings WHERE archive_id='extra'").get().n, 0);
  assert.throws(() => f.core.attachSource(ADMIN, input), e => e.code === 'SOURCE_BINDING_REQUIRED');
});

test('header inspection validates DSH identity and rejects missing or torn native headers', async t => {
  const dir = await directory(t), path = join(dir, 'dsh.zstd');
  await writeFile(path, zstdCompressSync(Buffer.from(JSON.stringify({ type: 'session', version: 4, id: 'native-dsh' }) + '\n')));
  assert.equal((await inspectSourceHeader(path, 'dsh-zstd')).native_session_id, 'native-dsh');
  await writeFile(path, '{"type":"session_meta"');
  await assert.rejects(inspectSourceHeader(path, 'codex-jsonl'), e => e.code === 'SOURCE_NOT_READY');
  await writeFile(path, row('Visible content is not identity'));
  await assert.rejects(inspectSourceHeader(path, 'codex-jsonl'), e => e.code === 'SOURCE_IDENTITY_MISMATCH');
});

test('an in-place native header rewrite between binding and first scan is refused', async t => {
  const f = await setup(t);
  await writeFile(f.path, header('different-native') + row('Changed'));
  await assert.rejects(f.indexer.scan('archive-a'), e => e.code === 'SOURCE_REPLACED');
  assert.equal(f.core.sourceState(ADMIN, 'archive-a').checkpoint, null);
});

test('from_now skips the fixed old prefix, retains completion of a torn record and captures intervening growth', async t => {
  const f = await setup(t, { mode: 'from_now', bytes: header('native-a') + row('Old') + row('Partial').slice(0, -2) });
  await appendFile(f.path, '}\n' + row('New'));
  const result = await f.indexer.scan('archive-a', { maxPages: 10 });
  assert.equal(result.records, 2); assert.equal(f.core.binding(ADMIN, 'archive-a').seeding, 0);
  const items = f.core.receive(f.cb).items;
  assert.deepEqual(items.flatMap(i => i.payload.records.map(r => r.text)), ['Partial', 'New']);
  assert.deepEqual(items.flatMap(i => i.payload.records.map(r => r.seq)), [0, 1]);
  assert.equal(f.core.historyCursor(f.cb, 'archive-a', 'g1'), -1);
  f.core.acknowledge(f.cb, items.map(i => ack(i)));
  assert.equal(f.core.historyCursor(f.cb, 'archive-a', 'g1'), 1);
});

test('from_now seeding progress and the fixed boundary survive a database core reopen', async t => {
  const f = await setup(t, { mode: 'from_now', maxVisibleRecords: 1, bytes: header('native-a') + row('Old 1') + row('Old 2') });
  const seed = await f.indexer.scan('archive-a'); assert.equal(seed.records, 0); assert.equal(seed.has_more, true);
  assert.equal(f.core.binding(ADMIN, 'archive-a').seeding, 1);
  f.core.close(); await appendFile(f.path, row('New'));
  const core = new CandidateCore({ database: f.database, adminToken: ADMIN, allowedPaths: [f.path], now: () => f.clock.now });
  try {
    const indexer = new BoundedSourceIndexer({ core, adminToken: ADMIN, readerOptions: { maxVisibleRecords: 1 } });
    assert.equal((await indexer.scan('archive-a', { maxPages: 10 })).records, 1);
    assert.equal(core.sourceState(ADMIN, 'archive-a').checkpoint.reader_version, READER_VERSION);
    const conn = core.connect(f.b.id, f.b.credential);
    assert.deepEqual(core.receive(conn).items.flatMap(i => i.payload.records.map(r => r.text)), ['New']);
  } finally { core.close(); }
});

test('a legacy core and indexer cannot overwrite the reader version or advance candidate progress', async t => {
  const f = await setup(t); await f.indexer.scan('archive-a');
  f.core.acknowledge(f.cb, f.core.receive(f.cb).items.map(i => ack(i)));
  const before = f.core.sourceState(ADMIN, 'archive-a').checkpoint;
  f.core.close(); await appendFile(f.path, row('Later'));
  const old = new TransportCore({ database: f.database, adminToken: ADMIN, now: () => f.clock.now });
  try {
    const indexer = new SourceIndexer({ core: old, adminToken: ADMIN });
    await assert.rejects(indexer.scan('archive-a'), /READER_VERSION_MISMATCH/);
    assert.deepEqual(old.sourceState(ADMIN, 'archive-a').checkpoint, before);
    assert.equal(old.db.prepare('SELECT count(*) AS n FROM messages').get().n, 1);
    assert.equal(old.db.prepare('SELECT count(*) AS n FROM indexed_records').get().n, 1);
    old.registerArchive(ADMIN, { id: 'unbound', owner: f.a.id, generation: 'g1' });
    assert.throws(() => old.attachSource(ADMIN, { archive_id: 'unbound', path: f.path, format: 'codex-jsonl' }), /SOURCE_BINDING_REQUIRED/);
  } finally { old.close(); }
});

test('full history capacity pauses atomically, leaves message capacity and resumes after acknowledgement', async t => {
  const f = await setup(t, { bytes: header('native-a') + row('One') + row('Two'), maxVisibleRecords: 1,
    queueLimits: { pending_count: 3, history_count: 1 } });
  const first = await f.indexer.scan('archive-a'); const cp = first.checkpoint;
  await assert.rejects(f.indexer.scan('archive-a'), e => e.code === 'QUEUE_PRESSURE');
  assert.deepEqual(f.core.sourceState(ADMIN, 'archive-a').checkpoint, cp);
  assert.equal(f.core.sourceState(ADMIN, 'archive-a').status, 'paused_backpressure');
  assert.equal(f.core.db.prepare('SELECT count(*) AS n FROM indexed_records').get().n, 1);
  const event = f.core.publish(f.ca, message({ id: 'reserved-message' })); assert.equal(event.duplicate, false);
  const items = f.core.receive(f.cb).items;
  f.core.acknowledge(f.cb, items.filter(i => i.content_type === 'history_sync').map(i => ack(i)));
  assert.equal((await f.indexer.scan('archive-a')).records, 1);
  assert.equal(f.core.sourceState(ADMIN, 'archive-a').status, 'ready');
  assert.equal(f.core.db.prepare('SELECT count(*) AS n FROM indexed_records').get().n, 2);
});

test('byte capacity is enforced independently of count limits', async t => {
  const f = await setup(t, { bytes: header('native-a') + row('A'.repeat(1100)) + row('B'.repeat(1100)), maxVisibleRecords: 1,
    queueLimits: { pending_bytes: 4096, history_bytes: 2048 } });
  await f.indexer.scan('archive-a');
  await assert.rejects(f.indexer.scan('archive-a'), e => e.code === 'QUEUE_PRESSURE');
  assert.equal(f.core.db.prepare('SELECT count(*) AS n FROM messages').get().n, 1);
});

test('an atomic page too large for an empty queue requests a budget adjustment instead of endless retries', async t => {
  const f = await setup(t, { bytes: header('native-a') + row('A'.repeat(1100)) + row('B'.repeat(1100)),
    queueLimits: { pending_count: 3, history_count: 1 } });
  await assert.rejects(f.indexer.scan('archive-a'), e => e.code === 'PAGE_QUEUE_BUDGET');
  assert.equal(f.core.sourceState(ADMIN, 'archive-a').checkpoint, null);
  assert.equal(f.core.db.prepare('SELECT count(*) AS n FROM indexed_records').get().n, 0);
  f.indexer.readerOptions.maxVisibleRecords = 1;
  assert.equal((await f.indexer.scan('archive-a')).records, 1);
});

test('expired pending messages free capacity without requiring a recipient model turn', async t => {
  const f = await setup(t, { queueLimits: { pending_count: 2, history_count: 1 } });
  for (let i = 0; i < 2; i++) f.core.publish(f.ca, message({ id: `expired-${i}`, expires_at: new Date(f.clock.now + 50).toISOString() }));
  await assert.rejects(f.indexer.scan('archive-a'), e => e.code === 'QUEUE_PRESSURE');
  f.clock.now += 100;
  assert.equal((await f.indexer.scan('archive-a')).records, 1);
  assert.equal(f.core.db.prepare("SELECT count(*) AS n FROM messages WHERE delivery='delivery_failed'").get().n, 2);
});

test('reconnect defaults exclude already-read history, explicit IDs allow rereading and business recovery is preserved', async t => {
  const f = await setup(t); await f.indexer.scan('archive-a');
  const h = f.core.receive(f.cb).items[0]; f.core.acknowledge(f.cb, [ack(h)]);
  const event = f.core.publish(f.ca, message({ id: 'business-recoverable' }));
  const business = f.core.receive(f.cb).items[0]; f.core.acknowledge(f.cb, [ack(business)]);
  const reconnected = f.core.connect(f.b.id, f.b.credential);
  assert.deepEqual(f.core.receive(reconnected).items.map(i => i.id), [event.id]);
  assert.equal(f.core.historyCursor(reconnected, 'archive-a', 'g1'), 0);
  const explicit = f.core.receive(reconnected, { ids: [h.id] }).items;
  assert.equal(explicit[0].id, h.id); assert.equal(explicit[0].delivery, 'received');
  assert.equal(f.core.status(f.ca, event.id).processing, 'unread');
});

test('explicit queue budget increases survive reopening and permit the formerly blocked page', async t => {
  const f = await setup(t, { bytes: header('native-a') + row('A'.repeat(1100)) + row('B'.repeat(1100)),
    queueLimits: { pending_count: 3, history_count: 1 } });
  await assert.rejects(f.indexer.scan('archive-a'), e => e.code === 'PAGE_QUEUE_BUDGET');
  f.core.close();
  const reopened = new CandidateCore({ database: f.database, adminToken: ADMIN, allowedPaths: [f.path], now: () => f.clock.now,
    queueLimits: { pending_count: 4, history_count: 2 } });
  try {
    const indexer = new BoundedSourceIndexer({ core: reopened, adminToken: ADMIN });
    assert.equal((await indexer.scan('archive-a')).records, 2);
    assert.equal(reopened.db.prepare('SELECT count(*) AS n FROM messages').get().n, 2);
    assert.throws(() => reopened.setQueueLimits(ADMIN, { history_count: 1 }), e => e.code === 'QUEUE_LIMIT_BELOW_PENDING');
  } finally { reopened.close(); }
});
