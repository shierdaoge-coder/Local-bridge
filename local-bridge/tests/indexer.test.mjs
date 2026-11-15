import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, appendFileSync, rmSync } from 'node:fs';
import { dirname, basename, resolve, join } from 'node:path';
import { tmpdir } from 'node:os';
import { SourceIndexer } from '../src/indexer.mjs';
import { TransportCore } from '../src/core.mjs';
import { decodeHistory } from '../src/history.mjs';
import { ADMIN, fixture, code } from './helpers.mjs';

const record = value => JSON.stringify({ type: 'response_item', payload: { type: 'message', role: 'user', content: [{ type: 'input_text', text: value }] } }) + '\n';
function setup(t, { persistent = false, budget = 64 * 1024 * 1024 } = {}) {
  const directory = mkdtempSync(join(tmpdir(), 'local-bridge-index-')), path = join(directory, 'source.jsonl'); writeFileSync(path, '');
  const f = fixture(persistent ? { database: join(directory, 'bridge.sqlite') } : {});
  f.core.attachSource(ADMIN, { archive_id: 'archive-a', path, format: 'codex-jsonl', max_decoded_bytes: budget });
  const indexer = new SourceIndexer({ core: f.core, adminToken: ADMIN }); let restarted;
  t.after(() => { restarted?.close(); f.core.close(); assert.equal(dirname(directory), resolve(tmpdir())); assert.ok(basename(directory).startsWith('local-bridge-index-')); rmSync(directory, { recursive: true, force: true }); });
  return { ...f, directory, path, indexer, restart: () => { f.core.close(); restarted = new TransportCore({ database: join(directory, 'bridge.sqlite'), adminToken: ADMIN, now: () => f.clock.now }); return restarted; } };
}
const subscribe = f => f.core.subscribeHistory(ADMIN, { id: 'history-b', archive_id: 'archive-a', to: 'dsh-b', grant_id: 'a-b' });

test('index, fanout and source position commit together and repeat scans do not duplicate history', async t => {
  const f = setup(t); subscribe(f); writeFileSync(f.path, record('One') + record('Two'));
  const first = await f.indexer.scan('archive-a'); assert.equal(first.records, 2); assert.ok(first.queued.length > 0);
  assert.equal(f.core.sourceState(ADMIN, 'archive-a').checkpoint.visible_next, 2);
  assert.equal(f.core.historyCursor(f.cb, 'archive-a', 'g1'), -1, 'Indexing alone is never reading');
  assert.equal((await f.indexer.scan('archive-a')).queued.length, 0); assert.deepEqual(f.core.backfillHistory(ADMIN, 'archive-a'), []);
  appendFileSync(f.path, record('Three')); const next = await f.indexer.scan('archive-a'); assert.equal(next.records, 1);
  assert.equal(f.core.sourceState(ADMIN, 'archive-a').checkpoint.visible_next, 3);
  const items = f.core.receive(f.cb).items; assert.deepEqual(items.flatMap(m => m.payload.records.map(r => r.text)), ['One', 'Two', 'Three']);
});

test('storage failure after a first enqueue rolls back messages, index, cursor and notifications', async t => {
  const f = setup(t); subscribe(f); writeFileSync(f.path, record('A'.repeat(1200)) + record('B'.repeat(1200)));
  let signals = 0; f.core.on('activity', () => { signals++; });
  f.core.db.exec("CREATE TRIGGER fixture_fail BEFORE INSERT ON messages WHEN NEW.queue_seq=2 BEGIN SELECT RAISE(ABORT,'injected write failure'); END;");
  await assert.rejects(f.indexer.scan('archive-a'), /injected write failure/);
  assert.equal(f.core.db.prepare('SELECT COUNT(*) AS n FROM messages').get().n, 0);
  assert.equal(f.core.db.prepare('SELECT COUNT(*) AS n FROM indexed_records').get().n, 0);
  assert.equal(f.core.sourceState(ADMIN, 'archive-a').checkpoint, null); assert.equal(signals, 0);
  f.core.db.exec('DROP TRIGGER fixture_fail'); const retried = await f.indexer.scan('archive-a');
  assert.equal(retried.records, 2); assert.equal(retried.queued.length, 2); assert.equal(signals, 1, 'Notify once per target after durable commit');
});

test('one source can fan out to multiple endpoints without global message ID collisions', async t => {
  const f = setup(t); subscribe(f); writeFileSync(f.path, record('Shared original')); await f.indexer.scan('archive-a');
  const c = f.core.register(ADMIN, { id: 'dsh-c', harness: 'dsh', native_session_id: 'native-c' });
  f.core.grant(ADMIN, { id: 'a-c', from: 'codex-a', to: 'dsh-c', content_types: ['history_sync'], allow_wake: false, author_ref: 'fixture-author' });
  const second = f.core.subscribeHistory(ADMIN, { id: 'history-c', archive_id: 'archive-a', to: 'dsh-c', grant_id: 'a-c' });
  assert.equal(second.queued.length, 1);
  const b = f.core.receive(f.cb).items[0], third = f.core.receive(f.core.connect(c.id, c.credential)).items[0];
  assert.notEqual(b.id, third.id); assert.deepEqual(b.payload, third.payload);
});

test('paging and partial source tails persist a safe resumable checkpoint', async t => {
  const f = setup(t, { budget: 300 }); subscribe(f); writeFileSync(f.path, Array.from({ length: 8 }, (_, i) => record(`Line ${i}`)).join(''));
  const first = await f.indexer.scan('archive-a'); assert.equal(first.has_more, true); const base = first.checkpoint.visible_next;
  const rest = await f.indexer.scan('archive-a', { maxPages: 50 }); assert.equal(base + rest.records, 8);
  appendFileSync(f.path, record('Last').slice(0, -2)); const pending = await f.indexer.scan('archive-a');
  assert.equal(pending.incomplete_tail, true); assert.equal(pending.records, 0);
  appendFileSync(f.path, '}\n'); const completed = await f.indexer.scan('archive-a'); assert.equal(completed.records, 1);
});

test('source replacement requires reindexing and leaves old queued work and checkpoint intact', async t => {
  const f = setup(t); subscribe(f); writeFileSync(f.path, record('Original')); await f.indexer.scan('archive-a');
  const before = f.core.sourceState(ADMIN, 'archive-a').checkpoint; writeFileSync(f.path, record('Replaced'));
  await assert.rejects(f.indexer.scan('archive-a'), code('SOURCE_REPLACED'));
  const state = f.core.sourceState(ADMIN, 'archive-a'); assert.deepEqual(state.checkpoint, before); assert.equal(state.status, 'needs_reindex');
  assert.equal(f.core.receive(f.cb).items[0].payload.records[0].text, 'Original');
});

test('a revoked subscription does not prevent indexing or fanout to another authorized target', async t => {
  const f = setup(t); subscribe(f);
  f.core.grant(ADMIN, { id: 'a-other', from: 'codex-a', to: 'other', content_types: ['history_sync'], allow_wake: false, author_ref: 'fixture-author' });
  f.core.subscribeHistory(ADMIN, { id: 'history-other', archive_id: 'archive-a', to: 'other', grant_id: 'a-other' });
  f.core.revoke(ADMIN, 'a-b'); writeFileSync(f.path, record('Allowed elsewhere'));
  const result = await f.indexer.scan('archive-a'); assert.equal(result.records, 1); assert.equal(result.queued.length, 1);
  assert.equal(f.core.db.prepare('SELECT last_error FROM history_subscriptions WHERE id=?').get('history-b').last_error, 'NOT_AUTHORIZED');
  assert.equal(f.core.receive(f.cb).items.length, 0);
});

test('concurrent scans share work, while stale external pages cannot rewind a checkpoint', async t => {
  const f = setup(t); subscribe(f); const bytes = Buffer.from(record('Once')); writeFileSync(f.path, bytes);
  const a = f.indexer.scan('archive-a'), b = f.indexer.scan('archive-a'); assert.equal(a, b); await a;
  const page = decodeHistory(bytes, { archive_id: 'archive-a', generation: 'g1', format: 'codex-jsonl' });
  assert.throws(() => f.core.commitIndexedPage(ADMIN, 'archive-a', null, page), code('CHECKPOINT_CONFLICT'));
  assert.equal(f.core.sourceState(ADMIN, 'archive-a').checkpoint.visible_next, 1);
});

test('service restart preserves indexed source positions and source-level fanout receipts', async t => {
  const f = setup(t, { persistent: true }); subscribe(f); writeFileSync(f.path, record('Before restart')); await f.indexer.scan('archive-a');
  const core = f.restart(), indexer = new SourceIndexer({ core, adminToken: ADMIN });
  assert.equal((await indexer.scan('archive-a')).queued.length, 0);
  appendFileSync(f.path, record('After restart')); assert.equal((await indexer.scan('archive-a')).records, 1);
  assert.equal(core.db.prepare('SELECT COUNT(*) AS n FROM history_fanout').get().n, 2);
});

test('source registry requires operator authority and never accepts a message-provided path', t => {
  const f = setup(t);
  assert.throws(() => f.core.attachSource('wrong', { archive_id: 'archive-a', path: f.path, format: 'codex-jsonl' }), code('UNAUTHORIZED'));
  assert.throws(() => f.core.sourceState('wrong', 'archive-a'), code('UNAUTHORIZED'));
});
