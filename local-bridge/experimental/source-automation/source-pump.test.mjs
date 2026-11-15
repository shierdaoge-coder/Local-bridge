import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, appendFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { SourcePump, BoundedSourceIndexer } from './source-pump.mjs';
import { fixture, ADMIN } from '../../tests/helpers.mjs';
import { TransportCore } from '../../src/core.mjs';

const row = text => JSON.stringify({ type: 'response_item', payload: { type: 'message', role: 'user', content: [{ type: 'input_text', text }] } }) + '\n';
const sources = ['a', 'b'].map(archive_id => ({ archive_id, path: `/unused/${archive_id}` }));

test('bursts coalesce and each source gets one page before another pass', async () => {
  const calls = [];
  const pump = new SourcePump({ sources, delayMs: 60000,
    indexer: { scan: async id => { calls.push(id); return { has_more: id === 'a' && calls.length === 1 }; } } });
  try {
    for (let i = 0; i < 100; i++) pump.notify('a'); pump.notify('b');
    await pump.flush(); assert.deepEqual(calls, ['a', 'b']);
    await pump.flush(); assert.deepEqual(calls, ['a', 'b', 'a']);
    await pump.flush(); assert.equal(calls.length, 3);
  } finally { await pump.close(); }
});

test('notifications during a running scan queue exactly one follow-up pass', async () => {
  let release, calls = 0;
  const gate = new Promise(resolve => { release = resolve; });
  const pump = new SourcePump({ sources: sources.slice(0, 1), delayMs: 60000,
    indexer: { scan: async () => { calls++; if (calls === 1) await gate; return { has_more: false }; } } });
  try {
    pump.notify('a'); const pending = pump.flush();
    pump.notify('a'); pump.notify('a'); assert.equal(pump.flush(), pending);
    release(); await pending; await pump.flush(); assert.equal(calls, 2);
  } finally { release(); await pump.close(); }
});

test('transient mutation retries while a permanent budget block does not spin', async () => {
  let calls = 0, now = 1000; const errors = [];
  const pump = new SourcePump({ sources: sources.slice(0, 1), delayMs: 60000,
    now: () => now,
    onError: (_id, e) => errors.push(e.code), indexer: { scan: async () => {
      calls++; if (calls === 1) throw Object.assign(new Error(), { code: 'SOURCE_CHANGED' });
      return { has_more: true, blocked_by_budget: 'encoded_unit' };
    } } });
  try {
    pump.notify('a'); await pump.flush(); assert.equal(pump.dirty.size, 1);
    assert.deepEqual(await pump.flush(), [], 'Retry waits instead of spinning');
    now += 60000;
    await pump.flush(); assert.equal(pump.dirty.size, 0);
    await pump.flush(); assert.equal(calls, 2); assert.deepEqual(errors, ['SOURCE_CHANGED']);
  } finally { await pump.close(); }
});

test('repeated mutation failures back off while another ready source can still scan', async () => {
  let now = 1000; const calls = [];
  const pump = new SourcePump({ sources, delayMs: 100, retryMaxMs: 1000, now: () => now,
    indexer: { scan: async id => {
      calls.push(id); if (id === 'a') throw Object.assign(new Error(), { code: 'SOURCE_CHANGED' });
      return { has_more: false };
    } } });
  try {
    pump.notify('a'); await pump.flush(); assert.equal(pump.retryAfter.get('a'), 1200);
    pump.notify('b'); await pump.flush(); assert.deepEqual(calls, ['a', 'b']);
    now = 1200; await pump.flush(); assert.equal(pump.retryAfter.get('a'), 1600);
    now = 1600; await pump.flush(); assert.equal(pump.retryAfter.get('a'), 2400);
    now = 2400; await pump.flush(); assert.equal(pump.retryAfter.get('a'), 3400);
    assert.equal(pump.failures.get('a'), 4);
    assert.equal(pump.timerAt, 3400);
    pump.notify('b'); assert.equal(pump.timerAt, 2500, 'A ready source shortens a delayed retry timer');
  } finally { await pump.close(); }
});

test('observer exceptions cannot turn a committed page into duplicate retry work', async () => {
  let calls = 0;
  const pump = new SourcePump({ sources: sources.slice(0, 1), delayMs: 60000,
    onResult: () => { throw new Error('observer'); },
    indexer: { scan: async () => { calls++; return { has_more: false }; } } });
  try { pump.notify('a'); await pump.flush(); await pump.flush(); assert.equal(calls, 1); }
  finally { await pump.close(); }
});

async function realFixture(t, bytes) {
  const directory = await mkdtemp(join(tmpdir(), 'local-bridge-pump-')), path = join(directory, 'source.jsonl');
  await writeFile(path, bytes);
  const f = fixture({ database: join(directory, 'bridge.sqlite') });
  f.core.attachSource(ADMIN, { archive_id: 'archive-a', path, format: 'codex-jsonl', max_decoded_bytes: 4096 });
  f.core.subscribeHistory(ADMIN, { id: 'pump-sub', archive_id: 'archive-a', to: f.b.id, grant_id: 'a-b' });
  const indexer = new BoundedSourceIndexer({ core: f.core, adminToken: ADMIN, readerOptions: { maxVisibleRecords: 1 } });
  let pump;
  t.after(async () => {
    await pump?.close(); f.core.close();
    assert.equal(dirname(directory), resolve(tmpdir())); assert.ok(directory.startsWith(join(resolve(tmpdir()), 'local-bridge-pump-')));
    await rm(directory, { recursive: true, force: true });
  });
  return { ...f, path, directory, indexer, setPump: value => { pump = value; } };
}

test('quiet incomplete tails stay idle; polling notices a later completion', async t => {
  const f = await realFixture(t, row('Partial').slice(0, -2));
  const pump = new SourcePump({ indexer: f.indexer, sources: [{ archive_id: 'archive-a', path: f.path }], delayMs: 60000 }); f.setPump(pump);
  await pump.poll(); const first = await pump.flush();
  assert.equal(first[0].incomplete_tail, true); assert.equal(first[0].records, 0);
  await pump.poll(); assert.deepEqual(await pump.flush(), []);
  await appendFile(f.path, '}\n'); await pump.poll(); const next = await pump.flush();
  assert.equal(next[0].records, 1); assert.equal(next[0].checkpoint.visible_next, 1);
});

test('concurrent bounded scans share work and paginated fanout remains idempotent', async t => {
  const f = await realFixture(t, row('One') + row('Two'));
  const first = f.indexer.scan('archive-a'), duplicate = f.indexer.scan('archive-a'); assert.equal(first, duplicate);
  assert.equal((await first).records, 1);
  assert.equal((await f.indexer.scan('archive-a', { maxPages: 10 })).records, 1);
  assert.equal((await f.indexer.scan('archive-a')).queued.length, 0);
  assert.equal(f.core.db.prepare('SELECT count(*) AS n FROM messages').get().n, 2);
  assert.equal(f.core.historyCursor(f.cb, 'archive-a', 'g1'), -1);
});

test('actual directory watcher and metadata polling detect growth and close without another scan', async t => {
  const f = await realFixture(t, row('Initial'));
  const pump = new SourcePump({ indexer: f.indexer, sources: [{ archive_id: 'archive-a', path: f.path }], delayMs: 60000 }); f.setPump(pump);
  await pump.startWatching(); await pump.flush();
  // Polling remains authoritative even when the OS coalesces or drops a native event.
  await appendFile(f.path, row('Added')); await delay(10); await pump.poll();
  assert.equal((await pump.flush())[0].records, 1);
  await pump.close(); pump.notify('archive-a'); assert.deepEqual(await pump.flush(), []);
  assert.equal(pump.watchers.length, 0);
});

test('closing while watcher installation awaits an error observer does not install later resources', async () => {
  let release, entered;
  const startedError = new Promise(resolve => { entered = resolve; });
  const gate = new Promise(resolve => { release = resolve; });
  const pump = new SourcePump({ sources: [{ archive_id: 'missing', path: join(tmpdir(), 'missing-local-bridge-parent', 'file') }, ...sources],
    indexer: { scan: async () => { throw new Error('must not scan'); } },
    onError: async () => { entered(); await gate; } });
  const starting = pump.startWatching(); await startedError;
  await pump.close(); release(); await starting;
  assert.equal(pump.watchers.length, 0); assert.equal(pump.interval, undefined);
  assert.equal(pump.dirty.size, 0);
});

test('bounded indexing rolls back failed fanout and a restart retains only committed source progress', async t => {
  const f = await realFixture(t, row('One') + row('Two'));
  f.core.db.exec("CREATE TRIGGER fixture_fail BEFORE INSERT ON messages BEGIN SELECT RAISE(ABORT,'isolated fanout failure'); END;");
  await assert.rejects(f.indexer.scan('archive-a'), /isolated fanout failure/);
  assert.equal(f.core.sourceState(ADMIN, 'archive-a').checkpoint, null);
  assert.equal(f.core.db.prepare('SELECT count(*) AS n FROM indexed_records').get().n, 0);
  f.core.db.exec('DROP TRIGGER fixture_fail');
  const first = await f.indexer.scan('archive-a'); assert.equal(first.records, 1);
  f.core.close();
  const reopened = new TransportCore({ database: join(f.directory, 'bridge.sqlite'), adminToken: ADMIN, now: () => f.clock.now });
  try {
    const indexer = new BoundedSourceIndexer({ core: reopened, adminToken: ADMIN, readerOptions: { maxVisibleRecords: 1 } });
    assert.deepEqual(reopened.sourceState(ADMIN, 'archive-a').checkpoint, first.checkpoint);
    assert.equal((await indexer.scan('archive-a')).records, 1);
    assert.equal((await indexer.scan('archive-a')).queued.length, 0);
    assert.equal(reopened.db.prepare('SELECT count(*) AS n FROM messages').get().n, 2);
  } finally { reopened.close(); }
});

test('native file notifications drive automatic isolated indexing without a manual flush or poll', async t => {
  const f = await realFixture(t, row('Initial'));
  const pump = new SourcePump({ indexer: f.indexer, sources: [{ archive_id: 'archive-a', path: f.path }], delayMs: 5, pollMs: 60000 }); f.setPump(pump);
  await pump.startWatching(); await pump.flush();
  await appendFile(f.path, row('Via watcher'));
  const deadline = Date.now() + 2000;
  while (Date.now() < deadline && f.core.sourceState(ADMIN, 'archive-a').checkpoint.visible_next < 2) await delay(10);
  assert.equal(f.core.sourceState(ADMIN, 'archive-a').checkpoint.visible_next, 2);
});
