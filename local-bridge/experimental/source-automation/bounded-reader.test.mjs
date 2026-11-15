import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, appendFile, rename, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname, resolve } from 'node:path';
import { zstdCompressSync } from 'node:zlib';
import { readBoundedPage } from './bounded-reader.mjs';
import { decodeHistory } from '../../src/history.mjs';
import { fixture, ADMIN } from '../../tests/helpers.mjs';

const options = { archive_id: 'archive-a', generation: 'g1', format: 'codex-jsonl' };
const row = text => JSON.stringify({ type: 'response_item', payload: { type: 'message', role: 'user', content: [{ type: 'input_text', text }] } }) + '\n';
const dshRow = text => JSON.stringify({ type: 'user/message', data: { role: 'user', source: { kind: 'user' }, content: [{ type: 'text', text }] } }) + '\n';
async function setup(t, bytes = '') {
  const directory = await mkdtemp(join(tmpdir(), 'local-bridge-bounded-')), path = join(directory, 'source.log');
  await writeFile(path, bytes);
  t.after(async () => {
    assert.equal(dirname(directory), resolve(tmpdir()));
    assert.ok(directory.startsWith(join(resolve(tmpdir()), 'local-bridge-bounded-')));
    await rm(directory, { recursive: true, force: true });
  });
  return { directory, path };
}

test('paged JSONL matches visible records and exact offsets from the established decoder', async t => {
  const bytes = Buffer.from(row('First 🐉') + JSON.stringify({ type: 'ignored' }) + '\n' + row('Second'));
  const { path } = await setup(t, bytes);
  const expected = decodeHistory(bytes, options), records = []; let checkpoint, page;
  do {
    page = await readBoundedPage(path, { ...options, checkpoint, chunkBytes: 3, maxVisibleRecords: 1 });
    records.push(...page.records); checkpoint = page.checkpoint;
  } while (page.has_more);
  assert.deepEqual(records, expected.records);
  for (const [key, value] of Object.entries(expected.checkpoint)) assert.deepEqual(checkpoint[key], value);
  assert.equal(page.stats.max_read_request, 3);
});

test('Codex page context is omitted by its per-block host provenance and composer text is retained', async t => {
  const selected = '<external_codex_apps_selected_page_part_1_of_2>fixture page</external_codex_apps_selected_page_part_1_of_2>';
  const open = '<external_codex_apps_open_page>{"page_id":null}</external_codex_apps_open_page>';
  const selectedKind = 'additional_content.codex_apps_selected_page_part_1_of_2';
  const host = { provenance: { type: 'harness', hook: false } };
  const author = { provenance: { type: 'user' } };
  const record = (texts, content_item_kinds, content_item_metadata) => JSON.stringify({ type: 'response_item', payload: {
    type: 'message', role: 'user', content: texts.map(text => ({ type: 'input_text', text })),
    internal_chat_message_metadata_passthrough: { content_item_kinds, content_item_metadata },
  } }) + '\n';
  const bytes = record([selected], [selectedKind], [host])
    + record([open, 'exact author prompt'], ['additional_content.codex_apps_open_page', 'user.text'], [host, author])
    + record([selected], [selectedKind], [author])
    + record([selected], [selectedKind], [])
    + row(selected);
  const { path } = await setup(t, bytes);
  const page = await readBoundedPage(path, options);
  assert.deepEqual(page.records.map(r => [r.seq, r.text, r.source_ref.start_line]), [
    [0, 'exact author prompt', 2], [1, selected, 3], [2, selected, 4], [3, selected, 5],
  ]);
  assert.equal(page.checkpoint.byte_offset, Buffer.byteLength(bytes));
  assert.equal(page.checkpoint.line_offset, 5);
});

test('large JSONL source reads a bounded first page instead of the complete file', async t => {
  const bytes = Buffer.from(row('Bounded').repeat(30000)), { path } = await setup(t, bytes);
  const page = await readBoundedPage(path, { ...options, maxVisibleRecords: 2, chunkBytes: 4096 });
  assert.equal(page.records.length, 2); assert.equal(page.has_more, true);
  assert.ok(page.stats.bytes_read <= 4096);
  assert.ok(page.stats.bytes_read < bytes.length / 100);
  assert.ok(page.stats.max_unit_bytes < 1024);
});

test('partial final line is retried only after completion and never checkpoints its bytes', async t => {
  const final = row('Incomplete 🐋'), { path } = await setup(t, row('Committed') + final.slice(0, -2));
  const first = await readBoundedPage(path, options);
  assert.equal(first.records.length, 1); assert.equal(first.incomplete_tail, true);
  assert.equal(first.tail_complete_known, false);
  assert.equal(first.checkpoint.byte_offset, Buffer.byteLength(row('Committed')));
  await appendFile(path, '}\n');
  const next = await readBoundedPage(path, { ...options, checkpoint: first.checkpoint });
  assert.equal(next.records[0].text, 'Incomplete 🐋'); assert.equal(next.incomplete_tail, false);
});

test('truncation and indexed-prefix edits reject stale checkpoints', async t => {
  const { path } = await setup(t, row('Original'));
  const first = await readBoundedPage(path, options);
  await writeFile(path, '');
  await assert.rejects(readBoundedPage(path, { ...options, checkpoint: first.checkpoint }), e => e.code === 'SOURCE_REPLACED');
  await writeFile(path, row('Replaced'));
  await assert.rejects(readBoundedPage(path, { ...options, checkpoint: first.checkpoint }), e => e.code === 'SOURCE_REPLACED');
});

test('same-path replacement is detected even when it preserves every previously indexed byte', async t => {
  const bytes = row('Same prefix'), { path } = await setup(t, bytes);
  const first = await readBoundedPage(path, options);
  await rename(path, path + '.old'); await writeFile(path, bytes + row('New suffix'));
  await assert.rejects(readBoundedPage(path, { ...options, checkpoint: first.checkpoint }), e => e.code === 'SOURCE_REPLACED');
});

test('growth during a scan fails closed and a later scan captures the new record', async t => {
  const { path } = await setup(t, row('Before'));
  await assert.rejects(readBoundedPage(path, { ...options, afterRead: () => appendFile(path, row('After')) }), e => e.code === 'SOURCE_CHANGED');
  const retry = await readBoundedPage(path, options);
  assert.deepEqual(retry.records.map(r => r.text), ['Before', 'After']);
});

test('oversized record needs an explicit larger budget and never silently drops content', async t => {
  const { path } = await setup(t, row('X'.repeat(2000)));
  await assert.rejects(readBoundedPage(path, { ...options, maxDecodedBytes: 256 }), e => e.code === 'ARCHIVE_BUDGET');
  const page = await readBoundedPage(path, { ...options, maxDecodedBytes: 4096 });
  assert.equal(page.records[0].text.length, 2000);
});

test('prefix verification itself has an explicit finite I/O budget', async t => {
  const { path } = await setup(t, row('Prior'));
  const first = await readBoundedPage(path, options);
  await assert.rejects(readBoundedPage(path, { ...options, checkpoint: first.checkpoint, maxPrefixBytes: 4 }), e => e.code === 'ARCHIVE_IO_BUDGET');
});

test('Zstandard frames match the decoder with incomplete-tail completion and paging', async t => {
  const a = zstdCompressSync(Buffer.from(dshRow('One'))), b = zstdCompressSync(Buffer.from(dshRow('Two')));
  const { path } = await setup(t, Buffer.concat([a, b.subarray(0, b.length - 2)]));
  const opts = { ...options, format: 'dsh-zstd', chunkBytes: 5 };
  const first = await readBoundedPage(path, opts);
  assert.equal(first.records.length, 1); assert.equal(first.incomplete_tail, true);
  assert.equal(first.tail_complete_known, false);
  assert.equal(first.checkpoint.byte_offset, a.length);
  await appendFile(path, b.subarray(b.length - 2));
  const second = await readBoundedPage(path, { ...opts, checkpoint: first.checkpoint });
  assert.equal(second.records[0].text, 'Two');
  const expected = decodeHistory(Buffer.concat([a, b]), opts);
  assert.deepEqual([...first.records, ...second.records], expected.records);
  assert.equal(second.checkpoint.prefix_sha256, expected.checkpoint.prefix_sha256);
});

test('a frame is atomic when its visible records exceed the configured page limit', async t => {
  const { path } = await setup(t, zstdCompressSync(Buffer.from(dshRow('One') + dshRow('Two'))));
  await assert.rejects(readBoundedPage(path, { ...options, format: 'dsh-zstd', maxVisibleRecords: 1 }), e => e.code === 'ARCHIVE_BUDGET');
  const page = await readBoundedPage(path, { ...options, format: 'dsh-zstd', maxVisibleRecords: 2 });
  assert.equal(page.records.length, 2);
});

test('oversized unit after a good page reports a budget block and resumes at the same checkpoint after adjustment', async t => {
  const { path } = await setup(t, row('Good') + row('X'.repeat(2000)));
  const first = await readBoundedPage(path, { ...options, maxDecodedBytes: 512 });
  assert.equal(first.records.length, 1); assert.equal(first.blocked_by_budget, 'encoded_unit');
  const next = await readBoundedPage(path, { ...options, checkpoint: first.checkpoint, maxDecodedBytes: 4096 });
  assert.equal(next.records.length, 1); assert.equal(next.records[0].seq, 1);
  assert.equal(next.records[0].text.length, 2000);
});

test('oversized decoded frame leaves an earlier complete frame resumable', async t => {
  const a = zstdCompressSync(Buffer.from(dshRow('Good'))), b = zstdCompressSync(Buffer.from(dshRow('X'.repeat(5000))));
  const { path } = await setup(t, Buffer.concat([a, b]));
  const first = await readBoundedPage(path, { ...options, format: 'dsh-zstd', maxDecodedBytes: 512 });
  assert.equal(first.records.length, 1); assert.equal(first.blocked_by_budget, 'decoded_frame');
  assert.equal(first.checkpoint.byte_offset, a.length);
  const next = await readBoundedPage(path, { ...options, format: 'dsh-zstd', checkpoint: first.checkpoint, maxDecodedBytes: 8192 });
  assert.equal(next.records[0].text.length, 5000);
});

test('bad complete JSON or compressed data is rejected without a checkpoint', async t => {
  const { path } = await setup(t, '{bad}\n');
  await assert.rejects(readBoundedPage(path, options), e => e.code === 'CORRUPT_ARCHIVE');
  await writeFile(path, Buffer.from('not zstd data'));
  await assert.rejects(readBoundedPage(path, { ...options, format: 'dsh-zstd' }), e => e.code === 'CORRUPT_ARCHIVE');
});

test('extended checkpoint commits atomically through the existing transport core', async t => {
  const { path } = await setup(t, row('Once'));
  const f = fixture(); t.after(() => f.core.close());
  f.core.attachSource(ADMIN, { archive_id: 'archive-a', path, format: 'codex-jsonl' });
  f.core.subscribeHistory(ADMIN, { id: 'sub-bounded', archive_id: 'archive-a', to: f.b.id, grant_id: 'a-b' });
  const page = await readBoundedPage(path, options);
  assert.equal(f.core.commitIndexedPage(ADMIN, 'archive-a', null, page).indexed_records, 1);
  assert.deepEqual(f.core.sourceState(ADMIN, 'archive-a').checkpoint.file_identity, page.checkpoint.file_identity);
  const again = await readBoundedPage(path, { ...options, checkpoint: page.checkpoint });
  assert.equal(f.core.commitIndexedPage(ADMIN, 'archive-a', page.checkpoint, again).queued.length, 0);
  assert.equal(f.core.receive(f.cb).items.length, 1);
});
