import test from 'node:test';
import assert from 'node:assert/strict';
import { zstdCompressSync, constants } from 'node:zlib';
import { decodeHistory, scanFrames, visibleMessage } from '../src/history.mjs';
import { code } from './helpers.mjs';

const jsonl = rows => Buffer.from(rows.map(row => JSON.stringify(row)).join('\n') + '\n');
const codex = (role, text, phase) => ({ type: 'response_item', payload: { type: 'message', role, phase, content: [{ type: role === 'user' ? 'input_text' : 'output_text', text }] } });
const dsh = (role, text, kind = role === 'user' ? 'user' : 'model') => ({ type: role === 'user' ? 'user/message' : 'assistant/message', data: role === 'user' ? { role, content: [{ type: 'text', text }], source: { kind } } : { message: { role, content: [{ type: 'text', text }], source: { kind } } } });
const frame = rows => zstdCompressSync(jsonl(rows), { params: { [constants.ZSTD_c_checksumFlag]: 1 } });
const source = format => ({ archive_id: 'fixture-archive', generation: 'one', format });

test('Codex extraction keeps visible commentary and final answers; hidden reasoning and tool noise stay out', () => {
  const raw = jsonl([codex('user', 'Question'), codex('assistant', 'Private', 'analysis'), { type: 'response_item', payload: { type: 'reasoning', text: 'hidden' } }, codex('assistant', 'Working', 'commentary'), codex('assistant', 'Answer', 'final_answer'), { type: 'response_item', payload: { type: 'function_call_output', output: 'noise' } }]);
  const parsed = decodeHistory(raw, source('codex-jsonl'));
  assert.deepEqual(parsed.records.map(r => r.text), ['Question', 'Working', 'Answer']);
  assert.deepEqual(parsed.records.map(r => r.seq), [0, 1, 2]); assert.deepEqual(parsed.records.map(r => r.source_ref.start_line), [1, 4, 5]);
});

test('runtime user-role wrappers are omitted and real question replies are normalized', () => {
  const reply = '<send_user_message_question_reply>' + JSON.stringify([{ question: 'Which?', answer: 'This one.' }]) + '</send_user_message_question_reply>';
  const raw = jsonl([codex('user', '<environment_context>metadata</environment_context>'), codex('user', '<external_codex_apps_open_page>{}</external_codex_apps_open_page>'), codex('user', reply), codex('user', 'Literal <environment_context> text')]);
  const parsed = decodeHistory(raw, source('codex-jsonl'));
  assert.deepEqual(parsed.records.map(r => r.text), ['问题：Which?\n回答：This one.', 'Literal <environment_context> text']);
});

test('attachments become lazy original-record references, never base64 bodies', () => {
  const row = codex('user', 'See picture'); row.payload.content.push({ type: 'input_image', image_url: 'data:image/png;base64,PRIVATE_IMAGE' });
  const parsed = decodeHistory(jsonl([row]), source('codex-jsonl'));
  assert.match(parsed.records[0].text, /附件：图像/); assert.ok(!parsed.records[0].text.includes('PRIVATE_IMAGE'));
  assert.equal(parsed.records[0].source_ref.start_line, 1);
});

test('plain JSONL resumes only after the committed newline and preserves physical line numbers', () => {
  const first = jsonl([codex('user', 'First')]), partial = Buffer.from(JSON.stringify(codex('assistant', 'Second', 'final_answer')).slice(0, 20));
  const initial = decodeHistory(Buffer.concat([first, partial]), source('codex-jsonl'));
  assert.equal(initial.incomplete_tail, true); assert.equal(initial.checkpoint.byte_offset, first.length);
  const completed = Buffer.concat([first, jsonl([codex('assistant', 'Second', 'final_answer')])]);
  const next = decodeHistory(completed, { ...source('codex-jsonl'), checkpoint: initial.checkpoint });
  assert.equal(next.records.length, 1); assert.equal(next.records[0].seq, 1); assert.equal(next.records[0].source_ref.start_line, 2);
  assert.equal(decodeHistory(completed, { ...source('codex-jsonl'), checkpoint: next.checkpoint }).records.length, 0);
});

test('DSH concatenated frames retain real author messages and omit bridge/runtime machine users', () => {
  const raw = Buffer.concat([frame([{ type: 'session', version: 4 }]), frame([dsh('user', 'Hello'), dsh('user', 'Wake!', 'local-bridge')]), frame([dsh('assistant', 'Reply')])]);
  assert.equal(scanFrames(raw).frames.length, 3);
  const parsed = decodeHistory(raw, source('dsh-zstd'));
  assert.deepEqual(parsed.records.map(r => r.text), ['Hello', 'Reply']); assert.equal(parsed.checkpoint.line_offset, 4);
});

test('incomplete DSH frames wait intact and are decoded exactly once after completion', () => {
  const first = frame([dsh('user', 'One')]), second = frame([dsh('assistant', 'Two')]);
  const start = decodeHistory(Buffer.concat([first, second.subarray(0, second.length - 2)]), source('dsh-zstd'));
  assert.equal(start.incomplete_tail, true); assert.equal(start.checkpoint.byte_offset, first.length);
  const end = decodeHistory(Buffer.concat([first, second]), { ...source('dsh-zstd'), checkpoint: start.checkpoint });
  assert.deepEqual(end.records.map(r => r.text), ['Two']); assert.equal(end.records[0].seq, 1);
});

test('replacement, truncation and generation mismatch refuse to guess a cursor', () => {
  const first = jsonl([codex('user', 'Original')]), checkpoint = decodeHistory(first, source('codex-jsonl')).checkpoint;
  assert.throws(() => decodeHistory(jsonl([codex('user', 'Changed')]), { ...source('codex-jsonl'), checkpoint }), code('SOURCE_REPLACED'));
  assert.throws(() => decodeHistory(first.subarray(0, 10), { ...source('codex-jsonl'), checkpoint }), code('SOURCE_REPLACED'));
  assert.throws(() => decodeHistory(first, { ...source('codex-jsonl'), generation: 'two', checkpoint }), code('SOURCE_REPLACED'));
});

test('committed corruption and complete-frame checksum failures never advance checkpoints', () => {
  assert.throws(() => decodeHistory(Buffer.from('{bad}\n'), source('codex-jsonl')), code('CORRUPT_ARCHIVE'));
  const damaged = frame([dsh('user', 'Payload')]); damaged[damaged.length - 1] ^= 1;
  assert.throws(() => decodeHistory(damaged, source('dsh-zstd')), code('CORRUPT_ARCHIVE'));
  assert.throws(() => decodeHistory(Buffer.from('not-zstd'), source('dsh-zstd')), code('CORRUPT_ARCHIVE'));
});

test('decode budgets fail explicitly and do not truncate large visible text', () => {
  const raw = jsonl([codex('user', 'Long'.repeat(2000))]);
  assert.throws(() => decodeHistory(raw, { ...source('codex-jsonl'), maxDecodedBytes: 100 }), code('ARCHIVE_BUDGET'));
  assert.equal(decodeHistory(raw, source('codex-jsonl')).records[0].text.length, 8000);
  assert.equal(visibleMessage({ type: 'tool/result' }, 'dsh-zstd'), null);
});

test('large plain archives page at complete line boundaries without losing or repeating messages', () => {
  const raw = jsonl(Array.from({ length: 12 }, (_, i) => codex('user', `Record ${i}`))), found = []; let checkpoint, pages = 0;
  for (;;) {
    const result = decodeHistory(raw, { ...source('codex-jsonl'), checkpoint, maxDecodedBytes: 350 });
    found.push(...result.records); checkpoint = result.checkpoint; pages++;
    assert.equal(result.incomplete_tail, false); if (!result.has_more) break;
  }
  assert.ok(pages > 1); assert.deepEqual(found.map(r => r.text), Array.from({ length: 12 }, (_, i) => `Record ${i}`));
  assert.equal(checkpoint.byte_offset, raw.length); assert.equal(checkpoint.visible_next, 12);
});

test('DSH paging advances only complete frames and is independent of the final torn tail', () => {
  const raw = Buffer.concat(Array.from({ length: 6 }, (_, i) => frame([dsh('user', `Record ${i}`)]))), found = []; let checkpoint, pages = 0;
  for (;;) {
    const result = decodeHistory(raw, { ...source('dsh-zstd'), checkpoint, maxDecodedBytes: 200 });
    found.push(...result.records); checkpoint = result.checkpoint; pages++;
    if (!result.has_more) break;
  }
  assert.ok(pages > 1); assert.equal(checkpoint.byte_offset, raw.length); assert.equal(found.length, 6);
  assert.deepEqual(found.map(r => r.seq), [0, 1, 2, 3, 4, 5]);
});
