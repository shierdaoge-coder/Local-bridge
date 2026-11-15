import test from 'node:test';
import assert from 'node:assert/strict';
import { nativeTestScope } from './native-test-scope.mjs';
import { PROTOCOL } from '../../src/protocol.mjs';
import { visibleMessage } from '../../src/history.mjs';

test('native batch admits only armed synthetic history and publications', () => {
  const event = { id: 'event', to: 'receiver', grant_id: 'direction', payload: { kind: 'event', text: 'fixture' } };
  const user = { role: 'user', text: 'exact prompt' };
  const final = { role: 'assistant', text: 'exact final' };
  let stage = 0;
  const saved = [];
  const db = { prepare: () => ({ all: () => saved.map(record => ({ record: JSON.stringify(record) })) }) };
  const scope = nativeTestScope([{ stage: 1, archive: 'raw', endpoint: 'sender', prompt: user.text,
    progress: 'exact progress', final: final.text, event }], () => stage);
  const publication = { protocol: PROTOCOL, content_type: 'message', ...event };
  const rejected = error => error.code === 'TEST_SCOPE_MISMATCH';
  assert.throws(() => scope.checkHistory('raw', [user], db), rejected);
  assert.throws(() => scope.checkPublication('sender', publication), rejected);
  stage = 1;
  assert.doesNotThrow(() => scope.checkHistory('raw', [user, final], db));
  assert.throws(() => scope.checkHistory('raw', [user, { ...final, text: 'other text' }], db), rejected);
  assert.throws(() => scope.checkHistory('other', [user], db), rejected);
  saved.push(user);
  assert.doesNotThrow(() => scope.checkHistory('raw', [{ role: 'assistant', text: 'exact progress' }, final], db));
  assert.throws(() => scope.checkHistory('raw', [final, final], db), rejected);
  assert.doesNotThrow(() => scope.checkPublication('sender', publication));
  assert.throws(() => scope.checkPublication('other', publication), rejected);
  assert.throws(() => scope.checkPublication('sender', { ...publication, id: 'other' }), rejected);
});

test('a Codex app-tool continuation does not stand in for the same prompt in the native composer', () => {
  const prompt = 'local-bridge-fixture synthetic transport test';
  const final = 'local-bridge-fixture-DONE';
  const row = payload => ({ type: 'response_item', payload });
  const toolOutput = row({ type: 'function_call_output', output: 'Continuation prompt:\n' + prompt });
  const composer = row({ type: 'message', role: 'user', content: [{ type: 'input_text', text: prompt }] });
  const answer = row({ type: 'message', role: 'assistant', phase: 'final_answer', content: [{ type: 'output_text', text: final }] });
  const scope = nativeTestScope([{ stage: 1, archive: 'native', prompt, progress: 'local-bridge-fixture-START', final }], () => 1);
  const db = { prepare: () => ({ all: () => [] }) };
  const visible = rows => rows.map(value => visibleMessage(value, 'codex-jsonl')).filter(Boolean);

  assert.deepEqual(visible([toolOutput, answer]), [{ role: 'assistant', text: final }]);
  assert.throws(() => scope.checkHistory('native', visible([toolOutput, answer]), db), { code: 'TEST_SCOPE_MISMATCH' });
  assert.deepEqual(visible([composer, answer]), [{ role: 'user', text: prompt }, { role: 'assistant', text: final }]);
  assert.doesNotThrow(() => scope.checkHistory('native', visible([composer, answer]), db));
});
