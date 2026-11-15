import test from 'node:test';
import assert from 'node:assert/strict';
import { PassThrough, Readable } from 'node:stream';
import { spawn } from 'node:child_process';
import { resolve } from 'node:path';
import { readHookInput } from '../src/codex-hook-input.mjs';

test('hook input completes without waiting for the host to close stdin', async () => {
  const pipe = new PassThrough(), work = readHookInput(pipe);
  pipe.write('{"hook_event_name":"PreToolUse","session_id":"test-only"}\n');
  const result = await work; assert.equal(result.session_id, 'test-only'); assert.equal(pipe.writableEnded, false);
});
test('hook JSON handles split multibyte UTF8 and rejects malformed/oversized input', async () => {
  const bytes = Buffer.from('\uFEFF' + JSON.stringify({ text: '作者' }));
  const chunks = [...bytes].map(byte => Buffer.from([byte]));
  assert.deepEqual(await readHookInput(Readable.from(chunks)), { text: '作者' });
  await assert.rejects(readHookInput(Readable.from(['{bad json'])), SyntaxError);
  await assert.rejects(readHookInput(Readable.from(['{"oversized":"input"}']), { maxBytes: 4 }), error => error.code === 'PAYLOAD_TOO_LARGE');
});
test('real hook process returns denial while the native input pipe remains open', async t => {
  const child = spawn(process.execPath, [resolve('src/codex-hook.mjs')], { windowsHide: true, env: { ...process.env, LOCAL_BRIDGE_CONFIG: resolve('.runtime/does-not-exist-hook-input-test.json') }, stdio: ['pipe', 'pipe', 'pipe'] });
  t.after(() => { if (child.exitCode === null) child.kill(); });
  const output = new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(Error('Hook waited for pipe EOF')), 2000);
    child.stdout.once('data', bytes => { clearTimeout(timer); resolve(bytes.toString('utf8')); }); child.once('error', reject);
  });
  child.stdin.on('error', () => {});
  child.stdin.write(JSON.stringify({ hook_event_name: 'PreToolUse', tool_name: 'mcp__local_bridge__bridge_context', tool_use_id: 'test-open-pipe', tool_input: {}, session_id: 'test-only', cwd: process.cwd(), transcript_path: resolve('test.jsonl') }) + '\n');
  const result = JSON.parse(await output); assert.equal(result.hookSpecificOutput.permissionDecision, 'deny');
  child.stdin.end();
});
