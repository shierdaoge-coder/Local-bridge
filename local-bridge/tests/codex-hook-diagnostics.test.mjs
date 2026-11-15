import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, readFile, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { recordHookDiagnostic } from '../src/codex-hook-diagnostics.mjs';

async function setup(t) {
  const directory = await mkdtemp(path.join(tmpdir(), 'local-bridge-hook-diagnostic-'));
  t.after(async () => { assert.equal(path.dirname(directory), path.resolve(tmpdir())); assert.ok(path.basename(directory).startsWith('local-bridge-hook-diagnostic-')); await rm(directory, { recursive: true, force: true }); });
  return directory;
}
test('hook diagnostics capture native metadata only after a bounded operator opt-in', async t => {
  const directory = await setup(t), now = () => 10000, config = path.join(directory, 'hook-diagnostics-config.json');
  const event = { hook_event_name: 'PreToolUse', session_id: 'test-session', cwd: 'D:/test', transcript_path: 'C:/transcript', tool_name: 'functions.exec', tool_use_id: 'host-call', prompt: 'private prompt never record', tool_input: { code: 'private tool body never record', credential: 'secret never record' } };
  await recordHookDiagnostic(event, { directory, now }); assert.deepEqual(await readdir(directory), []);
  await writeFile(config, JSON.stringify({ session_ids: ['test-session'], expires_at: new Date(20000).toISOString() }));
  await recordHookDiagnostic(event, { directory, now, env: { PLUGIN_DATA: 'C:/host/data', PLUGIN_ROOT: 'C:/host/plugin', AUTH_TOKEN: 'unrelated secret never record' } });
  const saved = await readFile(path.join(directory, 'hook-diagnostic-PreToolUse.json'), 'utf8'), value = JSON.parse(saved);
  assert.equal(value.session_id, 'test-session'); assert.equal(value.tool_name, 'functions.exec'); assert.equal(value.plugin_data, 'C:/host/data');
  assert.doesNotMatch(saved, /private prompt|private tool body|secret never record/);
  assert.equal(Object.hasOwn(value, 'tool_input'), false); assert.equal(Object.hasOwn(value, 'prompt'), false);
});
test('hook diagnostics ignore unrelated sessions, expired and malformed opt-ins', async t => {
  const directory = await setup(t), file = path.join(directory, 'hook-diagnostics-config.json'), now = () => 10000, event = { hook_event_name: 'Stop', session_id: 'wrong-session' };
  await writeFile(file, JSON.stringify({ session_ids: ['selected-session'], expires_at: new Date(20000).toISOString() }));
  await recordHookDiagnostic(event, { directory, now }); assert.deepEqual(await readdir(directory), ['hook-diagnostics-config.json']);
  await writeFile(file, JSON.stringify({ session_ids: ['wrong-session'], expires_at: new Date(5000).toISOString() }));
  await recordHookDiagnostic(event, { directory, now }); assert.deepEqual(await readdir(directory), ['hook-diagnostics-config.json']);
  await writeFile(file, '{not json'); await recordHookDiagnostic(event, { directory, now }); assert.deepEqual(await readdir(directory), ['hook-diagnostics-config.json']);
});
