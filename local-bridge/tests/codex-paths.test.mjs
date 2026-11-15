import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, realpathSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, toNamespacedPath, dirname, resolve, basename } from 'node:path';
import { hookBinding } from '../src/codex-config.mjs';
import { runCodexHook } from '../src/codex-hook.mjs';

test('native Windows namespace aliases match the same bound directory and transcript', { skip: process.platform !== 'win32' }, t => {
  const directory = mkdtempSync(join(tmpdir(), 'bridge-native-path-'));
  t.after(() => { assert.equal(dirname(directory), resolve(tmpdir())); assert.ok(basename(directory).startsWith('bridge-native-path-')); rmSync(directory, { recursive: true, force: true }); });
  const transcript = join(directory, 'rollout.jsonl'); writeFileSync(transcript, '{}\n');
  const binding = { endpoint_id: 'selected-endpoint', credential: 'private', cwd: directory, transcript_path: transcript };
  const config = { bindings: { selected: binding } };
  assert.equal(realpathSync.native(transcript), realpathSync.native(toNamespacedPath(transcript)));
  for (const event of [
    { session_id: 'selected', cwd: directory, transcript_path: toNamespacedPath(transcript) },
    { session_id: 'selected', cwd: toNamespacedPath(directory), transcript_path: transcript },
    { session_id: 'selected', cwd: toNamespacedPath(directory), transcript_path: toNamespacedPath(transcript) },
  ]) assert.equal(hookBinding(config, event), binding);
  const another = join(directory, 'another.jsonl'); writeFileSync(another, '{}\n');
  assert.throws(() => hookBinding(config, { session_id: 'selected', cwd: directory, transcript_path: toNamespacedPath(another) }), error => error.code === 'CALLER_MISMATCH');
  assert.throws(() => hookBinding(config, { session_id: 'selected', cwd: directory, transcript_path: transcript, agent_id: 'child' }), error => error.code === 'CALLER_MISMATCH');
});

test('namespace compatibility does not accept a relative or different calling path', () => {
  const cwd = resolve(tmpdir()), transcript = join(cwd, 'bound-rollout.jsonl');
  const config = { bindings: { selected: { endpoint_id: 'selected-endpoint', credential: 'private', cwd, transcript_path: transcript } } };
  assert.throws(() => hookBinding(config, { session_id: 'selected', cwd, transcript_path: 'bound-rollout.jsonl' }), error => error.code === 'CALLER_MISMATCH');
  assert.throws(() => hookBinding(config, { session_id: 'selected', cwd: join(cwd, 'different'), transcript_path: transcript }), error => error.code === 'CALLER_MISMATCH');
  assert.equal(hookBinding(config, { session_id: 'another', cwd, transcript_path: transcript }), null);
});

test('native hook failure reports an allowlisted code without error details or secrets', async () => {
  const cwd = resolve(tmpdir()), transcript = join(cwd, 'bound-rollout.jsonl');
  const event = { hook_event_name: 'PreToolUse', session_id: 'selected', cwd, transcript_path: transcript, tool_name: 'mcp__local_bridge__bridge_context', tool_use_id: 'native-id', tool_input: {} };
  const config = { bindings: { selected: { endpoint_id: 'selected-endpoint', credential: 'private-credential-never-echo', cwd, transcript_path: transcript } } };
  const output = await runCodexHook(event, { config, client: { request: async () => { throw Object.assign(Error('private-credential-never-echo'), { code: 'CALLER_MISMATCH' }); } } });
  assert.equal(output.hookSpecificOutput.permissionDecision, 'deny');
  assert.match(output.hookSpecificOutput.permissionDecisionReason, /\[CALLER_MISMATCH\]/);
  assert.doesNotMatch(JSON.stringify(output), /private-credential-never-echo/);
  const other = await runCodexHook(event, { config, client: { request: async () => { throw Object.assign(Error('secret'), { code: 'UNLISTED_SECRET' }); } } });
  assert.doesNotMatch(JSON.stringify(other), /UNLISTED_SECRET|secret/);
});
