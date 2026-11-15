import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { spawn } from 'node:child_process';
import { isMainModule } from '../src/entrypoint.mjs';

const hook = resolve('src/codex-hook.mjs');

test('entrypoint recognizes direct execution and rejects imports', () => {
  assert.equal(isMainModule(new URL('../src/codex-hook.mjs', import.meta.url).href, { argv1: hook }), true);
  assert.equal(isMainModule(new URL('../src/codex-hook.mjs', import.meta.url).href, { argv1: undefined }), false);
});

test('hook invoked through a junction alias reads stdin and returns denial', async t => {
  if (process.platform !== 'win32') return t.skip('Windows junction behavior');
  const root = await mkdtemp(join(tmpdir(), 'codex-entry-test-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const aliasDir = join(root, 'alias');
  await symlink(resolve('src'), aliasDir, 'junction');

  const child = spawn(process.execPath, [join(aliasDir, 'codex-hook.mjs')], {
    windowsHide: true,
    env: { ...process.env, LOCAL_BRIDGE_CONFIG: join(root, 'missing-config.json'), PLUGIN_DATA: join(root, 'plugin-data-no-opt-in') },
    stdio: ['pipe', 'pipe', 'pipe'],
  });
  t.after(() => { if (child.exitCode === null) child.kill(); });
  const stdout = new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(Error('Hook did not answer')), 3000);
    child.stdout.once('data', bytes => { clearTimeout(timer); resolve(bytes.toString('utf8')); });
    child.once('error', reject);
  });
  child.stdin.on('error', () => {});
  child.stdin.write(JSON.stringify({ hook_event_name: 'PreToolUse', tool_name: 'mcp__local_bridge__bridge_context', tool_use_id: 'isolated-fixture', tool_input: {}, session_id: 'isolated-fixture', cwd: root, transcript_path: join(root, 'fixture.jsonl') }) + '\n');
  const result = JSON.parse(await stdout);
  assert.equal(result.hookSpecificOutput.permissionDecision, 'deny');
  child.stdin.end();
});

test('importing hook module does not invoke its CLI main', async () => {
  const child = spawn(process.execPath, ['--input-type=module', '-e', `await import(${JSON.stringify(new URL('../src/codex-hook.mjs', import.meta.url).href)})`], { windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'] });
  child.stdin.end(JSON.stringify({ hook_event_name: 'PreToolUse', tool_name: 'mcp__local_bridge__bridge_context' }));
  let output = '';
  for await (const chunk of child.stdout) output += chunk;
  assert.equal(output, '');
});
