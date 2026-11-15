import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtemp, rm, readFile } from 'node:fs/promises';
import path from 'node:path';
import { tmpdir } from 'node:os';
import { build } from '../scripts/build-codex.mjs';

test('Windows hook launcher returns valid JSON through both CMD and PowerShell without special outer quoting', { skip: process.platform !== 'win32' }, async t => {
  const dir = await mkdtemp(path.join(tmpdir(), 'local-bridge-launcher-'));
  t.after(async () => { assert.equal(path.dirname(dir), path.resolve(tmpdir())); assert.ok(path.basename(dir).startsWith('local-bridge-launcher-')); await rm(dir, { recursive: true, force: true }); });
  await build(dir);
  const root = path.join(dir, 'plugins/local-bridge'), hooks = JSON.parse(await readFile(path.join(root, 'hooks/hooks.json'), 'utf8')), command = hooks.hooks.PreToolUse[0].hooks[0].command;
  const input = JSON.stringify({ hook_event_name: 'PreToolUse', session_id: 'isolated-launcher', tool_name: 'mcp__local_bridge__bridge_context', tool_use_id: 'call', tool_input: {}, cwd: dir, transcript_path: path.join(dir, 'rollout.jsonl') });
  const powershell = path.join(process.env.SystemRoot ?? 'C:/Windows', 'System32/WindowsPowerShell/v1.0/powershell.exe');
  for (const [label, executable, args] of [['cmd', process.env.ComSpec ?? 'C:/Windows/System32/cmd.exe', ['/C', command]], ['powershell', powershell, ['-NoLogo', '-NoProfile', '-NonInteractive', '-Command', command]]]) {
    const child = spawn(executable, args, { windowsHide: true, windowsVerbatimArguments: label === 'cmd', env: { ...process.env, LOCAL_BRIDGE_CONFIG: path.join(dir, 'absent-private-config.json') }, stdio: ['pipe', 'pipe', 'pipe'] });
    let stdout = '', stderr = ''; child.stdout.on('data', b => { stdout += b; }); child.stderr.on('data', b => { stderr += b; }); child.stdin.on('error', () => {}); child.stdin.end(input);
    const exit = await new Promise((done, reject) => { const timer = setTimeout(() => { child.kill(); reject(Error(label + ' launcher timeout')); }, 5000); child.once('error', reject); child.once('close', code => { clearTimeout(timer); done(code); }); });
    assert.equal(exit, 0, stderr); assert.equal(JSON.parse(stdout).hookSpecificOutput.permissionDecision, 'deny', label);
  }
});
