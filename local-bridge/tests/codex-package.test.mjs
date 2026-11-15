import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, readFile, readdir } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { build } from '../scripts/build-codex.mjs';

test('build is repeatable and excludes runtime material', async () => {
 const dir = await mkdtemp(path.join(os.tmpdir(), 'local-bridge-package-'));
 const target = path.resolve(dir, 'out');
 try {
  await build(target); const first = await readFile(path.join(target, 'plugins/local-bridge/src/core.mjs'));
  await build(target); assert.deepEqual(await readFile(path.join(target, 'plugins/local-bridge/src/core.mjs')), first);
  const root = path.join(target, 'plugins/local-bridge');
  const walk = async d => { const out=[]; for (const e of await readdir(d, { withFileTypes: true })) { if (e.isDirectory()) out.push(...await walk(path.join(d,e.name))); else out.push(path.join(d,e.name)); } return out; };
  const names = await walk(root);
  assert.equal(names.some(p => p.includes(`${path.sep}.runtime${path.sep}`) || /secret|credential/i.test(path.basename(p))), false);
  const manifest = JSON.parse(await readFile(path.join(root, 'plugin.json'), 'utf8'));
  assert.equal(manifest.extensions['com.openai'].hooks, './hooks/hooks.json');
  assert.equal(manifest.extensions['com.openai'].interface.displayName, 'local-bridge');
  const mcp = JSON.parse(await readFile(path.join(root, 'mcp.json'), 'utf8'));
  assert.equal(mcp.mcpServers.local_bridge.type, 'stdio');
  assert.equal(mcp.mcpServers.local_bridge.command, process.execPath);
  assert.deepEqual(mcp.mcpServers.local_bridge.args, [path.join(root, 'src/codex-mcp.mjs')]);
  assert.equal(mcp.mcpServers.local_bridge.cwd, root);
  const hooks = JSON.parse(await readFile(path.join(root, 'hooks/hooks.json'), 'utf8'));
  const preToolMatcher = new RegExp(hooks.hooks.PreToolUse[0].matcher);
  assert.ok(preToolMatcher.test('mcp__local_bridge__bridge_context'));
  assert.equal(preToolMatcher.test('Bash'), false);
  const hookCommand = hooks.hooks.PreToolUse[0].hooks[0].command;
  if (process.platform === 'win32') {
    assert.equal(hookCommand, path.join(root, 'hooks/run-hook.cmd'));
    const launcher = await readFile(hookCommand, 'utf8'); assert.ok(launcher.includes(process.execPath)); assert.ok(launcher.includes('%~dp0..\\src\\codex-hook.mjs'));
  } else assert.ok(hookCommand.includes(path.join(root, 'src/codex-hook.mjs')));
  const catalog = JSON.parse(await readFile(path.join(target, '.agents/plugins/marketplace.json'), 'utf8'));
  const entry = catalog.plugins[0];
  assert.deepEqual(entry.source, { source: 'local', path: './plugins/local-bridge' });
  assert.deepEqual(entry.policy, { installation: 'AVAILABLE', authentication: 'ON_INSTALL' });
  assert.equal((await readFile(path.join(target, entry.source.path, 'plugin.json'), 'utf8')).length > 0, true);
  assert.match(await readFile(path.join(root, 'src/history-sync.py'), 'utf8'), /^AUTO_SYNC_HISTORY = False$/m);
  const protectedFile = path.join(root, 'plugin.json');
  const { writeFile } = await import('node:fs/promises');
  await writeFile(protectedFile, 'user data');
  await assert.rejects(build(target), /BUILD_CONFLICT/);
  assert.equal(await readFile(protectedFile, 'utf8'), 'user data');
 } finally {
  const resolved = path.resolve(dir);
  if (path.dirname(resolved) !== path.resolve(os.tmpdir()) || !path.basename(resolved).startsWith('local-bridge-package-')) throw Error('Refusing unsafe test cleanup');
  await rm(resolved, { recursive: true, force: true });
 }
});

test('rejects unsafe and relative output paths', async () => {
 const { fileURLToPath } = await import('node:url');
 await assert.rejects(build(path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')));
 await assert.rejects(build('relative-out'));
  await assert.rejects(build(path.resolve(os.tmpdir(), 'unused-bridge-output'), { pluginData: 'relative' }), /absolute/);
});
