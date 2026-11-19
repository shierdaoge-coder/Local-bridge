import { mkdir, readFile, readdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { serviceBundleFiles } from './service-bundle.mjs';

const project = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const node = process.execPath;
const plugin = 'local-bridge';
function hookDefinitions(root, diagnosticHooks) {
const hookCommand = process.platform === 'win32' ? path.join(root, 'hooks', 'run-hook.cmd') : `"${node}" "${path.join(root, 'src/codex-hook.mjs')}"`;
if (process.platform === 'win32' && /[\s&|<>^()%!"']/.test(hookCommand)) throw Error('Windows local hook bundle path must not contain whitespace or shell metacharacters; choose another --output directory');
return { hooks: {
  UserPromptSubmit: [{ hooks: [{ type: 'command', command: hookCommand, commandWindows: hookCommand, timeout: 5 }] }],
  SessionStart: [{ hooks: [{ type: 'command', command: hookCommand, commandWindows: hookCommand, timeout: 5 }] }],
  Stop: [{ hooks: [{ type: 'command', command: hookCommand, commandWindows: hookCommand, timeout: 3 }] }],
  Interrupt: [{ hooks: [{ type: 'command', command: hookCommand, commandWindows: hookCommand, timeout: 3 }] }],
  SessionEnd: [{ hooks: [{ type: 'command', command: hookCommand, commandWindows: hookCommand, timeout: 3 }] }],
  PreToolUse: [{ matcher: diagnosticHooks ? '.*' : '^mcp__local_bridge__bridge_.*$', hooks: [{ type: 'command', command: hookCommand, commandWindows: hookCommand, timeout: 5 }] }]
} };
}
const base = { name: plugin, version: '0.1.12-dev', description: 'Local shared history synchronization and event messaging for registered sessions.', extensions: { 'com.openai': { hooks: './hooks/hooks.json', interface: { displayName: plugin } } } };
const configExample = { service_url: 'http://127.0.0.1:4780', bindings: {} };
function packageFiles(root, diagnosticHooks, pluginData) {
const mcp = { mcpServers: { local_bridge: { type: 'stdio', command: node, args: [path.join(root, 'src/codex-mcp.mjs')], cwd: root,
  ...(pluginData ? { env: { LOCAL_BRIDGE_CONFIG: path.join(pluginData, 'bridge.json') } } : {}),
} } };
return new Map([
 ['plugin.json', JSON.stringify(base, null, 2) + '\n'],
 ['.codex-plugin/plugin.json', JSON.stringify({ ...base, mcpServers: './.mcp.json', hooks: './hooks/hooks.json' }, null, 2) + '\n'],
 ['mcp.json', JSON.stringify(mcp, null, 2) + '\n'], ['.mcp.json', JSON.stringify(mcp, null, 2) + '\n'],
 ['hooks/hooks.json', JSON.stringify(hookDefinitions(root, diagnosticHooks), null, 2) + '\n'],
 ...(process.platform === 'win32' ? [['hooks/run-hook.cmd', `@echo off\r\n@"${node}" "%~dp0..\\src\\codex-hook.mjs"\r\n`]] : []),
 ['PLUGIN_DATA/bridge.example.json', JSON.stringify(configExample, null, 2) + '\n'],
 ['README.md', `# local-bridge for Codex

启动配置引用构建目录和构建时的 Node 可执行文件。移动目录或换机器后重新构建。

从此插件目录运行以下命令，预览所选会话的绑定。

    node src/codex-setup.mjs --session "CODEX_SESSION_UUID" --plugin-data "ABSOLUTE_CODEX_PLUGIN_DATA" --cwd "ABSOLUTE_WORKSPACE_DIRECTORY" --transcript "ABSOLUTE_CODEX_TRANSCRIPT_PATH"

将占位符换成所选会话的 UUID 和目标机器上的绝对路径。ABSOLUTE_CODEX_PLUGIN_DATA 对应宿主提供的 PLUGIN_DATA 目录。工作目录和归档文件路径取自该会话的 cwd 和 transcript_path。

与接入同一共享服务有关的步骤见源码目录中的 CODEX-SETUP.md。使用本包的共享服务入口时，参见 SHARED-SERVICE.md。
`]
]);
}
function parseArgs(args) { const i = args.indexOf('--output'); if (i < 0) return path.join(project, 'dist', 'codex'); if (!args[i + 1] || args[i + 1].startsWith('--')) throw Error('--output requires an absolute directory'); if (!path.isAbsolute(args[i + 1])) throw Error('--output must be absolute'); return path.resolve(args[i + 1]); }
function within(p, root) { const rel = path.relative(root, p); return rel === '' || (!rel.startsWith('..' + path.sep) && rel !== '..' && !path.isAbsolute(rel)); }
async function ensureFile(dst, data) { await mkdir(path.dirname(dst), { recursive: true }); const bytes = Buffer.isBuffer(data) ? data : Buffer.from(data); try { const old = await readFile(dst); if (!old.equals(bytes)) throw Error(`BUILD_CONFLICT: ${dst}`); } catch (e) { if (e.code !== 'ENOENT') throw e; await writeFile(dst, bytes, { flag: 'wx' }); } }
async function sourceFiles(dir, prefix, out) { for (const ent of await readdir(dir, { withFileTypes: true })) { const p = path.join(dir, ent.name); if (ent.isDirectory()) await sourceFiles(p, prefix, out); else if (ent.isFile() && (/\.(mjs|js|html|css)$/i.test(ent.name) || p === path.join(project, 'src', 'history-sync.py'))) out.push([path.join(prefix, path.relative(path.join(project, prefix), p)), p]); } }
function privateDataArg(args) { const index = args.indexOf('--plugin-data'); if (index < 0) return undefined; if (!args[index + 1] || args[index + 1].startsWith('--')) throw Error('--plugin-data requires an absolute directory'); return args[index + 1]; }
export async function build(output = parseArgs(process.argv.slice(2)), { diagnosticHooks = process.argv.includes('--diagnostic-hooks'), pluginData = privateDataArg(process.argv.slice(2)) } = {}) {
 if (!path.isAbsolute(output)) throw Error('Output must be an absolute directory');
 if (pluginData !== undefined && (typeof pluginData !== 'string' || !path.isAbsolute(pluginData))) throw Error('PLUGIN_DATA must be an absolute directory');
 const resolved = path.resolve(output), dist = path.join(project, 'dist');
 if (resolved === path.parse(resolved).root || within(project, resolved) || (within(resolved, project) && !within(resolved, dist))) throw Error('Output path is within or above the source tree or a protected directory');
 const marketplace = path.join(resolved, '.agents', 'plugins');
 await ensureFile(path.join(marketplace, 'marketplace.json'), JSON.stringify({ name: 'local-bridge', plugins: [{ name: plugin, source: { source: 'local', path: `./plugins/${plugin}` }, policy: { installation: 'AVAILABLE', authentication: 'ON_INSTALL' } }] }, null, 2) + '\n');
 const root = path.join(resolved, 'plugins', plugin);
 for (const [rel, body] of packageFiles(root, diagnosticHooks, pluginData)) await ensureFile(path.join(root, rel), body);
 const list = serviceBundleFiles.filter(rel => !rel.startsWith('src/') && !rel.startsWith('web/')).map(rel => [rel, path.join(project, rel)]);
 for (const dir of ['src', 'web']) await sourceFiles(path.join(project, dir), dir, list);
 for (const [rel, src] of list) await ensureFile(path.join(root, rel), await readFile(src));
 return resolved;
}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) build().then(p => console.log(`Built Codex plugin at ${p}`)).catch(e => { console.error(e.message); process.exitCode = 1; });
