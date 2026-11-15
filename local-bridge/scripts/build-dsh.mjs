import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';

const project = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
export const DSH_PACKAGE = '@local/local-bridge';
const sourceNames = ['src/protocol.mjs', 'src/client.mjs', 'src/session-tools.mjs', 'src/adapters/dsh.mjs', 'src/dsh-outbox.mjs',
  'src/dsh-service-start.mjs', 'src/dsh-plugin.mjs', 'src/history-sync-host.mjs', 'src/history-sync.py'];
const within = (p, root) => { const rel = path.relative(root, p); return rel === '' || (rel !== '..' && !rel.startsWith('..' + path.sep) && !path.isAbsolute(rel)); };
async function ensure(file, data) {
  await mkdir(path.dirname(file), { recursive: true }); const bytes = Buffer.isBuffer(data) ? data : Buffer.from(data);
  try { if (!(await readFile(file)).equals(bytes)) throw Error('BUILD_CONFLICT: ' + file); }
  catch (e) { if (e.code !== 'ENOENT') throw e; await writeFile(file, bytes, { flag: 'wx' }); }
}
export async function buildDsh(output = path.join(project, 'dist', 'dsh'), { bindingFile, sdkEntry = process.env.DSH_NATIVE_PERSISTENCE_ENTRY } = {}) {
  if (!path.isAbsolute(output)) throw Error('Output must be an absolute directory');
  const root = path.resolve(output);
  if (root === path.parse(root).root || within(project, root) || (within(root, project) && !within(root, path.join(project, 'dist')))) throw Error('Unsafe output path');
  if (typeof sdkEntry !== 'string' || !path.isAbsolute(sdkEntry)) throw Error('Supply the absolute installed DSH SDK entry with --sdk-entry or DSH_NATIVE_PERSISTENCE_ENTRY');
  // Read all inputs before writing, so a missing implementation cannot create a misleading package.
  const sources = await Promise.all(sourceNames.map(async name => [name, await readFile(path.join(project, name))]));
  const manifest = { name: DSH_PACKAGE, version: '0.1.4-dev', private: true, type: 'module',
    description: 'Local history synchronization and event messaging for DSH.', exports: { '.': './src/dsh-plugin.mjs' },
    engines: { node: '>=24', dsh: '0.2.1-rc.3' }, dsh: { manifestVersion: 1, bundle: { patch: './cordis.patch.yml' } } };
  const binding = bindingFile ?? path.join(root, 'PLUGIN_DATA', 'bridge.json');
  if (!path.isAbsolute(binding)) throw Error('Binding file must be an absolute path');
  const patch = '- insert:\n    - id: local-bridge\n      name: ' + JSON.stringify(DSH_PACKAGE) + '\n      config:\n        binding_file: ' + JSON.stringify(binding) + '\n        sdk_entry: ' + JSON.stringify(sdkEntry) + '\n';
  await ensure(path.join(root, 'package.json'), JSON.stringify(manifest, null, 2) + '\n');
  await ensure(path.join(root, 'cordis.patch.yml'), patch);
  await ensure(path.join(root, 'PLUGIN_DATA', 'bridge.example.json'), JSON.stringify({ service_url: 'http://127.0.0.1:4776', bindings: {} }, null, 2) + '\n');
  await ensure(path.join(root, 'README.md'), `# local-bridge for DSH

从源码根目录运行以下构建命令。将 ABSOLUTE_DSH_SDK_ENTRY 换成目标机器上 DSH 自带 SDK 入口文件的绝对路径，将 ABSOLUTE_DSH_PRIVATE_BRIDGE_JSON 换成私密绑定文件 bridge.json 的完整绝对路径。

    node scripts/build-dsh.mjs --sdk-entry "ABSOLUTE_DSH_SDK_ENTRY" --binding-file "ABSOLUTE_DSH_PRIVATE_BRIDGE_JSON"

SDK 路径和绑定文件路径写入 cordis.patch.yml。移动相关目录后重新构建。

私密绑定文件由 cordis.patch.yml 指定，可在构建时用 --binding-file 选择。待发账本 bridge-outbox.sqlite 位于同一目录。pending_local 表示待发，local_failed 表示终止重试。重试沿用原消息 ID。

有关配置、本地安装和服务自动启动的步骤，见源码目录的 DSH-SETUP.md。
`);
  for (const [name, data] of sources) await ensure(path.join(root, name), data);
  return { root, binding_file: binding, files: sources.length + 4 };
}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const { values } = parseArgs({ options: { output: { type: 'string' }, 'binding-file': { type: 'string' }, 'sdk-entry': { type: 'string' } } });
    process.stdout.write(JSON.stringify(await buildDsh(values.output, { bindingFile: values['binding-file'], sdkEntry: values['sdk-entry'] }), null, 2) + '\n');
  } catch (e) { process.stderr.write(e.message + '\n'); process.exitCode = 1; }
}
