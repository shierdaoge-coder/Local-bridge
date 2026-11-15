import { access, readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { BridgeClient } from '../src/client.mjs';

const project = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const defaultMarketplace = path.join(project, 'dist', 'codex');
const args = process.argv.slice(2);
function option(name, fallback) { const i = args.indexOf(name); return i < 0 ? fallback : args[i + 1]; }
function check(name, ok, detail) { return { name, status: ok ? 'ok' : 'needs_attention', detail }; }

export async function readiness({ marketplace = defaultMarketplace, pluginData = process.env.PLUGIN_DATA, serviceUrl = process.env.LOCAL_BRIDGE_URL ?? 'http://127.0.0.1:4776' } = {}) {
  marketplace = path.resolve(marketplace);
  const pluginRoot = path.join(marketplace, 'plugins', 'local-bridge');
  const checks = [];
  const paths = [
    path.join(marketplace, '.agents', 'plugins', 'marketplace.json'),
    path.join(pluginRoot, 'plugin.json'), path.join(pluginRoot, '.codex-plugin', 'plugin.json'),
    path.join(pluginRoot, 'mcp.json'), path.join(pluginRoot, '.mcp.json'),
    path.join(pluginRoot, 'hooks', 'hooks.json'), path.join(pluginRoot, 'src', 'codex-mcp.mjs'),
    path.join(pluginRoot, 'src', 'codex-hook.mjs'),
  ];
  const absent = [];
  for (const item of paths) { try { await access(item); } catch { absent.push(path.relative(marketplace, item).replaceAll('\\', '/')); } }
  checks.push(check('local_package', absent.length === 0, absent.length ? `missing: ${absent.join(', ')}` : 'marketplace, MCP entry, and hook files are present'));

  const stale = [];
  for (const name of ['codex-hook.mjs', 'codex-mcp.mjs', 'codex-config.mjs', 'codex-gateway.mjs', 'codex-approval.mjs']) {
    try {
      const [source, packaged] = await Promise.all([readFile(path.join(project, 'src', name)), readFile(path.join(pluginRoot, 'src', name))]);
      if (!source.equals(packaged)) stale.push(name);
    } catch { stale.push(name); }
  }
  checks.push(check('source_freshness', stale.length === 0, stale.length ? `package is missing or stale for: ${stale.join(', ')}; rebuild the selected marketplace` : 'critical Codex source files match the packaged bytes'));

  try {
    const mcp = JSON.parse(await readFile(path.join(pluginRoot, 'mcp.json'), 'utf8')).mcpServers?.local_bridge;
    const privatePath = mcp?.env?.LOCAL_BRIDGE_CONFIG;
    const privateDelivery = typeof privatePath === 'string' && path.isAbsolute(privatePath)
      && (!pluginData || path.resolve(privatePath) === path.resolve(pluginData, 'bridge.json'));
    checks.push(check('mcp_private_config_delivery', privateDelivery, privateDelivery ? 'MCP manifest explicitly supplies the selected private configuration path; contents omitted'
      : 'MCP manifest must set LOCAL_BRIDGE_CONFIG to the selected PLUGIN_DATA/bridge.json; hook environment is not sufficient evidence'));
    const hookDoc = JSON.parse(await readFile(path.join(pluginRoot, 'hooks', 'hooks.json'), 'utf8'));
    const mcpEntry = mcp?.args?.[0], mcpNode = mcp?.command;
    const hookCommand = hookDoc?.hooks?.PreToolUse?.[0]?.hooks?.[0]?.commandWindows ?? hookDoc?.hooks?.PreToolUse?.[0]?.hooks?.[0]?.command;
    let hookEntry = typeof hookCommand === 'string' ? hookCommand.match(/^"([^"]+)"\s+"([^"]+)"/) : null;
    let launcher;
    if (typeof hookCommand === 'string' && hookCommand.endsWith('run-hook.cmd')) {
      launcher = hookCommand;
      const body = await readFile(launcher, 'utf8'), executable = /^@?"([^"]+)"\s+"%~dp0\.\.\\src\\codex-hook\.mjs"/m.exec(body);
      hookEntry = executable ? [null, executable[1], path.resolve(path.dirname(launcher), '../src/codex-hook.mjs')] : null;
    }
    const startupMissing = [];
    for (const [label, item] of [['MCP entry', mcpEntry], ['MCP Node executable', mcpNode], ...(launcher ? [['Windows launcher', launcher]] : []), ['hook entry', hookEntry?.[2]], ['hook Node executable', hookEntry?.[1]]]) {
      if (!item) startupMissing.push(`${label} is not configured`);
      else try { await access(item); } catch { startupMissing.push(`${label} is missing`); }
    }
    checks.push(check('startup_paths', startupMissing.length === 0, startupMissing.length ? startupMissing.join('; ') : 'configured MCP and hook Node/script paths exist'));
  } catch {
    checks.push(check('startup_paths', false, 'MCP or hook manifest is invalid; configured startup paths could not be verified'));
  }

  try {
    const health = await new BridgeClient(serviceUrl, { timeoutMs: 1500 }).request('/api/health');
    const supported = Boolean(health?.features?.codex_hooks && health?.features?.provided_endpoint_credentials && health?.features?.codex_approval_redemption);
    checks.push(check('service_health', supported, supported ? 'loopback service supports native approval redemption and endpoint credentials' : 'service responds but required Codex features are missing; start the intended local-bridge version'));
  } catch {
    checks.push(check('service_health', false, 'local service is unavailable; start the intended bridge service before setup'));
  }

  if (pluginData) {
    const configPath = path.join(pluginData, 'bridge.json');
    try {
      const config = JSON.parse(await readFile(configPath, 'utf8'));
      const bindings = config?.bindings && typeof config.bindings === 'object' && !Array.isArray(config.bindings) ? Object.entries(config.bindings) : [];
      const invalid = bindings.filter(([session, binding]) => !/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i.test(session)
        || !binding || typeof binding !== 'object' || Array.isArray(binding)
        || typeof binding.endpoint_id !== 'string' || !binding.endpoint_id
        || typeof binding.credential !== 'string' || !binding.credential
        || !['cwd', 'transcript_path'].every(key => typeof binding[key] === 'string' && path.isAbsolute(binding[key])));
      const valid = bindings.length > 0 && invalid.length === 0;
      checks.push(check('private_bindings', valid, valid ? `${bindings.length} complete binding(s) in ${configPath}; credentials omitted` : `${bindings.length} binding(s), ${invalid.length} incomplete or invalid; expected session UUID plus endpoint_id, credential, absolute cwd and transcript_path; credentials omitted`));
    } catch (error) {
      checks.push(check('private_bindings', false, error.code === 'ENOENT' ? `no private bridge.json at ${configPath}; this session is not bound yet` : `private bridge.json is unreadable or invalid at ${configPath}; contents omitted`));
    }
  } else checks.push(check('private_bindings', false, 'PLUGIN_DATA is not available; provide --plugin-data or run from an installed plugin hook'));

  return { read_only: true, starts_model: false, installs_plugin: false, native_validation: 'pending-manual-test', checks, ready: checks.every(item => item.status === 'ok') };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const result = await readiness({ marketplace: option('--marketplace', defaultMarketplace), pluginData: option('--plugin-data', process.env.PLUGIN_DATA), serviceUrl: option('--service-url', undefined) });
  process.stdout.write(JSON.stringify(result, null, 2) + '\n');
  if (!result.ready) process.exitCode = 1;
}
