import { readFile, mkdir, open, rename, unlink } from 'node:fs/promises';
import { join, resolve, isAbsolute } from 'node:path';
import { fileURLToPath } from 'node:url';
import { randomBytes, randomUUID } from 'node:crypto';
import { BridgeClient } from './client.mjs';
import { insist, object, text } from './protocol.mjs';

const optional = async file => { try { return await readFile(file, 'utf8'); } catch (e) { if (e.code === 'ENOENT') return null; throw e; } };

/** Operator-only registration; no model tools can create a binding or grant. */
export async function provisionDsh(options, { client } = {}) {
  text(options.session, 'native DSH session id', 192);
  insist(/^[A-Za-z0-9_-]+$/.test(options.session), 'INVALID_INPUT', 'Use the exact native session id, not a display name');
  insist(typeof options.pluginData === 'string' && isAbsolute(options.pluginData), 'INVALID_INPUT', 'pluginData must be an absolute directory');
  const path = join(resolve(options.pluginData), 'bridge.json'), endpointId = 'dsh-' + options.session;
  const endpoint = { id: endpointId, harness: 'dsh', native_session_id: options.session, work_domains: ['text'] };
  if (!options.write) return { writes: false, config_file: path, endpoint, native_delivery_enabled: false };
  let serviceUrl = options.serviceUrl ?? 'http://127.0.0.1:4776';
  if (!client) {
    insist(typeof options.serviceConfig === 'string' && isAbsolute(options.serviceConfig), 'INVALID_INPUT', 'Absolute service config required');
    const service = JSON.parse(await readFile(options.serviceConfig, 'utf8'));
    serviceUrl = options.serviceUrl ?? 'http://127.0.0.1:' + service.port;
    client = new BridgeClient(serviceUrl, { adminToken: service.admin_token });
  }
  const health = await client.request('/api/health');
  insist(health?.features?.dsh_host && health.features.provided_endpoint_credentials, 'CAPABILITY_MISSING', 'Update the bridge service before binding DSH');
  const previous = await optional(path), config = previous ? object(JSON.parse(previous), 'existing config') : { service_url: serviceUrl, bindings: {} };
  insist(config.service_url === serviceUrl, 'CONFIG_CONFLICT', 'Existing bindings use a different service');
  object(config.bindings, 'bindings'); insist(!Object.hasOwn(config.bindings, options.session), 'ALREADY_REGISTERED', 'Native session already has a private binding');
  await mkdir(options.pluginData, { recursive: true });
  const lockPath = join(options.pluginData, '.bridge-setup.lock'), lock = await open(lockPath, 'wx', 0o600);
  const pending = join(options.pluginData, '.bridge-pending-' + randomUUID() + '.json'); let saved = false;
  try {
    const credential = randomBytes(32).toString('base64url');
    config.bindings[options.session] = { endpoint_id: endpointId, credential };
    const handle = await open(pending, 'wx', 0o600);
    try { await handle.writeFile(JSON.stringify(config, null, 2) + '\n'); await handle.sync(); saved = true; } finally { await handle.close(); }
    await client.request('/api/endpoints', { method: 'POST', admin: true, body: { ...endpoint, credential } });
    insist(await optional(path) === previous, 'CONFIG_CONFLICT', 'Private config changed; recover the pending candidate manually');
    await rename(pending, path); saved = false;
    return { writes: true, endpoint_id: endpointId, config_file: path, native_delivery_enabled: false, reload_plugin: true };
  } finally {
    await lock.close(); await unlink(lockPath);
    if (!saved) await unlink(pending).catch(e => { if (e.code !== 'ENOENT') throw e; });
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const flags = { '--session': 'session', '--plugin-data': 'pluginData', '--service-config': 'serviceConfig', '--service-url': 'serviceUrl' }, args = {};
  if (process.argv.includes('--help')) process.stdout.write('DSH operator setup (dry by default): --session NATIVE_ID --plugin-data ABS_DIR [--service-config ABS_FILE --write]\n');
  else {
    try {
      const argv = process.argv.slice(2);
      for (let i = 0; i < argv.length; i++) {
        if (argv[i] === '--write') args.write = true;
        else { insist(flags[argv[i]] && argv[i + 1] && !argv[i + 1].startsWith('--'), 'INVALID_INPUT', 'Unknown or incomplete setup option'); args[flags[argv[i]]] = argv[++i]; }
      }
      const result = await provisionDsh(args); process.stdout.write(JSON.stringify(result, null, 2) + '\n');
    } catch (error) { process.stderr.write('DSH setup failed (' + (error.code ?? 'SETUP_FAILED') + '); inspect private .bridge-pending candidates before retrying.\n'); process.exitCode = 1; }
  }
}
