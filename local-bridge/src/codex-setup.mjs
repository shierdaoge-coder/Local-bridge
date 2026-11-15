import { readFile, mkdir, open, rename, unlink } from 'node:fs/promises';
import { join, resolve, isAbsolute } from 'node:path';
import { fileURLToPath } from 'node:url';
import { randomBytes, randomUUID } from 'node:crypto';
import { BridgeClient } from './client.mjs';
import { insist, object } from './protocol.mjs';

const UUID = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i;
async function readOptional(path) { try { return await readFile(path, 'utf8'); } catch (e) { if (e.code === 'ENOENT') return null; throw e; } }

/** Operator-only setup; dry by default. Saves a private recovery candidate before registration. */
export async function provisionCodex(options, { client } = {}) {
  insist(UUID.test(options.session), 'INVALID_INPUT', 'An exact Codex session UUID is required');
  for (const key of ['pluginData', 'cwd', 'transcript']) insist(typeof options[key] === 'string' && isAbsolute(options[key]), 'INVALID_INPUT', key + ' must be absolute');
  const path = join(resolve(options.pluginData), 'bridge.json'), endpointId = 'codex-' + options.session;
  const endpoint = { id: endpointId, harness: 'codex', native_session_id: options.session, work_domains: ['text', 'engineering'] };
  if (!options.write) return { writes: false, config_file: path, endpoint, native_delivery_enabled: false };
  let serviceUrl = options.serviceUrl ?? 'http://127.0.0.1:4776';
  if (!client) {
    insist(typeof options.serviceConfig === 'string' && isAbsolute(options.serviceConfig), 'INVALID_INPUT', 'Absolute service config file required');
    const service = JSON.parse(await readFile(options.serviceConfig, 'utf8'));
    serviceUrl = options.serviceUrl ?? 'http://127.0.0.1:' + service.port;
    client = new BridgeClient(serviceUrl, { adminToken: service.admin_token });
  }
  const health = await client.request('/api/health');
  insist(health?.features?.codex_hooks && health.features.provided_endpoint_credentials && health.features.codex_approval_redemption, 'CAPABILITY_MISSING', 'Restart or update the local bridge service before binding a Codex session');
  const previous = await readOptional(path), config = previous ? object(JSON.parse(previous), 'existing config') : { service_url: serviceUrl, bindings: {} };
  insist(config.service_url === serviceUrl, 'CONFIG_CONFLICT', 'Existing bindings use a different local service');
  object(config.bindings, 'bindings'); insist(!Object.hasOwn(config.bindings, options.session), 'ALREADY_REGISTERED', 'This native session already has a private binding');
  await mkdir(options.pluginData, { recursive: true });
  const lockPath = join(options.pluginData, '.bridge-setup.lock'), lock = await open(lockPath, 'wx', 0o600);
  const pending = join(options.pluginData, '.bridge-pending-' + randomUUID() + '.json'); let candidateSaved = false;
  try {
    const credential = randomBytes(32).toString('base64url');
    config.bindings[options.session] = { endpoint_id: endpointId, credential, cwd: resolve(options.cwd), transcript_path: resolve(options.transcript) };
    const handle = await open(pending, 'wx', 0o600);
    try { await handle.writeFile(JSON.stringify(config, null, 2) + '\n'); await handle.sync(); candidateSaved = true; } finally { await handle.close(); }
    // Credential is already recoverable on disk even if the HTTP response is lost.
    await client.request('/api/endpoints', { method: 'POST', admin: true, body: { ...endpoint, credential } });
    insist(await readOptional(path) === previous, 'CONFIG_CONFLICT', 'Private config changed while setup was running; recover the pending candidate manually');
    await rename(pending, path); candidateSaved = false;
    return { writes: true, endpoint_id: endpointId, config_file: path, native_delivery_enabled: false };
  } finally {
    await lock.close(); await unlink(lockPath);
    // An unsent/uncertain candidate remains private for operator reconciliation.
    if (!candidateSaved) await unlink(pending).catch(error => { if (error.code !== 'ENOENT') throw error; });
  }
}

function parse(args) {
  const flags = { '--session': 'session', '--plugin-data': 'pluginData', '--cwd': 'cwd', '--transcript': 'transcript', '--service-config': 'serviceConfig', '--service-url': 'serviceUrl' }, output = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--write') output.write = true;
    else { insist(flags[args[i]] && args[i + 1] && !args[i + 1].startsWith('--'), 'INVALID_INPUT', 'Unknown or incomplete setup option'); output[flags[args[i]]] = args[++i]; }
  }
  return output;
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  if (process.argv.includes('--help')) process.stdout.write('Operator setup (dry by default): --session UUID --plugin-data ABS_DIR --cwd ABS_DIR --transcript ABS_FILE [--service-config ABS_FILE --write]\n');
  else provisionCodex(parse(process.argv.slice(2))).then(result => process.stdout.write(JSON.stringify(result, null, 2) + '\n')).catch(error => { process.stderr.write('local-bridge setup failed (' + (error.code ?? 'SETUP_FAILED') + '); check for a private .bridge-pending file before retrying.\n'); process.exitCode = 1; });
}
