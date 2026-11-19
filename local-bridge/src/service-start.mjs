import { spawn } from 'node:child_process';
import { readFile, realpath, stat } from 'node:fs/promises';
import { basename, dirname, isAbsolute, join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { fileURLToPath } from 'node:url';
import { digest, insist, keys, object } from './protocol.mjs';

const bundledEntry = fileURLToPath(new URL('../scripts/shared-service.mjs', import.meta.url));
const json = async path => JSON.parse((await readFile(path, 'utf8')).replace(/^\uFEFF/, ''));
const optionalJson = async path => {
  try { return await json(path); }
  catch (error) { if (error.code !== 'ENOENT') throw error; }
};
const samePath = (a, b) => process.platform === 'win32' ? a.toLowerCase() === b.toLowerCase() : a === b;

export function isDailyUseAutoStartHeld(control) {
  return control?.hold_autostart === true || control?.action === 'stop' || control?.action === 'rollback';
}

export async function readSharedServiceConfig(directory) {
  const config = object(await json(join(directory, 'config.json')), 'shared service config');
  const { port, directory: data } = config;
  insist(Number.isInteger(port) && port > 0 && port <= 65535 && port !== 4776 && config.nativeAdapters === true
    && typeof data === 'string' && isAbsolute(data) && samePath(await realpath(data), join(directory, 'data')),
  'CONFIG_CONFLICT', 'Shared service requires its configured loopback port and a data directory beside config.json');
  return config;
}

async function matchesLedger(config, harness, bindings) {
  const file = join(config.directory, 'candidate.sqlite');
  try { await stat(file); }
  catch (error) { if (error.code === 'ENOENT') return false; throw error; }
  const db = new DatabaseSync(file, { readOnly: true });
  try {
    const lookup = db.prepare('SELECT credential_hash FROM endpoints WHERE id=? AND harness=? AND native_session_id=?');
    return bindings.some(([id, binding]) => lookup.get(binding.endpoint_id, harness, id)?.credential_hash === digest(binding.credential));
  } finally { db.close(); }
}

export async function createServiceStarter({ harness, bindingFile, serviceUrl, bindings, logger }) {
  if (!bindingFile) return null;
  const settingsFile = join(dirname(bindingFile), 'service-start.json');
  const options = await optionalJson(settingsFile);
  if (options === undefined || options === false) return null;
  object(options, 'bridge startup settings');
  keys(options, ['node_path', 'service_directory', 'trial_directory'], 'bridge startup settings');
  const legacy = options.trial_directory !== undefined;
  const directoryKey = legacy ? 'trial_directory' : 'service_directory';
  insist(!legacy || options.service_directory === undefined, 'INVALID_INPUT', 'Choose one bridge service directory');
  for (const key of ['node_path', directoryKey]) {
    insist(typeof options[key] === 'string' && isAbsolute(options[key]), 'INVALID_INPUT', 'Bridge service startup requires absolute paths');
  }
  const node = await realpath(options.node_path), directory = await realpath(options[directoryKey]);
  const runtime = dirname(directory);
  const entry = legacy ? join(dirname(runtime), 'scripts', 'daily-use.mjs') : bundledEntry;
  insist(/^node(?:\.exe)?$/i.test(basename(node)) && (await stat(node)).isFile(), 'INVALID_INPUT', 'Bridge service startup requires a local Node executable');
  insist((!legacy || basename(runtime) === '.runtime' && /^daily-use-\d+$/.test(basename(directory))) && (await stat(entry)).isFile(),
    'INVALID_INPUT', 'Bridge service startup entry is unavailable');

  async function verify() {
    const [config, current, settings] = await Promise.all([
      readSharedServiceConfig(directory), json(bindingFile), optionalJson(settingsFile),
    ]);
    insist(serviceUrl === 'http://127.0.0.1:' + config.port && new URL(current.service_url).origin === serviceUrl
      && settings?.node_path === options.node_path && settings?.[directoryKey] === options[directoryKey]
      && settings?.[legacy ? 'service_directory' : 'trial_directory'] === undefined
      && samePath(await realpath(settings[directoryKey]), directory),
    'CONFIG_CONFLICT', 'Bridge service address or startup settings changed');
    const usable = [...bindings].filter(([id, binding]) => {
      const saved = current.bindings?.[id];
      return saved?.endpoint_id === binding.endpoint_id && saved?.credential === binding.credential
        && typeof binding.endpoint_id === 'string' && typeof binding.credential === 'string';
    });
    const matches = endpoint => endpoint?.harness === harness && usable.some(([id, binding]) =>
      endpoint.native_session_id === id && endpoint.id === binding.endpoint_id && endpoint.credential === binding.credential);
    if (legacy) {
      const endpoints = await json(join(directory, 'private-endpoints.json'));
      insist(Array.isArray(endpoints) && endpoints.some(matches),
        'CONFIG_CONFLICT', 'The prepared endpoint differs from the local binding');
    } else {
      insist(usable.length && ((config.endpoints ?? []).some(matches) || await matchesLedger(config, harness, usable)),
        'CONFIG_CONFLICT', 'No local binding matches the shared service');
    }
  }
  await verify();

  let child, starting = false, closed = false, nextAttempt = 0, failures = 0;
  function retry(code) {
    nextAttempt = Date.now() + Math.min(60_000, 5000 * 2 ** Math.min(failures++, 4));
    if (!closed) logger?.warn?.('Bridge supervisor could not stay running', { code });
  }
  async function start() {
    if (isDailyUseAutoStartHeld(await optionalJson(join(directory, 'control.json')))) {
      nextAttempt = Date.now() + 5000;
      return;
    }
    await verify();
    if (closed) return;
    const worker = spawn(node, [entry, 'supervise', directory, '--autostart'], {
      cwd: legacy ? dirname(runtime) : directory, detached: true, windowsHide: true, shell: false, stdio: 'ignore',
    });
    child = worker;
    worker.once('spawn', () => { if (!closed) logger?.info?.('Started bridge supervisor', { pid: worker.pid }); });
    worker.once('error', error => {
      if (child !== worker) return;
      child = null;
      retry(error.code ?? 'SPAWN_FAILED');
    });
    worker.once('exit', code => {
      if (child !== worker) return;
      child = null;
      if (code === 0) nextAttempt = Date.now() + 5000;
      else retry('SUPERVISOR_EXIT');
    });
    worker.unref();
  }
  return {
    onRefused() {
      if (closed || starting || child || Date.now() < nextAttempt) return;
      starting = true;
      void start().catch(error => retry(error.code ?? 'STARTUP_FAILED')).finally(() => { starting = false; });
    },
    onConnected() { failures = 0; nextAttempt = 0; },
    close() { closed = true; },
  };
}
