import { spawn } from 'node:child_process';
import { readFile, realpath, stat } from 'node:fs/promises';
import { basename, dirname, isAbsolute, join } from 'node:path';
import { insist, keys, object } from './protocol.mjs';

const json = async path => JSON.parse((await readFile(path, 'utf8')).replace(/^\uFEFF/, ''));
const optionalJson = async path => {
  try { return await json(path); }
  catch (error) { if (error.code !== 'ENOENT') throw error; }
};
const samePath = (a, b) => process.platform === 'win32' ? a.toLowerCase() === b.toLowerCase() : a === b;

export function isDailyUseAutoStartHeld(control) {
  return control?.hold_autostart === true || control?.action === 'stop';
}

export async function createDshServiceStarter({ bindingFile, serviceUrl, bindings, logger }) {
  const settingsFile = join(dirname(bindingFile), 'service-start.json');
  const options = await optionalJson(settingsFile);
  if (options === undefined || options === false) return null;
  object(options, 'DSH startup settings');
  keys(options, ['node_path', 'trial_directory'], 'DSH startup settings');
  for (const key of ['node_path', 'trial_directory']) {
    insist(typeof options[key] === 'string' && isAbsolute(options[key]), 'INVALID_INPUT', 'DSH service startup requires absolute paths');
  }
  const node = await realpath(options.node_path), trial = await realpath(options.trial_directory);
  const runtime = dirname(trial), project = dirname(runtime);
  const entry = join(project, 'scripts', 'daily-use.mjs');
  insist(/^node(?:\.exe)?$/i.test(basename(node)) && (await stat(node)).isFile(), 'INVALID_INPUT', 'DSH service startup requires a local Node executable');
  insist(basename(runtime) === '.runtime' && /^daily-use-\d+$/.test(basename(trial)) && (await stat(entry)).isFile(),
    'INVALID_INPUT', 'DSH service startup requires a prepared daily-use directory');

  async function verifyTrial() {
    const [config, endpoints, current, settings] = await Promise.all([
      json(join(trial, 'config.json')), json(join(trial, 'private-endpoints.json')),
      json(bindingFile), optionalJson(settingsFile),
    ]);
    const { port } = config;
    insist(Number.isInteger(port) && port > 0 && port <= 65535 && port !== 4776 && config.nativeAdapters === true
      && serviceUrl === new URL('http://127.0.0.1:' + port).origin
      && typeof config.directory === 'string' && isAbsolute(config.directory)
      && samePath(await realpath(config.directory), join(trial, 'data')),
    'CONFIG_CONFLICT', 'The selected daily-use service differs from the DSH connection');
    const matchingEndpoint = Array.isArray(endpoints) && endpoints.some(endpoint => {
      if (endpoint.harness !== 'dsh') return false;
      const binding = bindings.get(endpoint.native_session_id);
      const currentBinding = current.bindings?.[endpoint.native_session_id];
      return binding?.endpoint_id === endpoint.id && currentBinding?.endpoint_id === endpoint.id
        && binding.credential === endpoint.credential && currentBinding.credential === endpoint.credential;
    });
    insist(matchingEndpoint && new URL(current.service_url).origin === serviceUrl
      && settings?.node_path === options.node_path && settings?.trial_directory === options.trial_directory,
    'CONFIG_CONFLICT', 'The prepared DSH endpoint or private startup settings changed');
  }
  await verifyTrial();

  let child, starting = false, closed = false, nextAttempt = 0, failures = 0;
  function retry(code) {
    nextAttempt = Date.now() + Math.min(60_000, 5000 * 2 ** Math.min(failures++, 4));
    if (!closed) logger?.warn?.('DSH bridge supervisor could not stay running', { code });
  }
  async function start() {
    const control = await optionalJson(join(trial, 'control.json'));
    if (isDailyUseAutoStartHeld(control)) {
      nextAttempt = Date.now() + 5000;
      return;
    }
    await verifyTrial();
    if (closed) return;
    const worker = spawn(node, [entry, 'supervise', trial, '--autostart'], {
      cwd: project, detached: true, windowsHide: true, shell: false, stdio: 'ignore',
    });
    child = worker;
    worker.once('spawn', () => { if (!closed) logger?.info?.('DSH started the bridge supervisor', { pid: worker.pid }); });
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
