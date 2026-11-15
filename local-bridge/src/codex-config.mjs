import { readFile } from 'node:fs/promises';
import { join, resolve, isAbsolute } from 'node:path';
import { BridgeClient } from './client.mjs';
import { object, insist, text } from './protocol.mjs';

export async function loadCodexConfig(env = process.env) {
  const path = env.LOCAL_BRIDGE_CONFIG ?? (env.PLUGIN_DATA ? join(env.PLUGIN_DATA, 'bridge.json') : null);
  if (!path) return null;
  insist(isAbsolute(path), 'INVALID_INPUT', 'Bridge config path must be absolute');
  let config;
  try { config = JSON.parse(await readFile(path, 'utf8')); }
  catch (error) { if (error.code === 'ENOENT') return null; throw error; }
  object(config, 'config'); new BridgeClient(config.service_url ?? 'http://127.0.0.1:4776');
  if (config.bindings !== undefined) object(config.bindings, 'bindings');
  return config;
}

// Native Windows hooks may spell ordinary filesystem paths with a \\?\ prefix.
// Remove only the documented drive/UNC namespace spelling; keep the path,
// session, and subagent checks otherwise exact.
function filesystemPath(value) {
  let candidate = resolve(value);
  if (process.platform === 'win32' && candidate.startsWith('\\\\?\\')) {
    const rest = candidate.slice(4);
    if (/^[A-Za-z]:[\\/]/.test(rest)) candidate = rest;
    else if (rest.slice(0, 4).toUpperCase() === 'UNC\\') candidate = '\\\\' + rest.slice(4);
  }
  return resolve(candidate);
}

export function hookBinding(config, event) {
  if (!config?.bindings || !Object.hasOwn(config.bindings, event.session_id)) return null;
  const binding = object(config.bindings[event.session_id], 'binding');
  text(binding.endpoint_id, 'endpoint_id', 256); text(binding.credential, 'credential', 512);
  for (const key of ['cwd', 'transcript_path']) {
    insist(typeof binding[key] === 'string' && isAbsolute(binding[key]), 'INVALID_INPUT', 'Binding requires an absolute ' + key);
    insist(typeof event[key] === 'string' && isAbsolute(event[key]) && filesystemPath(event[key]) === filesystemPath(binding[key]), 'CALLER_MISMATCH', 'Native ' + key + ' differs from the operator binding');
  }
  insist(!event.agent_id, 'CALLER_MISMATCH', 'Subagents cannot use a parent session binding');
  return binding;
}
