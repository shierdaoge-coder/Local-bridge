import { readFile, writeFile, mkdir, rename, unlink } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { join } from 'node:path';

async function enabled(directory, now) {
  let config; try { config = JSON.parse(await readFile(join(directory, 'hook-diagnostics-config.json'), 'utf8')); } catch { return null; }
  const expires = Date.parse(config.expires_at);
  return Number.isFinite(expires) && expires > now() && expires <= now() + 60 * 60 * 1000 && Array.isArray(config.session_ids) ? config : null;
}

/** Proves the executable reached module initialization, independently of stdin/entry guards. */
export async function recordHookStartup(metadata, { directory = fileURLToPath(new URL('../PLUGIN_DATA/', import.meta.url)), now = Date.now } = {}) {
  if (!await enabled(directory, now)) return;
  await mkdir(directory, { recursive: true });
  await writeFile(join(directory, 'hook-diagnostic-startup.json'), JSON.stringify({ recorded_at: new Date(now()).toISOString(), pid: metadata.pid, entry_path: metadata.entry_path, module_path: metadata.module_path, cwd: metadata.cwd, plugin_data: metadata.plugin_data ?? null }, null, 2) + '\n', { mode: 0o600 });
}

/** Short-lived operator opt-in. Never records prompts, tool bodies, credentials or results. */
export async function recordHookDiagnostic(event, { directory = fileURLToPath(new URL('../PLUGIN_DATA/', import.meta.url)), env = process.env, now = Date.now } = {}) {
  const config = await enabled(directory, now); if (!config) return;
  if (typeof event.session_id === 'string' && !config.session_ids.includes(event.session_id)) return;
  const take = key => typeof event[key] === 'string' ? event[key].slice(0, 2048) : null;
  const value = { recorded_at: new Date(now()).toISOString(), hook_event_name: take('hook_event_name'), session_id: take('session_id'), cwd: take('cwd'), transcript_path: take('transcript_path'), tool_name: take('tool_name'), tool_use_id: take('tool_use_id'), agent_id: take('agent_id'),
    plugin_data: typeof env.PLUGIN_DATA === 'string' ? env.PLUGIN_DATA : null, plugin_root: typeof env.PLUGIN_ROOT === 'string' ? env.PLUGIN_ROOT : null,
    top_level_keys: Object.keys(event), tool_input_kind: typeof event.tool_input };
  // Each event kind gets one latest record, keeping both lifecycle and tool evidence bounded.
  const kind = ['SessionStart', 'UserPromptSubmit', 'PreToolUse', 'Stop', 'Interrupt', 'SessionEnd'].includes(event.hook_event_name) ? event.hook_event_name : 'unknown';
  await mkdir(directory, { recursive: true });
  const target = join(directory, 'hook-diagnostic-' + kind + '.json'), temp = target + '.' + randomUUID() + '.tmp';
  try { await writeFile(temp, JSON.stringify(value, null, 2) + '\n', { flag: 'wx', mode: 0o600 }); await rename(temp, target); }
  finally { await unlink(temp).catch(() => {}); }
}
