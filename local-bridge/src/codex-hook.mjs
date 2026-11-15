import { fileURLToPath } from 'node:url';
import { join } from 'node:path';
import { loadCodexConfig, hookBinding } from './codex-config.mjs';
import { CODEX_TOOL_PREFIX } from './adapters/codex.mjs';
import { BridgeClient } from './client.mjs';
import { object, insist } from './protocol.mjs';
import { recordHookDiagnostic, recordHookStartup } from './codex-hook-diagnostics.mjs';
import { readHookInput } from './codex-hook-input.mjs';
import { isMainModule } from './entrypoint.mjs';
import { encodeApprovalProof } from './codex-approval.mjs';
import { createHistorySync, historySyncPrompt, historySyncPython } from './history-sync-host.mjs';

const isBridgeTool = event => event.hook_event_name === 'PreToolUse' && typeof event.tool_name === 'string' && event.tool_name.startsWith(CODEX_TOOL_PREFIX);
const deny = reason => ({ hookSpecificOutput: { hookEventName: 'PreToolUse', permissionDecision: 'deny', permissionDecisionReason: reason } });
const isContextTool = event => event.tool_name === CODEX_TOOL_PREFIX + 'bridge_context';

function unboundReason(event, pluginData) {
  if (!isContextTool(event)) return 'local-bridge尚未绑定此会话；请先完成插件设置。';
  const metadata = {
    session_id: typeof event.session_id === 'string' ? event.session_id : null,
    cwd: typeof event.cwd === 'string' ? event.cwd : null,
    transcript_path: typeof event.transcript_path === 'string' ? event.transcript_path : null,
    plugin_data: typeof pluginData === 'string' ? pluginData : null,
  };
  const missing = Object.entries(metadata).filter(([, value]) => !value).map(([key]) => key);
  const hint = missing.includes('transcript_path')
    ? '此会话没有可用 transcript_path，当前绑定流程不支持；请改选提供绝对 transcript 路径的会话。'
    : missing.length ? '请在插件 hook 环境中确认缺失的宿主元数据后再设置。'
      : '请将以下原生 hook 元数据复制到本机设置预览命令；本提示不会登记端点或保存凭据。';
  return `local-bridge尚未绑定。${hint} metadata=${JSON.stringify(metadata)}`;
}

/** This receives stdin from the native hook, never a model-provided session-id argument. */
export async function runCodexHook(event, { config, client, env = process.env, historySync } = {}) {
  object(event, 'hook input');
  const tool = isBridgeTool(event);
  if (event.hook_event_name === 'PreToolUse' && !tool) return {};
  try {
    const binding = hookBinding(config, event);
    if (!binding) return tool ? deny(unboundReason(event, env.PLUGIN_DATA)) : {};
    const service = client ?? new BridgeClient(config.service_url ?? 'http://127.0.0.1:4776', { timeoutMs: 2000 });
    const payload = {};
    for (const key of ['session_id', 'cwd', 'transcript_path', 'hook_event_name', 'turn_id', 'tool_name', 'tool_use_id', 'tool_input', 'agent_id']) if (event[key] !== undefined) payload[key] = event[key];
    if (event.hook_event_name === 'UserPromptSubmit') {
      const sync = historySync ?? createHistorySync({
        python: await historySyncPython(env.LOCAL_BRIDGE_CONFIG ?? (env.PLUGIN_DATA ? join(env.PLUGIN_DATA, 'bridge.json') : null)),
      });
      if (await sync.autoEnabled()) payload.history_sync = true;
    }
    const result = await service.request('/api/codex/host-event', { method: 'POST', credential: binding.credential, body: { endpoint_id: binding.endpoint_id, event: payload } });
    if (tool) {
      insist(result.approval, 'CAPABILITY_MISSING', 'Service requires native approval-proof support');
      return { hookSpecificOutput: { hookEventName: 'PreToolUse', permissionDecision: 'allow', updatedInput: { ...event.tool_input, _bridge_ticket: encodeApprovalProof(result.approval) } } };
    }
    if (event.hook_event_name === 'UserPromptSubmit') {
      const additionalContext = [payload.history_sync ? historySyncPrompt(result.history_sources) : null, result.notice].filter(Boolean).join('\n');
      if (additionalContext) return { hookSpecificOutput: { hookEventName: 'UserPromptSubmit', additionalContext } };
    }
    return {};
  } catch (error) {
    const safeCodes = new Set(['CALLER_MISMATCH', 'INVALID_INPUT', 'CAPABILITY_MISSING', 'HOST_CONTEXT_UNVERIFIED', 'UNAUTHORIZED', 'STALE_CONNECTION', 'HOST_BUSY', 'CALL_CONFLICT', 'TICKET_USED', 'ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'EPERM', 'EACCES']);
    const code = [error?.code, error?.cause?.code].find(value => safeCodes.has(value));
    return tool ? deny('local-bridge无法验证本次会话调用或连接本地服务，未执行工具。' + (code ? ' [' + code + ']' : '')) : {};
  }
}

async function main() {
  const event = await readHookInput(process.stdin);
  // Diagnostics cannot authorize or block a call, even if the opt-in file is invalid.
  await recordHookDiagnostic(event).catch(() => {});
  let config;
  try { config = await loadCodexConfig(); } catch { config = null; }
  const output = await runCodexHook(event, { config }); process.stdout.write(JSON.stringify(output) + '\n');
}
await recordHookStartup({ pid: process.pid, entry_path: process.argv[1], module_path: fileURLToPath(import.meta.url), cwd: process.cwd(), plugin_data: process.env.PLUGIN_DATA }).catch(() => {});
if (isMainModule(import.meta.url)) main().catch(() => {
  // Malformed hook input never receives an allow response or leaks credentials.
  process.stderr.write('local-bridge hook input could not be processed.\n'); process.exitCode = 1;
});
