import { fileURLToPath } from 'node:url';
import { join, resolve } from 'node:path';
import { BridgeClient } from './client.mjs';
import { loadCodexConfig } from './codex-config.mjs';
import { sessionToolDefinitions } from './session-tools.mjs';
import { McpToolSession, serveMcpStdio } from './mcp.mjs';
import { object, insist } from './protocol.mjs';
import { decodeApprovalProof, isLegacyTicket } from './codex-approval.mjs';
import { createHistorySync, historySyncPython } from './history-sync-host.mjs';

export class CodexMcpProxy {
  constructor({ client, heartbeatClient = client, historyClient = client, config = null, getConfig = async () => config, historySync = createHistorySync() }) {
    this.client = client; this.heartbeatClient = heartbeatClient; this.getConfig = getConfig; this.liveness = new Map();
    this.historySync = historySync;
    this.historyClient = historyClient;
  }
  definitions() {
    return sessionToolDefinitions({ historySync: true }).map(tool => ({ ...tool, inputSchema: { ...tool.inputSchema, properties: { ...tool.inputSchema.properties,
      _bridge_ticket: { type: 'string', description: 'Injected by the native PreToolUse hook. Omit from model arguments.' },
    } } }));
  }
  async execute(name, args) {
    object(args, 'arguments'); const { _bridge_ticket: supplied, ...clean } = args;
    let ticket = supplied;
    let scope;
    if (!isLegacyTicket(supplied)) {
      const proof = decodeApprovalProof(supplied);
      scope = proof.endpoint_id;
      const config = await this.getConfig();
      if (config?.service_url && typeof this.client.base === 'string') insist(new URL(config.service_url).origin === this.client.base,
        'CONFIG_CONFLICT', 'Bridge service changed; reload the MCP connection before using private credentials');
      const bindings = config?.bindings;
      const binding = bindings && Object.hasOwn(bindings, proof.native_session_id) ? bindings[proof.native_session_id] : null;
      insist(binding?.endpoint_id === proof.endpoint_id && typeof binding.credential === 'string',
        'HOST_CONTEXT_UNVERIFIED', 'Native approval proof has no matching private endpoint binding');
      const exchange = await this.client.request('/api/codex/exchange', { method: 'POST', credential: binding.credential,
        body: { ...proof, name, arguments: clean } });
      insist(isLegacyTicket(exchange.ticket), 'INVALID_RESPONSE', 'Invalid execution ticket'); ticket = exchange.ticket;
    }
    if (name === 'bridge_sync_history') {
      insist(scope, 'HOST_CONTEXT_UNVERIFIED', 'History sync requires a native approval proof');
      return this.historySync.execute(clean, {
        scope,
        call: async (tool, parameters) => this._transport(await this.historyClient.request('/api/codex/history-sync', {
          method: 'POST', credential: ticket, body: { arguments: clean, tool, parameters },
        })),
      });
    }
    return this._transport(await this.client.request('/api/codex/tool-call', { method: 'POST', credential: ticket, body: { name, arguments: clean } }));
  }
  _transport(result) {
    insist(result.transport === 'local-bridge.codex-tool-result/1' && typeof result.liveness === 'string' && typeof result.endpoint_id === 'string', 'INVALID_RESPONSE', 'Invalid Codex gateway response');
    this.liveness.set(result.endpoint_id, result.liveness);
    return result.value; // Keep the liveness capability outside model-visible tool output.
  }
  async sendHeartbeats() {
    return Promise.allSettled([...this.liveness].map(async ([endpoint, credential]) => {
      try { await this.heartbeatClient.request('/api/codex/heartbeat', { method: 'POST', credential, body: {} }); }
      catch (error) { if (['UNAUTHORIZED', 'STALE_CONNECTION'].includes(error.code)) this.liveness.delete(endpoint); throw error; }
    }));
  }
}
async function main() {
  const config = await loadCodexConfig();
  const base = process.env.LOCAL_BRIDGE_URL ?? config?.service_url ?? 'http://127.0.0.1:4776';
  const localClient = new BridgeClient(base, { timeoutMs: 2000 });
  const proxy = new CodexMcpProxy({ client: new BridgeClient(base), heartbeatClient: localClient, historyClient: localClient,
    historySync: createHistorySync({
      python: await historySyncPython(process.env.LOCAL_BRIDGE_CONFIG ?? (process.env.PLUGIN_DATA ? join(process.env.PLUGIN_DATA, 'bridge.json') : null)),
    }),
    getConfig: async () => {
      const current = await loadCodexConfig();
      return current ? { ...current, service_url: process.env.LOCAL_BRIDGE_URL ?? current.service_url ?? base } : null;
    },
  });
  let sending = null;
  const timer = setInterval(() => { if (!sending) sending = proxy.sendHeartbeats().finally(() => { sending = null; }); }, 10_000); timer.unref();
  try { await serveMcpStdio({ input: process.stdin, output: process.stdout, session: new McpToolSession({ bridge: proxy }) }); }
  finally { clearInterval(timer); await sending; }
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) main().catch(() => { process.stderr.write('local-bridge MCP could not start.\n'); process.exitCode = 1; });
