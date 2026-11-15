import fs from 'node:fs/promises';
import { createRequire } from 'node:module';
import { randomUUID } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { dirname, isAbsolute, join } from 'node:path';
import { BridgeError, insist, keys, object, text } from './protocol.mjs';
import { BridgeClient } from './client.mjs';
import { DshDeliveryAdapter, dshHostContext, dshNotificationMessage } from './adapters/dsh.mjs';
import { sessionToolDefinitions } from './session-tools.mjs';
import { DshOutbox, dshOutboxScope, dshToolError } from './dsh-outbox.mjs';
import { createDshServiceStarter } from './dsh-service-start.mjs';
import { createHistorySync, historySyncPrompt, historySyncPython } from './history-sync-host.mjs';

export const name = 'local-bridge';
export const inject = ['tools', 'agents'];

async function nativeDependencies(config) {
  if (typeof config.loadNative === 'function') return config.loadNative();
  const entry = config.sdk_entry;
  if (typeof entry !== 'string' || !isAbsolute(entry)) throw new Error('DSH sdk_entry must point to the absolute installed dsh-session-persistence entry');
  const anchor = pathToFileURL(entry);
  const requireFromRuntime = createRequire(anchor);
  const load = async specifier => import(pathToFileURL(requireFromRuntime.resolve(specifier)).href);
  return {
    ...await import(anchor.href),
    ...await load('@deepseek-ai/dsh-session'),
    ...await load('@deepseek-ai/dsh-llm'),
  };
}

async function readBindings(file) {
  try {
    const config = JSON.parse(await fs.readFile(file, 'utf8'));
    object(config, 'binding file');
    object(config.bindings ?? {}, 'bindings');
    config.bindings ??= {};
    text(config.service_url, 'service_url', 2048);
    return config;
  } catch (error) {
    if (error.code === 'ENOENT') return { service_url: 'http://127.0.0.1:4776', bindings: {} };
    throw error;
  }
}

function toolDefinitions(bridges, configPath, settingsUrl) {
  return sessionToolDefinitions({ historySync: true }).map(({ name, description, inputSchema }) => ({
    name, parameters: inputSchema,
    description: description + (name === 'bridge_publish' ? ' A pending_local receipt means the DSH outbox stored it for retry. Check bridge_status for remote delivery.'
      : name === 'bridge_status' ? ' DSH also reports pending_local and local_failed for its own persisted outbound messages.' : ''),
    output: { schema: { type: 'string' }, render: (_args, value) => [{ type: 'text', text: value }] },
    async execute(args, exec) {
      try {
        const context = dshHostContext(exec);
        insist(context, 'HOST_CONTEXT_UNVERIFIED', 'Actual DSH execution context required');
        const bridge = bridges.get(context.native_session_id);
        if (!bridge) {
          if (name === 'bridge_context') return JSON.stringify({ ok: false, error: 'NOT_REGISTERED', native_session_id: context.native_session_id, binding_file: configPath, settings_url: settingsUrl });
          throw new BridgeError('NOT_REGISTERED', 'This native session has no operator-bound transport endpoint');
        }
        exec?.signal?.throwIfAborted?.();
        return JSON.stringify({ ok: true, result: await bridge.execute(name, args, exec) });
      } catch (error) {
        if (exec?.signal?.aborted) throw error;
        const failure = dshToolError(error);
        return JSON.stringify({ ok: false, error: failure.code, message: failure.message });
      }
    },
  }));
}

export async function apply(ctx, config = {}) {
  const bindingFile = config.binding_file;
  if (typeof bindingFile !== 'string' || !isAbsolute(bindingFile)) throw new Error('DSH binding_file must be an absolute private config path');
  const saved = await readBindings(bindingFile);
  const serviceUrl = saved.service_url;
  const bindings = new Map();
  for (const [sessionId, binding] of Object.entries(saved.bindings)) {
    text(sessionId, 'binding session id', 256); object(binding, `binding ${sessionId}`);
    text(binding.endpoint_id, 'endpoint_id', 256); text(binding.credential, 'credential', 4096);
    bindings.set(sessionId, { endpoint_id: binding.endpoint_id, credential: binding.credential });
  }

  const native = await nativeDependencies(config);
  insist(typeof native.createUserMessage === 'function' && typeof native.adoptSessionEvent === 'function', 'INVALID_NATIVE_SDK', 'Installed DSH message factory or event validator is unavailable');
  const adapter = new DshDeliveryAdapter({ agents: ctx.agents, createUserMessage: native.createUserMessage, adoptSessionEvent: native.adoptSessionEvent });
  const client = new BridgeClient(serviceUrl, { timeoutMs: 2000 });
  const historySync = config.historySync ?? createHistorySync({
    python: await historySyncPython(bindingFile),
    onUnavailable: () => ctx.logger?.warn?.('DSH Python history sync unavailable. Manual receive is available.'),
  });
  let serviceStarter;
  try { serviceStarter = await createDshServiceStarter({ bindingFile, serviceUrl: client.base, bindings, logger: ctx.logger }); }
  catch (error) { ctx.logger?.warn?.('DSH service startup unavailable', { code: dshToolError(error).code }); }
  let outbox;
  if (bindings.size) {
    try { outbox = await DshOutbox.open(join(dirname(bindingFile), 'bridge-outbox.sqlite')); }
    catch (error) { ctx.logger?.warn?.('DSH outbox unavailable', { code: dshToolError(error).code }); }
  }
  const scopes = new Map([...bindings].map(([id, binding]) => [id, dshOutboxScope(client.base, id, binding)]));
  const hostInstance = randomUUID();
  const disposers = [];
  let closed = false, pumping = false;
  const syncOffered = new WeakMap();
  const attached = new WeakSet();
  const online = new Set(), active = new Set();
  const tracked = task => { const work = Promise.resolve().then(task); active.add(work); work.finally(() => active.delete(work)).catch(() => {}); return work; };
  const requestTool = (sessionId, binding, name, args) => client.request('/api/dsh/tool-call', { method: 'POST', credential: binding.credential, body: {
    endpoint_id: binding.endpoint_id, native_session_id: sessionId, host_instance: hostInstance, name, arguments: args,
  } });
  const connectionErrors = new Map();
  const noteConnectionError = (sessionId, error) => {
    const { code } = dshToolError(error);
    if (!closed && code === 'ECONNREFUSED' && error?.status === undefined) serviceStarter?.onRefused();
    if (connectionErrors.get(sessionId) === code) return;
    connectionErrors.set(sessionId, code);
    ctx.logger?.warn?.('DSH bridge background request failed', { code });
  };

  const pulse = async (sessionId, agent, naturalSlot) => {
    if (closed) return;
    const binding = bindings.get(sessionId);
    if (!binding || !agent || agent.session?.id !== sessionId) return;
    const state = agent.status === 'idle' ? 'idle' : 'busy';
    online.add(sessionId);
    const autoSync = Boolean(naturalSlot?.autoSync);
    const requestPulse = history_sync => client.request('/api/dsh/pulse', {
      method: 'POST', credential: binding.credential,
      body: { endpoint_id: binding.endpoint_id, native_session_id: sessionId, host_instance: hostInstance, state,
        ...(naturalSlot ? { natural_turn: true } : {}), ...(history_sync ? { history_sync: true } : {}) },
    });
    let response;
    try { response = await requestPulse(autoSync); }
    catch (error) {
      if (!autoSync || error.code !== 'INVALID_INPUT') throw error;
      response = await requestPulse(false);
    }
    const prompt = autoSync && historySyncPrompt(response.history_sources);
    if (prompt && !closed && !naturalSlot.signal?.aborted && agent.status === 'running') {
      naturalSlot.messages.push(dshNotificationMessage(native, {
        endpoint: { id: binding.endpoint_id }, batch_id: randomUUID(), sync_prompt: prompt,
      }));
    }
    if (response.command) {
      const command = response.command;
      let result;
      try {
        insist(!closed && command.endpoint?.id === binding.endpoint_id && command.endpoint?.native_session_id === sessionId && command.endpoint?.harness === 'dsh', 'CALLER_MISMATCH', 'Delivery command differs from the bound native caller');
        insist(Number.isFinite(command.expires_at) && command.expires_at > Date.now(), 'COMMAND_EXPIRED', 'Delivery command expired before native invocation');
        const wake = command.wake === true;
        if (!wake) {
          // agent/request is too late: injecting there forces a second model step.
          // The supported pre-step waterfall admits this native message in the existing step.
          if (!naturalSlot || agent.status !== 'running' || naturalSlot.signal?.aborted) result = { accepted: false, error: 'No current native pre-step output slot; notification was not injected' };
          else {
            const message = dshNotificationMessage(native, command);
            naturalSlot.messages.push(message);
            result = { accepted: true, native_ref: 'dsh-pre-step:' + message.id };
          }
        } else result = await adapter.offer({ ...command, wake, signal: undefined });
      } catch (error) { result = { error: error.message || 'Native delivery failed; acceptance may be unknown' }; }
      await client.request('/api/dsh/host-result', { method: 'POST', credential: binding.credential, body: {
        endpoint_id: binding.endpoint_id, native_session_id: sessionId, host_instance: hostInstance, command_id: command.command_id,
        ...(result.accepted === true ? { accepted: true, native_ref: result.native_ref } : result.accepted === false ? { accepted: false, error: result.error } : { error: result.error || 'Native acceptance is unknown' }),
      } });
    }
  };

  const offlinePulse = async (sessionId, binding) => client.request('/api/dsh/pulse', { method: 'POST', credential: binding.credential, body: {
    endpoint_id: binding.endpoint_id, native_session_id: sessionId, host_instance: hostInstance, state: 'offline',
  } });

  const tick = async () => {
    if (closed || pumping) return;
    pumping = true;
    try {
      for (const sessionId of bindings.keys()) {
        if (closed) break;
        const binding = bindings.get(sessionId);
        const agent = ctx.agents.get(sessionId);
        if (!agent || agent.session?.id !== sessionId) {
          try { await offlinePulse(sessionId, binding); serviceStarter?.onConnected(); connectionErrors.delete(sessionId); }
          catch (error) { noteConnectionError(sessionId, error); }
          continue;
        }
        if (!attached.has(agent) && typeof agent.ctx?.on === 'function') {
          attached.add(agent);
          const dispose = agent.ctx.on('agent/pre-step', async (payload, next) => {
            const decision = await next();
            if (closed || decision.kind === 'reject' || payload.signal?.aborted) return decision;
            const firstStep = payload.step === 1 && payload.messages?.some(message => message.role === 'user' && message.source?.kind === 'user');
            const autoSync = firstStep && (!payload.signal || syncOffered.get(agent) !== payload.signal) && await historySync.autoEnabled();
            if (closed || payload.signal?.aborted) return decision;
            if (autoSync && payload.signal) syncOffered.set(agent, payload.signal);
            const slot = { messages: [], signal: payload.signal, autoSync };
            try { await tracked(() => pulse(sessionId, agent, slot)); } catch { ctx.logger?.warn?.('DSH natural-turn pulse failed'); }
            return slot.messages.length ? { ...decision, messages: [...decision.messages, ...slot.messages] } : decision;
          });
          if (typeof dispose === 'function') disposers.push(dispose);
        }
        try {
          await pulse(sessionId, agent);
          serviceStarter?.onConnected();
          if (!closed && outbox) await outbox.flush(scopes.get(sessionId), args => requestTool(sessionId, binding, 'bridge_publish', args));
          connectionErrors.delete(sessionId);
        } catch (error) { noteConnectionError(sessionId, error); }
      }
    } finally { pumping = false; }
  };

  const bridgeMap = new Map();
  for (const [sessionId, binding] of bindings) bridgeMap.set(sessionId, {
    execute: (name, args, execution) => tracked(() => {
      insist(!closed, 'HOST_CLOSED', 'DSH plugin was unloaded');
      const scope = scopes.get(sessionId);
      if (name === 'bridge_sync_history') return historySync.execute(args, {
        scope, signal: execution?.signal, call: (tool, parameters) => {
          insist(!closed, 'HOST_CLOSED', 'DSH plugin was unloaded');
          execution?.signal?.throwIfAborted();
          return requestTool(sessionId, binding, tool, parameters);
        },
      });
      if (name === 'bridge_publish') {
        insist(outbox, 'OUTBOX_UNAVAILABLE', 'The local DSH outbox could not be opened');
        return outbox.publish(scope, args, publishArgs => requestTool(sessionId, binding, name, publishArgs));
      }
      if (name === 'bridge_status') {
        object(args, 'status arguments'); keys(args, ['id'], 'status arguments'); text(args.id, 'id', 256);
        const local = outbox?.status(scope, args.id);
        if (local) return local;
      }
      return requestTool(sessionId, binding, name, args);
    }),
  });
  const unregister = [];
  try {
    for (const tool of toolDefinitions(bridgeMap, bindingFile, serviceUrl)) unregister.push(ctx.tools.register(tool));
  } catch (error) {
    for (const dispose of unregister) { try { dispose?.(); } catch {} }
    serviceStarter?.close();
    outbox?.close();
    throw error;
  }
  const timer = setInterval(() => { void tracked(tick).catch(() => {}); }, 1000); timer.unref?.();
  void tracked(tick).catch(() => {});

  return async () => {
    if (closed) return;
    closed = true; clearInterval(timer);
    serviceStarter?.close();
    for (const dispose of unregister) { try { dispose?.(); } catch {} }
    for (const dispose of disposers) { try { dispose(); } catch {} }
    await Promise.allSettled([...active]);
    outbox?.close();
    for (const sessionId of online) {
      const binding = bindings.get(sessionId); if (!binding) continue;
      try { await client.request('/api/dsh/pulse', { method: 'POST', credential: binding.credential, body: { endpoint_id: binding.endpoint_id, native_session_id: sessionId, host_instance: hostInstance, state: 'offline' } }); } catch {}
    }
  };
}
