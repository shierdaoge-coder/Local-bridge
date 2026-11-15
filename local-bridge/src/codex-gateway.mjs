import { randomBytes } from 'node:crypto';
import { BridgeError, insist, object, keys, text, canonical, digest } from './protocol.mjs';
import { SessionToolBridge, sessionToolDefinitions } from './session-tools.mjs';
import { codexHostProof, codexHostContext, CODEX_TOOL_PREFIX } from './adapters/codex.mjs';

const TOOL_NAMES = new Set(sessionToolDefinitions({ historySync: true }).map(tool => tool.name));
const EVENTS = ['SessionStart', 'UserPromptSubmit', 'PreToolUse', 'Stop', 'Interrupt', 'SessionEnd'];
const ticketHash = token => typeof token === 'string' ? digest(token) : '';

/** Native proof survives human review; the private MCP transport redeems it for a short execution ticket. */
export class CodexGateway {
  constructor({ core, now = () => Date.now(), ticketMs = 30_000, approvalMs = 7_200_000, presenceMs = 30_000, settingsUrl = 'http://127.0.0.1:4776' }) {
    insist(Number.isSafeInteger(ticketMs) && ticketMs > 0 && ticketMs <= 60_000, 'INVALID_INPUT', 'Invalid ticket lifetime');
    insist(Number.isSafeInteger(approvalMs) && approvalMs >= ticketMs && approvalMs <= 7_200_000, 'INVALID_INPUT', 'Invalid approval proof lifetime');
    this.core = core; this.now = now; this.ticketMs = ticketMs; this.approvalMs = approvalMs; this.presenceMs = presenceMs; this.settingsUrl = settingsUrl;
    this.sessions = new Map(); this.tickets = new Map(); this.approvals = new Map(); this.calls = new Map(); this.windows = new Map(); this.liveness = new Map(); this.coordinator = null;
  }
  _clean() {
    const now = this.now();
    for (const [key, value] of this.tickets) if (value.expires <= now) this.tickets.delete(key);
    for (const [key, value] of this.approvals) if (value.approvalExpires <= now) this.approvals.delete(key);
    for (const [key, value] of this.calls) if (value.forget <= now) this.calls.delete(key);
    for (const [key, value] of this.liveness) if (value.expires <= now) this.liveness.delete(key);
  }
  _session(endpoint, credential) {
    let row = this.sessions.get(endpoint.id);
    if (!row) { row = { state: 'busy', seen: this.now() }; this.sessions.set(endpoint.id, row); }
    if (row.connection) {
      try { this.core.heartbeat(row.connection); }
      catch (error) { if (error.code !== 'STALE_CONNECTION') throw error; row.connection = null; }
    }
    if (!row.connection) row.connection = this.core.connect(endpoint.id, credential);
    if (!row.bridge) {
      const client = {
        get connection() { return row.connection; },
        set connection(value) { row.connection = value; },
        connect: async () => { throw new BridgeError('STALE_CONNECTION', 'Only a fresh native hook can reconnect this endpoint'); },
        sessionInfo: async () => this.core.sessionInfo(row.connection),
        receive: async args => this.core.receive(row.connection, args),
        acknowledge: async args => this.core.acknowledge(row.connection, args),
        publish: async args => this.core.publish(row.connection, args),
        status: async id => this.core.status(row.connection, id),
        request: async path => {
          const url = new URL(path, 'http://127.0.0.1');
          insist(url.pathname === '/api/cursor', 'INVALID_INPUT', 'Unsupported core query');
          const archive = url.searchParams.get('archive_id'), generation = url.searchParams.get('generation'), requestId = url.searchParams.get('request_id');
          return requestId === null ? { read_through: this.core.historyCursor(row.connection, archive, generation) }
            : this.core.historySyncStatus(row.connection, archive, generation, requestId);
        },
      };
      row.bridge = new SessionToolBridge({ client, endpointId: endpoint.id, credential, binding: endpoint, getHostContext: codexHostContext });
    }
    return row;
  }
  async hostEvent(credential, input) {
    object(input, 'host event'); keys(input, ['endpoint_id', 'event'], 'host event'); text(input.endpoint_id, 'endpoint_id', 256);
    const event = object(input.event, 'native event'), endpoint = this.core.authenticateEndpoint(input.endpoint_id, credential);
    insist(endpoint.harness === 'codex' && event.session_id === endpoint.native_session_id, 'CALLER_MISMATCH', 'Hook does not match its registered Codex session');
    insist(EVENTS.includes(event.hook_event_name), 'INVALID_INPUT', 'Unsupported native event');
    insist(!event.agent_id, 'CALLER_MISMATCH', 'Subagent hooks cannot act as the parent endpoint');
    const type = event.hook_event_name;
    if (['SessionStart', 'UserPromptSubmit', 'Stop', 'SessionEnd', 'Interrupt'].includes(type)) {
      for (const entry of this.approvals.values()) if (entry.endpoint === endpoint.id) entry.cancelled = true;
    }
    if (['SessionEnd', 'Interrupt'].includes(type)) {
      const row = this.sessions.get(endpoint.id);
      if (row?.connection) { try { this.core.disconnect(row.connection); } catch (e) { if (e.code !== 'STALE_CONNECTION') throw e; } }
      this.sessions.delete(endpoint.id); this.windows.delete(endpoint.id);
      return { state: 'offline' };
    }
    // Mark busy before connect emits activity, so a turn-start cannot race an idle wake.
    const previous = this.sessions.get(endpoint.id); if (previous) previous.state = 'busy';
    const row = this._session(endpoint, credential); row.seen = this.now(); row.state = ['SessionStart', 'Stop'].includes(type) ? 'idle' : 'busy';
    if (type === 'PreToolUse') return this._ticket(endpoint, row, event);
    if (type === 'UserPromptSubmit' && this.coordinator) {
      insist(!this.windows.has(endpoint.id), 'HOST_BUSY', 'Another natural-turn hook is active');
      const window = { notice: null, batch: null }; this.windows.set(endpoint.id, window);
      try {
        const autoSync = event.history_sync === true && (!event.turn_id || row.historySyncTurn !== event.turn_id);
        await this.coordinator.pump(endpoint.id); // Drain a concurrent activity probe first.
        await this.coordinator.pump(endpoint.id, { naturalTurn: true, historySync: event.history_sync === true });
        const history_sources = autoSync ? this.core.historySourcesForSync(row.connection) : [];
        if (history_sources.length) row.historySyncTurn = event.turn_id;
        return { state: 'busy', notice: window.notice, batch_id: window.batch, ...(history_sources.length ? { history_sources } : {}) };
      } finally { this.windows.delete(endpoint.id); }
    }
    if (type === 'Stop') this.core._notify(endpoint.id);
    return { state: row.state };
  }
  _ticket(endpoint, row, event) {
    this._clean();
    text(event.tool_use_id, 'tool_use_id', 256); text(event.tool_name, 'tool_name', 256);
    insist(event.tool_name.startsWith(CODEX_TOOL_PREFIX), 'INVALID_INPUT', 'Hook is not for this MCP server');
    const name = event.tool_name.slice(CODEX_TOOL_PREFIX.length); insist(TOOL_NAMES.has(name), 'NOT_FOUND', 'Unknown bridge tool');
    const args = object(event.tool_input ?? {}, 'tool_input');
    insist(!Object.hasOwn(args, '_bridge_ticket'), 'INVALID_INPUT', 'The host ticket must not be supplied by the model');
    const fingerprint = digest(canonical({ name, args })), key = canonical([endpoint.id, event.tool_use_id]);
    const previous = this.calls.get(key);
    if (previous) {
      insist(previous.fingerprint === fingerprint, 'CALL_CONFLICT', 'Native call ID was reused with different arguments');
      insist(previous.epoch === row.connection.epoch, 'STALE_CONNECTION', 'Native call IDs cannot be reused across connections');
      insist(!previous.used && !previous.redeemed && !previous.cancelled && previous.approvalExpires > this.now(), 'TICKET_USED', 'Native call has already been consumed or expired');
      return this._issued(endpoint, previous);
    }
    insist(this.calls.size < 4096, 'HOST_BUSY', 'Too many recent hook calls');
    const token = randomBytes(32).toString('base64url'), proof = randomBytes(32).toString('base64url');
    const entry = { token, proof, fingerprint, name, endpoint: endpoint.id, epoch: row.connection.epoch, used: false,
      redeemed: false, cancelled: false, expires: this.now() + this.ticketMs, approvalExpires: this.now() + this.approvalMs,
      forget: this.now() + this.approvalMs + 300_000 };
    this.calls.set(key, entry); this.tickets.set(ticketHash(token), entry); this.approvals.set(ticketHash(proof), entry);
    return this._issued(endpoint, entry);
  }
  _issued(endpoint, entry) {
    return { ticket: entry.token, expires_at: entry.expires, approval: {
      endpoint_id: endpoint.id, native_session_id: endpoint.native_session_id, proof: entry.proof,
    }, approval_expires_at: entry.approvalExpires };
  }
  exchange(credential, input) {
    this._clean(); object(input, 'approval exchange');
    keys(input, ['endpoint_id', 'native_session_id', 'proof', 'name', 'arguments'], 'approval exchange');
    const endpoint = this.core.authenticateEndpoint(input.endpoint_id, credential);
    const entry = this.approvals.get(ticketHash(input.proof));
    insist(entry && !entry.cancelled, 'UNAUTHORIZED', 'Missing, cancelled or expired native approval proof');
    insist(endpoint.harness === 'codex' && endpoint.id === entry.endpoint && endpoint.native_session_id === input.native_session_id,
      'CALLER_MISMATCH', 'Approval proof does not match the private endpoint binding');
    insist(!entry.used && !entry.redeemed, 'TICKET_USED', 'Native approval proof has already been redeemed');
    const args = object(input.arguments ?? {}, 'arguments');
    insist(entry.name === input.name && entry.fingerprint === digest(canonical({ name: input.name, args })),
      'CALL_CONFLICT', 'Tool or arguments differ from the native hook');
    const row = this.sessions.get(entry.endpoint);
    insist(row?.state === 'busy' && row.connection?.epoch === entry.epoch && endpoint.epoch === entry.epoch,
      'STALE_CONNECTION', 'Native approval connection was closed or replaced');
    // A valid pending native proof plus its private endpoint secret may renew an
    // expired lease. A replacement epoch, interrupt or new turn cannot be revived.
    this._session(endpoint, credential); entry.epoch = row.connection.epoch;
    this.tickets.delete(ticketHash(entry.token));
    entry.token = randomBytes(32).toString('base64url'); entry.expires = this.now() + this.ticketMs; entry.redeemed = true;
    this.tickets.set(ticketHash(entry.token), entry); row.seen = this.now();
    return { ticket: entry.token, expires_at: entry.expires };
  }
  async call(token, input) {
    this._clean(); object(input, 'tool call'); keys(input, ['name', 'arguments'], 'tool call');
    const entry = this.tickets.get(ticketHash(token)); insist(entry, 'UNAUTHORIZED', 'Missing or expired native hook ticket');
    insist(!entry.used, 'TICKET_USED', 'Native hook ticket has already been used');
    const args = object(input.arguments ?? {}, 'arguments');
    insist(entry.name === input.name && entry.fingerprint === digest(canonical({ name: input.name, args })), 'CALL_CONFLICT', 'Tool or arguments differ from the native hook');
    const row = this.sessions.get(entry.endpoint);
    insist(row?.connection && row.connection.epoch === entry.epoch, 'STALE_CONNECTION', 'Native hook connection was replaced');
    this.core.sessionInfo(row.connection);
    insist(!entry.cancelled, 'UNAUTHORIZED', 'Native call was cancelled by a host lifecycle event'); entry.used = true;
    const caller = codexHostProof(this.core.endpoint(entry.endpoint).native_session_id);
    const result = await row.bridge.execute(input.name, args, caller);
    return input.name === 'bridge_context' ? { ...result, settings_url: this.settingsUrl } : result;
  }
  async historySyncCall(token, input) {
    this._clean(); object(input, 'history sync step');
    keys(input, ['arguments', 'tool', 'parameters'], 'history sync step');
    const entry = this.tickets.get(ticketHash(token));
    insist(entry && !entry.cancelled && entry.name === 'bridge_sync_history', 'UNAUTHORIZED', 'Missing or expired native history sync ticket');
    const args = object(input.arguments, 'approved arguments'), parameters = object(input.parameters, 'history tool arguments');
    keys(args, ['archive_id', 'batch_id', 'budgetBytes'], 'approved history sync arguments');
    insist(entry.fingerprint === digest(canonical({ name: entry.name, args })), 'CALL_CONFLICT', 'History sync differs from the approved tool call');
    const row = this.sessions.get(entry.endpoint);
    insist(row?.state === 'busy' && row.connection?.epoch === entry.epoch, 'STALE_CONNECTION', 'Native history sync connection changed');
    const confirming = args.batch_id !== undefined;
    if (confirming) {
      const { batch_id } = args;
      insist(typeof batch_id === 'string' && /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(batch_id), 'INVALID_INPUT', 'Invalid history batch ID');
      insist(!Object.hasOwn(args, 'archive_id') && !Object.hasOwn(args, 'budgetBytes'), 'INVALID_INPUT', 'Confirm with only a batch ID');
      const requestId = batch_id.replaceAll('-', '');
      insist(parameters.request_id === requestId && ['bridge_ack', 'bridge_history_cursor'].includes(input.tool),
        'CALL_CONFLICT', 'History confirmation differs from the approved batch');
    } else {
      text(args.archive_id, 'archive_id', 256);
      insist(input.tool === 'bridge_context' || input.tool === 'bridge_receive' && entry.historyContextChecked
        && parameters.archive_id === args.archive_id && parameters.budgetBytes === args.budgetBytes
        && typeof parameters.request_id === 'string' && (!entry.historyRequest || entry.historyRequest === parameters.request_id),
      'CALL_CONFLICT', 'History receive differs from the approved source');
      if (input.tool === 'bridge_receive') entry.historyRequest = parameters.request_id;
    }
    this.core.sessionInfo(row.connection);
    insist((entry.historySteps ?? 0) < 12, 'TICKET_USED', 'Native history synchronization reached its tool-call limit');
    entry.historySteps = (entry.historySteps ?? 0) + 1;
    entry.used = true;
    const caller = codexHostProof(this.core.endpoint(entry.endpoint).native_session_id);
    const result = await row.bridge.execute(input.tool, parameters, caller);
    if (input.tool === 'bridge_context') entry.historyContextChecked = true;
    return result;
  }
  hostState(endpoint) {
    const row = this.sessions.get(endpoint.id);
    return !row || !endpoint.online || row.seen + this.presenceMs <= this.now() ? 'offline' : row.state;
  }
  toolTransportResult(ticket, value) {
    const entry = this.tickets.get(ticketHash(ticket)), row = entry && this.sessions.get(entry.endpoint);
    insist(entry?.used && row?.connection?.epoch === entry.epoch, 'STALE_CONNECTION', 'Native call connection changed before transport response');
    this.core.sessionInfo(row.connection); this._clean();
    let capability = [...this.liveness.values()].find(c => c.endpoint === entry.endpoint && c.epoch === entry.epoch);
    if (!capability) {
      insist(this.liveness.size < 4096, 'HOST_BUSY', 'Too many plugin connections');
      capability = { token: randomBytes(32).toString('base64url'), endpoint: entry.endpoint, epoch: entry.epoch, expires: this.now() + this.presenceMs };
      this.liveness.set(ticketHash(capability.token), capability);
    }
    return { transport: 'local-bridge.codex-tool-result/1', value, liveness: capability.token, endpoint_id: entry.endpoint };
  }
  hostHeartbeat(token) {
    this._clean(); const capability = this.liveness.get(ticketHash(token));
    insist(capability, 'UNAUTHORIZED', 'Missing or expired plugin liveness capability');
    const row = this.sessions.get(capability.endpoint);
    insist(row?.connection?.epoch === capability.epoch, 'STALE_CONNECTION', 'Plugin endpoint was closed or replaced');
    this.core.heartbeat(row.connection); row.seen = this.now(); capability.expires = this.now() + this.presenceMs;
    return { online: true }; // Heartbeats cannot change busy/idle or mint tool tickets.
  }
  offerNatural(input, notice) {
    const window = this.windows.get(input.endpoint.id);
    if (!window || window.notice) return { accepted: false, error: 'No open Codex natural-turn context slot' };
    window.notice = notice; window.batch = input.batch_id;
    return { accepted: true, native_ref: 'codex-hook:' + input.batch_id };
  }
}
