import { randomBytes } from 'node:crypto';
import { BridgeError, insist, object, keys, text } from './protocol.mjs';
import { SessionToolBridge } from './session-tools.mjs';
import { dshHostContext } from './adapters/dsh.mjs';

/** Host-only loopback channel. Tools and delivery share one connection/epoch. */
export class DshGateway {
  constructor({ core, now = () => Date.now(), presenceMs = 30_000, settingsUrl = 'http://127.0.0.1:4776' }) {
    this.core = core; this.now = now; this.presenceMs = presenceMs; this.settingsUrl = settingsUrl;
    this.sessions = new Map(); this.coordinator = null;
  }
  _host(credential, input, state) {
    text(input.endpoint_id, 'endpoint_id', 256); text(input.native_session_id, 'native_session_id', 256); text(input.host_instance, 'host_instance', 128);
    const endpoint = this.core.authenticateEndpoint(input.endpoint_id, credential);
    insist(endpoint.harness === 'dsh' && endpoint.native_session_id === input.native_session_id, 'CALLER_MISMATCH', 'Native DSH session differs from its registration');
    let row = this.sessions.get(endpoint.id);
    insist(!row || row.host === input.host_instance || row.seen + this.presenceMs <= this.now(), 'HOST_BUSY', 'Another live DSH host owns this endpoint');
    if (row && row.host !== input.host_instance) { this._drop(row); this.sessions.delete(endpoint.id); row = null; }
    if (!row) {
      row = { id: endpoint.id, nativeId: endpoint.native_session_id, host: input.host_instance, seen: this.now(), state: state ?? 'busy', connection: null, command: null };
      this.sessions.set(endpoint.id, row);
    }
    // Set host state before connect emits activity.
    if (state) row.state = state;
    row.seen = this.now();
    if (row.connection) {
      try { this.core.heartbeat(row.connection); }
      catch (error) { if (error.code !== 'STALE_CONNECTION') throw error; row.connection = null; }
    }
    if (!row.connection) row.connection = this.core.connect(endpoint.id, credential);
    if (!row.bridge) {
      const client = {
        get connection() { return row.connection; }, set connection(value) { row.connection = value; },
        connect: async () => { throw new BridgeError('STALE_CONNECTION', 'A native host pulse is required to reconnect'); },
        sessionInfo: async () => this.core.sessionInfo(row.connection),
        receive: async args => this.core.receive(row.connection, args),
        acknowledge: async args => this.core.acknowledge(row.connection, args),
        publish: async args => this.core.publish(row.connection, args),
        status: async id => this.core.status(row.connection, id),
        request: async path => {
          const url = new URL(path, 'http://127.0.0.1');
          insist(url.pathname === '/api/cursor', 'INVALID_INPUT', 'Unsupported query');
          const archive = url.searchParams.get('archive_id'), generation = url.searchParams.get('generation'), requestId = url.searchParams.get('request_id');
          return requestId === null ? { read_through: this.core.historyCursor(row.connection, archive, generation) }
            : this.core.historySyncStatus(row.connection, archive, generation, requestId);
        },
      };
      row.bridge = new SessionToolBridge({ client, endpointId: endpoint.id, credential, binding: endpoint, getHostContext: dshHostContext });
    }
    return row;
  }
  _drop(row) {
    row.command?.finish({ error: 'Native DSH host disconnected; acceptance is unknown' });
    if (row.connection) { try { this.core.disconnect(row.connection); } catch (e) { if (e.code !== 'STALE_CONNECTION') throw e; } }
  }
  async pulse(credential, input) {
    object(input, 'native pulse'); keys(input, ['endpoint_id', 'native_session_id', 'host_instance', 'state', 'natural_turn', 'history_sync'], 'native pulse');
    insist(['idle', 'busy', 'offline'].includes(input.state), 'INVALID_INPUT', 'Invalid native state');
    insist(input.natural_turn === undefined || typeof input.natural_turn === 'boolean', 'INVALID_INPUT', 'Invalid natural-turn flag');
    insist(input.history_sync === undefined || input.history_sync === true && input.natural_turn === true, 'INVALID_INPUT', 'History sync needs a natural turn');
    insist(!input.natural_turn || input.state === 'busy', 'INVALID_INPUT', 'A native pre-step must belong to an already running agent');
    if (input.state === 'offline') {
      const endpoint = this.core.authenticateEndpoint(input.endpoint_id, credential), row = this.sessions.get(endpoint.id);
      insist(endpoint.harness === 'dsh' && endpoint.native_session_id === input.native_session_id, 'CALLER_MISMATCH', 'Native DSH session differs from registration');
      if (row) { insist(row.host === input.host_instance, 'HOST_BUSY', 'A different DSH host owns this endpoint'); this._drop(row); this.sessions.delete(endpoint.id); }
      return { state: 'offline', command: null };
    }
    const row = this._host(credential, input, input.state);
    let pumping = Promise.resolve();
    if (this.coordinator) {
      const run = async () => {
        await this.coordinator.pump(row.id);
        if (input.natural_turn) await this.coordinator.pump(row.id, { naturalTurn: true, historySync: Boolean(input.history_sync) });
      };
      // Delivery waits for a separate host-result request, never for this pulse.
      pumping = run().catch(() => {});
    }
    if (!row.command && input.natural_turn) {
      let done;
      const arrived = new Promise(resolve => {
        done = () => { clearTimeout(timer); if (row.waiter === done) row.waiter = null; resolve(); };
        const timer = setTimeout(done, 1000); row.waiter = done;
      });
      await Promise.race([arrived, pumping]); done();
    }
    const command = row.command;
    const history_sources = input.history_sync ? this.core.historySourcesForSync(row.connection) : [];
    const sync = history_sources.length ? { history_sources } : {};
    if (!command || command.sent || command.epoch !== row.connection.epoch || (!command.wire.wake && !input.natural_turn)) return { state: row.state, command: null, ...sync };
    command.sent = true;
    return { state: row.state, command: command.wire, ...sync };
  }
  async call(credential, input) {
    object(input, 'native tool call'); keys(input, ['endpoint_id', 'native_session_id', 'host_instance', 'name', 'arguments'], 'native tool call');
    const row = this._host(credential, input, 'busy');
    const value = await row.bridge.execute(input.name, input.arguments ?? {}, { agent: { session: { id: row.nativeId } } });
    return input.name === 'bridge_context' ? { ...value, settings_url: this.settingsUrl } : value;
  }
  result(credential, input) {
    object(input, 'native delivery result'); keys(input, ['endpoint_id', 'native_session_id', 'host_instance', 'command_id', 'accepted', 'native_ref', 'error'], 'native delivery result');
    const endpoint = this.core.authenticateEndpoint(input.endpoint_id, credential), row = this.sessions.get(endpoint.id);
    insist(endpoint.harness === 'dsh' && endpoint.native_session_id === input.native_session_id && row?.host === input.host_instance, 'CALLER_MISMATCH', 'Native delivery host differs from registration');
    const command = row.command;
    insist(command?.wire.command_id === input.command_id && command.sent && command.epoch === row.connection.epoch, 'STALE_CONNECTION', 'Delivery command expired or belongs to another host connection');
    this.core.sessionInfo(row.connection);
    insist(input.accepted === undefined || typeof input.accepted === 'boolean', 'INVALID_INPUT', 'Invalid delivery acceptance');
    if (input.accepted === true) text(input.native_ref, 'native_ref', 256);
    if (input.error !== undefined) text(input.error, 'error', 512);
    command.finish({ accepted: input.accepted, native_ref: input.native_ref, error: input.error });
    return { recorded: true };
  }
  state(endpoint) {
    const row = this.sessions.get(endpoint.id);
    return !row || !endpoint.online || row.seen + this.presenceMs <= this.now() ? 'offline' : row.state;
  }
  offer(input) {
    const row = this.sessions.get(input.endpoint.id);
    if (this.state(input.endpoint) === 'offline') return { accepted: false, error: 'Native DSH host is offline' };
    if (row.command) return { accepted: false, error: 'Native DSH host already has a delivery command' };
    if (input.wake && row.state !== 'idle') return { accepted: false, error: 'Native DSH host is busy' };
    input.signal?.throwIfAborted();
    return new Promise(resolve => {
      const command = { epoch: input.endpoint.epoch, sent: false, wire: {
        command_id: randomBytes(24).toString('base64url'), endpoint: { id: row.id, harness: 'dsh', native_session_id: row.nativeId },
        batch_id: input.batch_id, ids: input.ids, content_types: input.content_types, wake: input.wake, expires_at: this.now() + 4000,
        ...(input.history_sources?.length ? { history_sources: input.history_sources, message_ids: input.message_ids } : {}),
      } };
      const abort = () => command.finish({ error: 'DSH host channel timed out; acceptance is unknown' });
      command.finish = result => { if (row.command !== command) return; row.command = null; input.signal?.removeEventListener('abort', abort); resolve(result); };
      row.command = command; input.signal?.addEventListener('abort', abort, { once: true }); row.waiter?.();
    });
  }
  close() { for (const row of this.sessions.values()) this._drop(row); this.sessions.clear(); }
}
