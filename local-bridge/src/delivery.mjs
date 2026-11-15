import { MAX_BATCH_COUNT, MAX_BATCH_BYTES, integer } from './protocol.mjs';

/** One delivery path for both content types. Adapters never generate read receipts. */
export class DeliveryCoordinator {
  constructor({ core, adapters = new Map(), timeoutMs = 5000, auto = true, historyBatchNotices = false, onError = () => {} }) {
    integer(timeoutMs, 'timeoutMs', 1, 30_000);
    this.core = core; this.adapters = adapters; this.timeoutMs = timeoutMs; this.onError = onError;
    this.historyBatchNotices = historyBatchNotices;
    this.running = new Map(); this.controllers = new Set(); this.closed = false;
    this.listener = endpoint => { this.pump(endpoint).catch(onError); };
    if (auto) core.on('activity', this.listener);
  }
  pump(target, { naturalTurn = false, historySync = false } = {}) {
    if (this.closed) return Promise.resolve({ state: 'closed' });
    if (this.running.has(target)) return this.running.get(target);
    const work = this._pump(target, naturalTurn, historySync).finally(() => this.running.delete(target));
    this.running.set(target, work); return work;
  }
  async _pump(target, naturalTurn, historySync) {
    const endpoint = this.core.endpoint(target), adapter = this.adapters.get(endpoint.harness);
    if (!endpoint.online) return { state: 'offline' };
    if (endpoint.settings.paused) return { state: 'paused' };
    if (!adapter || !endpoint.capabilities.can_enqueue) return { state: 'capability_missing' };
    let stateTimer;
    let nativeState;
    try {
      nativeState = await Promise.race([Promise.resolve().then(() => adapter.state(endpoint)), new Promise((_, reject) => { stateTimer = setTimeout(() => reject(new Error('Host status lookup timed out')), this.timeoutMs); })]);
    } catch { return { state: 'state_unknown' }; }
    finally { clearTimeout(stateTimer); }
    if (nativeState === 'offline') return { state: 'offline' };
    if (!naturalTurn && nativeState !== 'idle') return { state: 'busy' };
    const pending = this.core.pending(target).filter(row => !historySync || row.content_type !== 'history_sync');
    if (!pending.length) return { state: 'empty' };
    const wakeRows = pending.filter(row => row.allow_wake && endpoint.settings[row.content_type] === 'idle_wake');
    if (!naturalTurn && (!endpoint.capabilities.can_wake || !wakeRows.length)) return { state: 'waiting_turn' };
    // A wake-authorized communication must not be blocked behind deferred history.
    const first = !naturalTurn ? wakeRows[0] : null;
    const ordered = first ? [first, ...pending.filter(row => row.id !== first.id)] : pending;
    const batch = []; let bytes = 0;
    for (const row of ordered) {
      if (batch.length >= MAX_BATCH_COUNT || bytes + row.payload_bytes > MAX_BATCH_BYTES) break;
      batch.push(row); bytes += row.payload_bytes;
    }
    batch.sort((a, b) => a.queue_seq - b.queue_seq);
    const canWake = endpoint.capabilities.can_wake && batch.some(row => row.allow_wake && endpoint.settings[row.content_type] === 'idle_wake');
    if (!naturalTurn && !canWake) return { state: 'waiting_turn' };
    const historySources = this.historyBatchNotices ? this.core.historySourcesForOffer(target, batch) : [];
    const notice = historySources.length ? { history_sources: historySources, message_ids: batch.filter(row => row.content_type === 'message').map(row => row.id) } : {};
    const attempt = this.core.beginOffer(target, batch.map(m => m.id), !naturalTurn);
    const controller = new AbortController(); this.controllers.add(controller);
    let timer;
    try {
      const timeout = new Promise((_, reject) => {
        timer = setTimeout(() => { controller.abort(); reject(new Error('Host delivery timed out; acceptance is unknown')); }, this.timeoutMs);
      });
      const result = await Promise.race([
        Promise.resolve().then(() => adapter.offer({
          endpoint: { ...endpoint, epoch: attempt.epoch }, batch_id: attempt.id, ids: attempt.ids,
          content_types: [...new Set(batch.map(m => m.content_type))], wake: attempt.wake,
          ...notice, source: { kind: 'local-bridge' }, signal: controller.signal,
        })), timeout,
      ]);
      return { attempt_id: attempt.id, ...this.core.finishOffer(attempt.id, result) };
    } catch (error) {
      return { attempt_id: attempt.id, ...this.core.finishOffer(attempt.id, { error: String(error.message ?? error) }) };
    } finally { clearTimeout(timer); this.controllers.delete(controller); }
  }
  async close() {
    this.closed = true; this.core.off('activity', this.listener);
    for (const controller of this.controllers) controller.abort();
    await Promise.allSettled([...this.running.values()]);
  }
}
