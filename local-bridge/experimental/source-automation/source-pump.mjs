// Opt-in isolated scheduling prototype. No native chat dispatch or wake capability.
import { watch } from 'node:fs';
import { stat } from 'node:fs/promises';
import { dirname, basename } from 'node:path';
import { integer, insist } from '../../src/protocol.mjs';
import { readBoundedPage } from './bounded-reader.mjs';

export class BoundedSourceIndexer {
  constructor({ core, adminToken, readerOptions = {} }) {
    Object.assign(this, { core, adminToken, readerOptions }); this.running = new Map();
  }
  scan(archiveId, { maxPages = 1 } = {}) {
    integer(maxPages, 'maxPages', 1, 256);
    if (this.running.has(archiveId)) return this.running.get(archiveId);
    const work = this._scan(archiveId, maxPages).finally(() => this.running.delete(archiveId));
    this.running.set(archiveId, work); return work;
  }
  async _scan(archiveId, maxPages) {
    let source = this.core.sourceState(this.adminToken, archiveId), page, pages = 0, records = 0;
    const queued = [], stats = [];
    try {
      do {
        this.core.prepareIndex?.(this.adminToken, archiveId);
        page = await readBoundedPage(source.path, { ...this.readerOptions,
          ...(this.core.indexReadOptions?.(this.adminToken, archiveId) ?? {}),
          maxDecodedBytes: Math.min(source.max_decoded_bytes, this.readerOptions.maxDecodedBytes ?? 1024 * 1024),
          archive_id: archiveId, generation: source.generation, format: source.format, checkpoint: source.checkpoint });
        const result = this.core.commitIndexedPage(this.adminToken, archiveId, source.checkpoint, page);
        if (result.has_more !== undefined) page.has_more = result.has_more;
        source = { ...source, checkpoint: result.checkpoint };
        pages++; records += result.indexed_records; queued.push(...result.queued); stats.push(page.stats);
      } while (page.has_more && !page.blocked_by_budget && pages < maxPages);
      if (page.blocked_by_budget) this.core.sourceError(this.adminToken, archiveId, 'ARCHIVE_BUDGET');
      return { archive_id: archiveId, pages, records, queued, has_more: page.has_more,
        incomplete_tail: page.incomplete_tail, blocked_by_budget: page.blocked_by_budget,
        checkpoint: source.checkpoint, stats };
    } catch (error) {
      if (!['CHECKPOINT_CONFLICT', 'SOURCE_CHANGED'].includes(error.code))
        this.core.sourceError(this.adminToken, archiveId, String(error.code ?? error.name));
      throw error;
    }
  }
}

/** One bounded page per source in each pass. Changes coalesce; quiet partial tails do not spin. */
export class SourcePump {
  constructor({ indexer, sources, onResult = () => {}, onError = () => {}, delayMs = 100, pollMs = 5000,
    retryMaxMs = 30000, now = Date.now }) {
    integer(delayMs, 'delayMs', 1, 60000); integer(pollMs, 'pollMs', 10, 3600000);
    integer(retryMaxMs, 'retryMaxMs', 1, 60000);
    insist(Array.isArray(sources) && sources.length > 0 && sources.length <= 64, 'INVALID_INPUT', 'A bounded source list is required');
    insist(new Set(sources.map(s => s.archive_id)).size === sources.length, 'INVALID_INPUT', 'Duplicate source IDs');
    Object.assign(this, { indexer, onResult, onError, delayMs, pollMs, retryMaxMs, now });
    this.sources = new Map(sources.map(s => [s.archive_id, { ...s, observed: null }]));
    this.dirty = new Set(); this.watchers = []; this.closed = false; this.running = null;
    this.retryAfter = new Map(); this.failures = new Map();
  }
  notify(archiveId) {
    insist(this.sources.has(archiveId), 'NOT_FOUND', 'Source is not managed by this pump');
    if (this.closed) return;
    this.dirty.add(archiveId); this._schedule();
  }
  resume(archiveId) {
    this.retryAfter.delete(archiveId); this.failures.delete(archiveId); this.notify(archiveId);
  }
  _schedule() {
    if (this.closed || this.running || !this.dirty.size) return;
    const earliest = Math.min(...[...this.dirty].map(id => this.retryAfter.get(id) ?? 0));
    const wait = Math.max(this.delayMs, earliest - this.now());
    const due = this.now() + wait;
    if (this.timer && this.timerAt <= due) return;
    clearTimeout(this.timer); this.timerAt = due;
    this.timer = setTimeout(() => { this.timer = null; this.timerAt = null; void this.flush(); }, wait);
    this.timer.unref();
  }
  async _report(callback, ...args) {
    // Observers cannot undo commits or turn successful scans into repeated work.
    try { await callback(...args); } catch { /* Optional observation only. */ }
  }
  flush() {
    if (this.running) return this.running;
    if (this.closed) return Promise.resolve([]);
    if (this.timer) { clearTimeout(this.timer); this.timer = null; this.timerAt = null; }
    const batch = [...this.dirty].filter(id => (this.retryAfter.get(id) ?? 0) <= this.now());
    batch.forEach(id => this.dirty.delete(id));
    if (!batch.length) { this._schedule(); return Promise.resolve([]); }
    this.running = (async () => {
      const results = [];
      for (const id of batch) {
        if (this.closed) break;
        try {
          const result = await this.indexer.scan(id, { maxPages: 1 }); results.push(result);
          this.failures.delete(id); this.retryAfter.delete(id);
          await this._report(this.onResult, id, result);
          if (result.has_more && !result.blocked_by_budget) this.dirty.add(id);
        } catch (error) {
          if (['SOURCE_CHANGED', 'CHECKPOINT_CONFLICT', 'QUEUE_PRESSURE'].includes(error.code)) {
            const failures = Math.min(16, (this.failures.get(id) ?? 0) + 1);
            this.failures.set(id, failures);
            this.retryAfter.set(id, this.now() + Math.min(this.retryMaxMs, this.delayMs * 2 ** failures));
            this.dirty.add(id);
          }
          await this._report(this.onError, id, error);
        }
      }
      return results;
    })().finally(() => { this.running = null; this._schedule(); });
    return this.running;
  }
  async poll() {
    if (this.closed || this.polling) return;
    this.polling = true;
    try {
      for (const [id, source] of this.sources) {
        if (this.closed) break;
        try {
          const s = await stat(source.path, { bigint: true });
          const signature = [s.dev, s.ino, s.birthtimeNs, s.size, s.mtimeNs, s.ctimeNs].map(String).join(':');
          if (source.observed !== signature) { source.observed = signature; this.notify(id); }
        } catch (error) { await this._report(this.onError, id, error); }
      }
    } finally { this.polling = false; }
  }
  async startWatching() {
    insist(!this.started && !this.closed, 'INVALID_STATE', 'Pump already started or closed');
    this.started = true;
    for (const [id, source] of this.sources) {
      if (this.closed) return;
      try {
        // Parent-directory watch survives file replacement. Polling covers missed/coalesced events.
        const watcher = watch(dirname(source.path), { persistent: false }, (_event, filename) => {
          if (filename === null || String(filename) === basename(source.path)) this.notify(id);
        });
        watcher.on('error', error => { void this._report(this.onError, id, error); });
        this.watchers.push(watcher);
      } catch (error) { await this._report(this.onError, id, error); }
    }
    if (this.closed) return;
    this.interval = setInterval(() => { void this.poll(); }, this.pollMs); this.interval.unref();
    await this.poll();
  }
  async close() {
    this.closed = true;
    clearTimeout(this.timer); clearInterval(this.interval);
    this.timer = null; this.timerAt = null;
    this.watchers.forEach(w => w.close()); this.watchers = []; this.dirty.clear();
    await this.running;
  }
}
