import { readFile } from 'node:fs/promises';
import { decodeHistory } from './history.mjs';
import { integer } from './protocol.mjs';

/** Reads only operator-registered paths. Parsed records, fanout and checkpoint commit together. */
export class SourceIndexer {
  constructor({ core, adminToken }) { this.core = core; this.adminToken = adminToken; this.running = new Map(); }
  scan(archiveId, { maxPages = 1 } = {}) {
    integer(maxPages, 'maxPages', 1, 1000);
    if (this.running.has(archiveId)) return this.running.get(archiveId);
    const work = this._scan(archiveId, maxPages).finally(() => this.running.delete(archiveId));
    this.running.set(archiveId, work); return work;
  }
  async _scan(archiveId, maxPages) {
    let source = this.core.sourceState(this.adminToken, archiveId), pages = 0, records = 0; const queued = [];
    try {
      const bytes = await readFile(source.path);
      let decoded;
      do {
        decoded = decodeHistory(bytes, { archive_id: archiveId, generation: source.generation, format: source.format, checkpoint: source.checkpoint, maxDecodedBytes: source.max_decoded_bytes });
        const result = this.core.commitIndexedPage(this.adminToken, archiveId, source.checkpoint, decoded);
        pages++; records += result.indexed_records; queued.push(...result.queued);
        source = { ...source, checkpoint: result.checkpoint };
      } while (decoded.has_more && pages < maxPages);
      return { archive_id: archiveId, pages, records, queued, has_more: decoded.has_more, incomplete_tail: decoded.incomplete_tail, checkpoint: source.checkpoint };
    } catch (error) { if (error.code !== 'CHECKPOINT_CONFLICT') this.core.sourceError(this.adminToken, archiveId, String(error.code ?? error.name)); throw error; }
  }
}
