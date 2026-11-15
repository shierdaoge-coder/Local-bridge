// Staged prototype: not imported by the active service or either installed adapter.
import { open, stat } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { zstdDecompressSync } from 'node:zlib';
import { visibleMessage } from '../../src/history.mjs';
import { BridgeError, insist, integer, text, digest, canonical } from '../../src/protocol.mjs';

const identity = s => ({ dev: String(s.dev), ino: String(s.ino), birthtime_ns: String(s.birthtimeNs) });
const version = s => canonical({ identity: identity(s), size: String(s.size), mtime: String(s.mtimeNs), ctime: String(s.ctimeNs) });
const emptyHash = digest(Buffer.alloc(0));
export const READER_VERSION = 'bounded-v2';
export const fileIdentity = identity;

const codexPageContext = /^additional_content\.codex_apps_(?:open_page|selected_page_part_[1-9]\d*_of_[1-9]\d*)$/;

export function candidateVisibleMessage(row, format) {
  const payload = row.payload;
  if (format !== 'codex-jsonl' || row.type !== 'response_item' || payload?.type !== 'message' || payload.role !== 'user') {
    return visibleMessage(row, format);
  }
  const { content, internal_chat_message_metadata_passthrough: metadata } = payload;
  const kinds = metadata?.content_item_kinds;
  const items = metadata?.content_item_metadata;
  if (!Array.isArray(content) || !Array.isArray(kinds) || !Array.isArray(items)
    || kinds.length !== content.length || items.length !== content.length) return visibleMessage(row, format);
  const kept = content.filter((block, index) => {
    const provenance = items[index]?.provenance;
    const kind = kinds[index];
    return !(block?.type === 'input_text' && provenance?.type === 'harness' && provenance.hook === false
      && typeof kind === 'string' && codexPageContext.test(kind));
  });
  return kept.length === content.length ? visibleMessage(row, format)
    : visibleMessage({ ...row, payload: { ...payload, content: kept } }, format);
}

class Cursor {
  constructor(handle, size, start, chunkBytes, maxReadBytes) {
    Object.assign(this, { handle, size, position: start, chunkBytes, maxReadBytes });
    this.buffer = Buffer.alloc(0); this.bufferOffset = 0;
    this.stats = { bytes_read: 0, max_read_request: 0, max_unit_bytes: 0, prefix_bytes_verified: 0 };
  }
  async fill() {
    if (this.bufferOffset < this.buffer.length) return true;
    if (this.position >= this.size) return false;
    const length = Math.min(this.chunkBytes, this.size - this.position);
    insist(this.stats.bytes_read + length <= this.maxReadBytes, 'ARCHIVE_IO_BUDGET', 'Source scan I/O budget exceeded');
    const buffer = Buffer.allocUnsafe(length);
    const { bytesRead } = await this.handle.read(buffer, 0, length, this.position);
    insist(bytesRead > 0, 'SOURCE_CHANGED', 'Source changed while reading');
    this.stats.bytes_read += bytesRead;
    this.stats.max_read_request = Math.max(this.stats.max_read_request, length);
    this.buffer = buffer.subarray(0, bytesRead); this.bufferOffset = 0;
    return true;
  }
  async exact(length) {
    if (this.size - this.position < length) return null;
    const chunks = []; let remaining = length;
    while (remaining) {
      if (!await this.fill()) return null;
      const count = Math.min(remaining, this.buffer.length - this.bufferOffset);
      chunks.push(this.buffer.subarray(this.bufferOffset, this.bufferOffset + count));
      this.bufferOffset += count; this.position += count; remaining -= count;
    }
    return chunks.length === 1 ? chunks[0] : Buffer.concat(chunks, length);
  }
  async line(limit) {
    const chunks = []; let length = 0;
    while (await this.fill()) {
      const newline = this.buffer.indexOf(10, this.bufferOffset);
      const count = (newline < 0 ? this.buffer.length : newline + 1) - this.bufferOffset;
      insist(length + count <= limit, 'ARCHIVE_BUDGET', 'A JSONL record exceeds the unit/page budget');
      chunks.push(this.buffer.subarray(this.bufferOffset, this.bufferOffset + count));
      this.bufferOffset += count; this.position += count; length += count;
      if (newline >= 0) return Buffer.concat(chunks, length);
    }
    return null; // A partial final record is never committed.
  }
  async frame(limit) {
    const pieces = []; let length = 0;
    const take = async n => {
      insist(length + n <= limit, 'ARCHIVE_BUDGET', 'An encoded frame exceeds the unit budget');
      const bytes = await this.exact(n);
      if (bytes) { pieces.push(bytes); length += n; }
      return bytes;
    };
    const magic = await take(4); if (!magic) return null;
    insist(magic.readUInt32LE() === 0xfd2fb528, 'CORRUPT_ARCHIVE', 'Invalid Zstandard frame');
    const header = await take(1); if (!header) return null;
    const descriptor = header[0];
    insist((descriptor & 24) === 0, 'CORRUPT_ARCHIVE', 'Reserved Zstandard header bits');
    const sizeFlag = descriptor >>> 6, single = Boolean(descriptor & 32), dictionaryFlag = descriptor & 3;
    const headerBytes = (single ? 0 : 1) + (dictionaryFlag === 3 ? 4 : dictionaryFlag)
      + (sizeFlag === 0 ? single ? 1 : 0 : 1 << sizeFlag);
    if (!await take(headerBytes)) return null;
    let blocks = 0;
    for (;;) {
      insist(++blocks <= 65536, 'ARCHIVE_BUDGET', 'Frame block count exceeds budget');
      const block = await take(3); if (!block) return null;
      const value = block.readUIntLE(0, 3), kind = value >>> 1 & 3;
      insist(kind !== 3, 'CORRUPT_ARCHIVE', 'Reserved Zstandard block type');
      if (!await take(kind === 1 ? 1 : value >>> 3)) return null;
      if (value & 1) break;
    }
    if ((descriptor & 4) && !await take(4)) return null;
    return Buffer.concat(pieces, length);
  }
}

/** Fixed snapshot, bounded reads/units/output, streaming prefix verification; fail closed on mutations. */
export async function readBoundedPage(path, { archive_id, generation, format, checkpoint,
  maxDecodedBytes = 1024 * 1024, maxEncodedUnitBytes = maxDecodedBytes + 65536,
  maxVisibleRecords = 200, chunkBytes = 65536, maxPrefixBytes = 256 * 1024 * 1024,
  maxReadBytes = maxPrefixBytes + maxEncodedUnitBytes * 2 + chunkBytes,
  afterRead, expectedFileIdentity, expectedHeaderSha256, snapshotBytes } = {}) {
  text(archive_id, 'archive_id', 256); text(generation, 'generation', 256);
  insist(['codex-jsonl', 'dsh-zstd'].includes(format), 'INVALID_INPUT', 'Unknown source format');
  for (const [key, value] of Object.entries({ maxDecodedBytes, maxEncodedUnitBytes, maxVisibleRecords,
    chunkBytes, maxPrefixBytes, maxReadBytes })) integer(value, key, 1);
  insist(chunkBytes <= 65536 && maxDecodedBytes <= 64 * 1024 * 1024
    && maxEncodedUnitBytes <= 64 * 1024 * 1024, 'INVALID_INPUT', 'Reader memory budget is too large');
  const previous = checkpoint ?? { archive_id, generation, byte_offset: 0, line_offset: 0, visible_next: 0, prefix_sha256: emptyHash };
  insist(!previous.reader_version || previous.reader_version === READER_VERSION, 'READER_VERSION_MISMATCH', 'Checkpoint requires a different reader');
  insist(previous.archive_id === archive_id && previous.generation === generation, 'SOURCE_REPLACED', 'Wrong source generation');
  for (const key of ['byte_offset', 'line_offset', 'visible_next']) integer(previous[key], key);
  insist(/^[a-f0-9]{64}$/.test(previous.prefix_sha256), 'INVALID_INPUT', 'Invalid checkpoint hash');
  insist(previous.byte_offset <= maxPrefixBytes, 'ARCHIVE_IO_BUDGET', 'Prefix verification requires an explicit larger I/O budget');
  const handle = await open(path, 'r');
  try {
    const before = await handle.stat({ bigint: true });
    insist(before.isFile() && before.size <= BigInt(Number.MAX_SAFE_INTEGER), 'INVALID_INPUT', 'Source must be a regular bounded file');
    insist(before.size >= BigInt(previous.byte_offset), 'SOURCE_REPLACED', 'Indexed source was truncated');
    const fileIdentity = identity(before);
    insist(!expectedFileIdentity || canonical(expectedFileIdentity) === canonical(fileIdentity), 'SOURCE_REPLACED', 'Bound source file identity changed');
    insist(!previous.file_identity || canonical(previous.file_identity) === canonical(fileIdentity), 'SOURCE_REPLACED', 'Indexed source file was replaced');
    if (snapshotBytes !== undefined) {
      integer(snapshotBytes, 'snapshotBytes');
      insist(before.size >= BigInt(snapshotBytes) && previous.byte_offset <= snapshotBytes, 'SOURCE_REPLACED', 'Initial source boundary was truncated');
    }
    const cursor = new Cursor(handle, snapshotBytes ?? Number(before.size), 0, chunkBytes, maxReadBytes);
    const hash = createHash('sha256');
    while (cursor.position < previous.byte_offset) hash.update(await cursor.exact(Math.min(chunkBytes, previous.byte_offset - cursor.position)));
    cursor.stats.prefix_bytes_verified = previous.byte_offset;
    insist(hash.copy().digest('hex') === previous.prefix_sha256, 'SOURCE_REPLACED', 'Indexed prefix changed');
    const records = []; let end = previous.byte_offset, line = previous.line_offset, visible = previous.visible_next;
    let decodedSize = 0, incomplete = false, hasMore = false, blockedByBudget = null;
    let headerVerified = !expectedHeaderSha256 || previous.byte_offset > 0;
    while (cursor.position < cursor.size) {
      if (records.length >= maxVisibleRecords || decodedSize >= maxDecodedBytes) { hasMore = true; break; }
      let unit;
      try {
        unit = format === 'codex-jsonl'
          ? await cursor.line(Math.min(maxEncodedUnitBytes, maxDecodedBytes))
          : await cursor.frame(maxEncodedUnitBytes);
      } catch (error) {
        if (error.code === 'ARCHIVE_BUDGET' && end > previous.byte_offset) {
          hasMore = true; blockedByBudget = 'encoded_unit'; break;
        }
        throw error;
      }
      if (!unit) { incomplete = true; break; }
      if (!headerVerified) {
        insist(digest(unit) === expectedHeaderSha256, 'SOURCE_REPLACED', 'Bound native session header changed before initial indexing');
        headerVerified = true;
      }
      cursor.stats.max_unit_bytes = Math.max(cursor.stats.max_unit_bytes, unit.length);
      let decoded = unit;
      if (format === 'dsh-zstd') {
        try { decoded = zstdDecompressSync(unit, { maxOutputLength: maxDecodedBytes }); }
        catch (error) {
          if (error.code === 'ERR_BUFFER_TOO_LARGE') {
            if (end > previous.byte_offset) { hasMore = true; blockedByBudget = 'decoded_frame'; break; }
            throw new BridgeError('ARCHIVE_BUDGET', 'Decoded frame exceeds the page budget');
          }
          throw new BridgeError('CORRUPT_ARCHIVE', 'Complete frame failed checksum or decoding');
        }
      }
      insist(decoded.length && decoded.at(-1) === 10, 'CORRUPT_ARCHIVE', 'Complete unit must end at a JSONL boundary');
      const rows = decoded.toString('utf8').split('\n'); rows.pop();
      const pending = [];
      rows.forEach((raw, index) => {
        if (!raw.trim()) return;
        let row;
        try { row = JSON.parse(raw); } catch { throw new BridgeError('CORRUPT_ARCHIVE', 'Malformed committed JSONL record'); }
        const message = candidateVisibleMessage(row, format);
        if (message) pending.push({ seq: visible + pending.length, ...message,
          source_ref: { archive_id, start_line: line + index + 1, end_line: line + index + 1, sha256: digest(Buffer.from(raw)) } });
      });
      if (decodedSize + decoded.length > maxDecodedBytes || records.length + pending.length > maxVisibleRecords) {
        insist(end > previous.byte_offset, 'ARCHIVE_BUDGET', 'One complete unit exceeds the page budget');
        hasMore = true; break;
      }
      records.push(...pending); decodedSize += decoded.length; line += rows.length; visible += pending.length;
      hash.update(unit); end = cursor.position;
    }
    if (afterRead) await afterRead(); // deterministic mutation injection for tests only
    const [after, currentPath] = await Promise.all([handle.stat({ bigint: true }), stat(path, { bigint: true })]);
    insist(version(before) === version(after) && version(after) === version(currentPath), 'SOURCE_CHANGED', 'Source changed during page read; retry without committing');
    insist(headerVerified, 'SOURCE_REPLACED', 'Bound native session header disappeared');
    return { records, has_more: hasMore, incomplete_tail: incomplete,
      tail_complete_known: !hasMore && !incomplete, blocked_by_budget: blockedByBudget,
      stats: { ...cursor.stats, decoded_bytes: decodedSize, snapshot_bytes: Number(before.size) },
      checkpoint: { archive_id, generation, byte_offset: end, line_offset: line, visible_next: visible,
        prefix_sha256: hash.digest('hex'), file_identity: fileIdentity, reader_version: READER_VERSION } };
  } finally { await handle.close(); }
}

/** Inspect the native first complete record/frame under the same bounded I/O rules. */
export async function inspectSourceHeader(path, format, { maxHeaderBytes = 1024 * 1024 } = {}) {
  integer(maxHeaderBytes, 'maxHeaderBytes', 1, 8 * 1024 * 1024);
  insist(['codex-jsonl', 'dsh-zstd'].includes(format), 'INVALID_INPUT', 'Unknown source format');
  const handle = await open(path, 'r');
  try {
    const before = await handle.stat({ bigint: true });
    insist(before.isFile() && before.size <= BigInt(Number.MAX_SAFE_INTEGER), 'INVALID_INPUT', 'Source must be a regular file');
    const cursor = new Cursor(handle, Number(before.size), 0, 65536, maxHeaderBytes + 65536);
    const unit = format === 'codex-jsonl' ? await cursor.line(maxHeaderBytes) : await cursor.frame(maxHeaderBytes);
    insist(unit, 'SOURCE_NOT_READY', 'The native session header is incomplete');
    let decoded = unit;
    if (format === 'dsh-zstd') {
      try { decoded = zstdDecompressSync(unit, { maxOutputLength: maxHeaderBytes }); }
      catch { throw new BridgeError('CORRUPT_ARCHIVE', 'Native session header cannot be decoded within budget'); }
    }
    let header;
    try { header = JSON.parse(decoded.toString('utf8').split('\n')[0]); }
    catch { throw new BridgeError('CORRUPT_ARCHIVE', 'Native session header is malformed'); }
    const valid = format === 'codex-jsonl' ? header?.type === 'session_meta' && typeof header.payload?.id === 'string'
      : header?.type === 'session' && header.version === 4 && typeof header.id === 'string';
    insist(valid, 'SOURCE_IDENTITY_MISMATCH', 'Expected a native session header as the first record');
    const after = await handle.stat({ bigint: true }), current = await stat(path, { bigint: true });
    insist(version(before) === version(after) && version(after) === version(current), 'SOURCE_CHANGED', 'Source changed during identity inspection');
    return { native_session_id: format === 'codex-jsonl' ? header.payload.id : header.id,
      file_identity: identity(before), snapshot_bytes: Number(before.size), header_sha256: digest(unit) };
  } finally { await handle.close(); }
}
