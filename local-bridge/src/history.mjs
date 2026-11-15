import { readFile } from 'node:fs/promises';
import { zstdDecompressSync } from 'node:zlib';
import { BridgeError, insist, text, integer, digest, canonical, MAX_PAYLOAD_BYTES } from './protocol.mjs';

// Frame layout follows the Zstandard format and the installed DSH reader.
// Unlike searching for magic bytes, block lengths cannot mistake payload data for a new frame.
export function scanFrames(buffer) {
  const frames = []; let offset = 0;
  while (offset < buffer.length) {
    const start = offset;
    const incomplete = () => ({ frames, incomplete_at: start });
    if (buffer.length - offset < 4) return incomplete();
    insist(buffer.readUInt32LE(offset) === 0xfd2fb528, 'CORRUPT_ARCHIVE', `Invalid Zstandard frame at byte ${offset}`); offset += 4;
    if (offset === buffer.length) return incomplete();
    const descriptor = buffer[offset++];
    insist((descriptor & 24) === 0, 'CORRUPT_ARCHIVE', 'Reserved Zstandard header bits');
    const sizeFlag = descriptor >>> 6, single = Boolean(descriptor & 32), checksum = Boolean(descriptor & 4), dictionaryFlag = descriptor & 3;
    const headerBytes = (single ? 0 : 1) + (dictionaryFlag === 3 ? 4 : dictionaryFlag) + (sizeFlag === 0 ? single ? 1 : 0 : 1 << sizeFlag);
    if (buffer.length - offset < headerBytes) return incomplete(); offset += headerBytes;
    for (;;) {
      if (buffer.length - offset < 3) return incomplete();
      const block = buffer.readUIntLE(offset, 3); offset += 3;
      const kind = block >>> 1 & 3, bytes = kind === 1 ? 1 : block >>> 3;
      insist(kind !== 3, 'CORRUPT_ARCHIVE', 'Reserved Zstandard block type');
      if (buffer.length - offset < bytes) return incomplete(); offset += bytes;
      if (block & 1) break;
    }
    if (checksum) { if (buffer.length - offset < 4) return incomplete(); offset += 4; }
    frames.push({ start, end: offset });
  }
  return { frames, incomplete_at: null };
}

function visibleContent(content) {
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return '';
  const parts = [];
  for (const block of content) {
    if (['text', 'input_text', 'output_text'].includes(block?.type) && typeof block.text === 'string') parts.push(block.text);
    else if (['image', 'input_image', 'file', 'input_file'].includes(block?.type)) parts.push(`[附件：${block.type.includes('image') ? '图像' : '文件'}；通过原文引用按需读取]`);
  }
  return parts.join('\n');
}
function authorText(value) {
  const result = value.trim();
  if (/^<(environment_context|external_codex_apps_open_page)>[\s\S]*<\/\1>$/.test(result)) return '';
  const reply = /^<send_user_message_question_reply>\s*([\s\S]*?)\s*<\/send_user_message_question_reply>$/.exec(result);
  if (reply) {
    try { const answers = JSON.parse(reply[1]); if (Array.isArray(answers)) return answers.map(a => `问题：${a.question ?? ''}\n回答：${a.answer ?? ''}`).join('\n'); } catch { /* Keep malformed visible user text as data. */ }
  }
  return result;
}

export function visibleMessage(row, format) {
  if (format === 'codex-jsonl') {
    if (row.type !== 'response_item' || row.payload?.type !== 'message') return null;
    const message = row.payload;
    if (message.role === 'assistant') {
      const phase = message.phase ?? message.channel;
      if (!['commentary', 'final_answer', 'final'].includes(phase)) return null;
    } else if (message.role !== 'user') return null;
    const value = visibleContent(message.content), result = message.role === 'user' ? authorText(value) : value.trim();
    return result ? { role: message.role, text: result } : null;
  }
  if (format === 'dsh-zstd') {
    if (!['user/message', 'assistant/message'].includes(row.type)) return null;
    const message = row.type === 'user/message' ? row.data : row.data?.message;
    if (!message || message.role !== (row.type === 'user/message' ? 'user' : 'assistant')) return null;
    if (message.role === 'user' && message.source?.kind !== 'user') return null;
    const result = visibleContent(message.content).trim();
    return result ? { role: message.role, text: result } : null;
  }
  throw new BridgeError('INVALID_INPUT', 'Unknown archive format');
}

/** Decode only complete records. Checkpoints advance after complete frames/lines, never a torn tail. */
export function decodeHistory(buffer, { archive_id, generation, format, checkpoint, maxDecodedBytes = 64 * 1024 * 1024 }) {
  text(archive_id, 'archive_id', 256); text(generation, 'generation', 256);
  insist(['codex-jsonl', 'dsh-zstd'].includes(format), 'INVALID_INPUT', 'Unknown archive format');
  integer(maxDecodedBytes, 'maxDecodedBytes', 1, 512 * 1024 * 1024);
  const previous = checkpoint ?? { archive_id, generation, byte_offset: 0, line_offset: 0, visible_next: 0, prefix_sha256: digest(Buffer.alloc(0)) };
  insist(previous.archive_id === archive_id && previous.generation === generation, 'SOURCE_REPLACED', 'Checkpoint belongs to another source generation');
  for (const key of ['byte_offset', 'line_offset', 'visible_next']) integer(previous[key], key);
  insist(buffer.length >= previous.byte_offset && digest(buffer.subarray(0, previous.byte_offset)) === previous.prefix_sha256, 'SOURCE_REPLACED', 'Previously indexed source prefix changed');
  let decoded, end, incomplete, hasMore = false;
  if (format === 'codex-jsonl') {
    const newline = buffer.lastIndexOf(10);
    const committedEnd = Math.max(previous.byte_offset, newline + 1);
    end = committedEnd;
    if (end - previous.byte_offset > maxDecodedBytes) {
      end = buffer.lastIndexOf(10, previous.byte_offset + maxDecodedBytes - 1) + 1;
      insist(end > previous.byte_offset, 'ARCHIVE_BUDGET', 'A single committed JSONL record exceeds this page budget');
      hasMore = true;
    }
    decoded = buffer.subarray(previous.byte_offset, end); incomplete = committedEnd < buffer.length;
  } else {
    const pending = buffer.subarray(previous.byte_offset), result = scanFrames(pending), pieces = [];
    let size = 0, selectedEnd = 0;
    for (const frame of result.frames) {
      let bytes;
      try { bytes = zstdDecompressSync(pending.subarray(frame.start, frame.end), { maxOutputLength: maxDecodedBytes }); }
      catch (error) { throw new BridgeError(error.code === 'ERR_BUFFER_TOO_LARGE' ? 'ARCHIVE_BUDGET' : 'CORRUPT_ARCHIVE', 'Complete Zstandard frame failed decoding, output budget, or checksum verification', { cause: error.code }); }
      insist(bytes.length > 0 && bytes.at(-1) === 10, 'CORRUPT_ARCHIVE', 'Complete frame must end at a JSONL record boundary');
      if (size + bytes.length > maxDecodedBytes) { hasMore = true; break; }
      size += bytes.length; pieces.push(bytes); selectedEnd = frame.end;
    }
    decoded = Buffer.concat(pieces); end = previous.byte_offset + selectedEnd; incomplete = result.incomplete_at !== null;
  }
  insist(decoded.length <= maxDecodedBytes, 'ARCHIVE_BUDGET', 'Decode budget exceeded; checkpoint was not advanced');
  const rows = decoded.toString('utf8').split('\n');
  insist(rows.pop() === '', 'CORRUPT_ARCHIVE', 'Decoded records are not newline terminated');
  const records = []; let visible = previous.visible_next;
  rows.forEach((line, index) => {
    if (!line.trim()) return;
    let row;
    try { row = JSON.parse(line); } catch { throw new BridgeError('CORRUPT_ARCHIVE', `Malformed committed record at line ${previous.line_offset + index + 1}`); }
    const message = visibleMessage(row, format);
    if (message) records.push({ seq: visible++, ...message, source_ref: { archive_id, start_line: previous.line_offset + index + 1, end_line: previous.line_offset + index + 1, sha256: digest(Buffer.from(line)) } });
  });
  return { records, has_more: hasMore, incomplete_tail: incomplete, checkpoint: { archive_id, generation, byte_offset: end, line_offset: previous.line_offset + rows.length, visible_next: visible, prefix_sha256: digest(buffer.subarray(0, end)) } };
}

/** source.path comes from an operator-owned source registry, never a message's arbitrary path. */
export async function readHistoryFile(source, checkpoint) {
  const buffer = await readFile(source.path);
  return decodeHistory(buffer, { archive_id: source.archive_id, generation: source.generation, format: source.format, checkpoint, maxDecodedBytes: source.maxDecodedBytes });
}

/** Deterministic IDs permit crash replay; large records are never silently truncated. */
export function historyEnvelopes(records, { archive_id, generation }) {
  text(archive_id, 'archive_id', 256); text(generation, 'generation', 256);
  const packets = [];
  const payload = rows => ({ archive_id, generation, start_seq: rows[0].seq, end_seq: rows.at(-1).seq, records: rows });
  const fits = rows => Buffer.byteLength(canonical(payload(rows))) <= MAX_PAYLOAD_BYTES;
  const emit = rows => { const value = payload(rows); packets.push({ id: 'history-' + digest(canonical(value)), content_type: 'history_sync', payload: value }); };
  let pending = [];
  records.forEach((record, index) => {
    if (index) insist(record.seq === records[index - 1].seq + 1, 'INVALID_INPUT', 'History input must be contiguous');
    if (fits([record])) {
      if (pending.length && !fits([...pending, record])) { emit(pending); pending = []; }
      pending.push(record); return;
    }
    if (pending.length) { emit(pending); pending = []; }
    const chars = Array.from(record.text), completeSha = digest(record.text), parts = []; let start = 0;
    while (start < chars.length) {
      let lo = start + 1, hi = chars.length, found = start;
      while (lo <= hi) {
        const end = Math.floor((lo + hi) / 2), fragment = { ...record, text: chars.slice(start, end).join(''), fragment: { index: 255, count: 256, sha256: completeSha } };
        if (fits([fragment])) { found = end; lo = end + 1; } else hi = end - 1;
      }
      insist(found > start, 'PAYLOAD_TOO_LARGE', 'Source metadata cannot fit in a history packet');
      parts.push(chars.slice(start, found).join('')); start = found;
      insist(parts.length <= 256, 'PAYLOAD_TOO_LARGE', 'Source record exceeds the supported fragment count');
    }
    parts.forEach((value, index) => emit([{ ...record, text: value, fragment: { index, count: parts.length, sha256: completeSha } }]));
  });
  if (pending.length) emit(pending);
  return packets;
}
