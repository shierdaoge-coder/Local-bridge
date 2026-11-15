import { once } from 'node:events';
import { insist } from './protocol.mjs';

// Explicitly scoped to MCP 2025-11-25 tools over stdio; no sampling or background tasks.
export const MCP_VERSION = '2025-11-25';
const failure = (id, code, message) => ({ jsonrpc: '2.0', id, error: { code, message } });
const response = (id, result) => ({ jsonrpc: '2.0', id, result });
const record = value => value !== null && typeof value === 'object' && !Array.isArray(value);

/** Host context is an out-of-band callback. Request _meta and startup env are not identities. */
export class McpToolSession {
  constructor({ bridge, getExecutionContext = () => undefined }) {
    insist(bridge && typeof bridge.execute === 'function' && typeof bridge.definitions === 'function', 'INVALID_INPUT', 'Session-bound tool bridge required');
    insist(typeof getExecutionContext === 'function', 'INVALID_INPUT', 'Host context provider must be callable');
    this.bridge = bridge; this.getExecutionContext = getExecutionContext; this.phase = 'new';
    this.names = new Set(bridge.definitions().map(t => t.name));
  }
  async handle(request) {
    if (!record(request) || request.jsonrpc !== '2.0' || typeof request.method !== 'string') return failure(null, -32600, 'Invalid JSON-RPC request');
    const hasId = Object.hasOwn(request, 'id'), id = request.id;
    if (hasId && !(typeof id === 'string' || Number.isSafeInteger(id))) return failure(null, -32600, 'Invalid request ID');
    if (request.params !== undefined && !record(request.params)) return hasId ? failure(id, -32602, 'Parameters must be an object') : null;
    const params = request.params ?? {};
    if (!hasId) {
      if (request.method === 'notifications/initialized' && this.phase === 'negotiated') this.phase = 'ready';
      return null;
    }
    if (request.method === 'ping') return response(id, {});
    if (request.method === 'initialize') {
      if (this.phase !== 'new') return failure(id, -32600, 'Already initialized');
      if (typeof params.protocolVersion !== 'string' || !record(params.capabilities) || !record(params.clientInfo) || typeof params.clientInfo.name !== 'string' || typeof params.clientInfo.version !== 'string') return failure(id, -32602, 'Incomplete initialization');
      this.phase = 'negotiated';
      return response(id, { protocolVersion: MCP_VERSION, capabilities: { tools: { listChanged: false } }, serverInfo: { name: 'local-bridge', version: '0.1.0-dev' } });
    }
    if (this.phase !== 'ready') return failure(id, -32600, 'Initialize and send notifications/initialized first');
    if (request.method === 'tools/list') {
      if (params.cursor !== undefined) return failure(id, -32602, 'This fixed tool list has no continuation cursor');
      return response(id, { tools: this.bridge.definitions() });
    }
    if (request.method !== 'tools/call') return failure(id, -32601, 'Method not found');
    if (!this.names.has(params.name) || (params.arguments !== undefined && !record(params.arguments))) return failure(id, -32602, 'Unknown tool or invalid arguments');
    try {
      // Deliberately pass no client-controlled parameters or metadata to the host provider.
      const execution = await this.getExecutionContext();
      const result = await this.bridge.execute(params.name, params.arguments ?? {}, execution);
      return response(id, { content: [{ type: 'text', text: JSON.stringify(result) }], isError: false });
    } catch (error) {
      if (error.code === 'INVALID_INPUT') return failure(id, -32602, error.message);
      const value = { ok: false, error: error.code ?? 'TOOL_FAILED', message: error.code ? error.message : 'Bridge tool failed' };
      return response(id, { content: [{ type: 'text', text: JSON.stringify(value) }], isError: true });
    }
  }
}

/** A bounded newline transport. The embedding host supplies input/output; no app config edits. */
export async function serveMcpStdio({ input, output, session, maxFrameBytes = 16_384 }) {
  insist(Number.isSafeInteger(maxFrameBytes) && maxFrameBytes > 0 && maxFrameBytes <= 1_048_576, 'INVALID_INPUT', 'Invalid frame budget');
  let tail = Buffer.alloc(0);
  const write = async value => { if (value && !output.write(JSON.stringify(value) + '\n')) await once(output, 'drain'); };
  const decoder = new TextDecoder('utf-8', { fatal: true });
  for await (const chunk of input) {
    const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    let start = 0, newline;
    while ((newline = bytes.indexOf(10, start)) !== -1) {
      const length = tail.length + newline - start;
      if (length > maxFrameBytes) { await write(failure(null, -32600, 'MCP frame exceeds local limit')); return; }
      const line = Buffer.concat([tail, bytes.subarray(start, newline)]); tail = Buffer.alloc(0); start = newline + 1;
      let request;
      try { request = JSON.parse(decoder.decode(line)); }
      catch { await write(failure(null, -32700, 'Invalid UTF-8 JSON frame')); continue; }
      await write(await session.handle(request));
    }
    if (tail.length + bytes.length - start > maxFrameBytes) { await write(failure(null, -32600, 'MCP frame exceeds local limit')); return; }
    tail = Buffer.concat([tail, bytes.subarray(start)]);
  }
  if (tail.length) await write(failure(null, -32700, 'Incomplete final MCP frame'));
}
