import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { createHash, timingSafeEqual } from 'node:crypto';
import { BridgeError, PROTOCOL } from './protocol.mjs';

const LIMIT = 16_384;
const assets = new Map([
  ['/', ['index.html', 'text/html; charset=utf-8']], ['/app.js', ['app.js', 'text/javascript; charset=utf-8']],
  ['/style.css', ['style.css', 'text/css; charset=utf-8']],
]);
const hash = text => createHash('sha256').update(text).digest();
const bearer = request => /^Bearer ([A-Za-z0-9_-]+)$/.exec(request.headers.authorization ?? '')?.[1];
const connection = request => ({ endpoint_id: request.headers['x-bridge-endpoint'], epoch: Number(request.headers['x-bridge-epoch']), token: bearer(request) });
const statusCode = code => code === 'NOT_FOUND' ? 404 : ['UNAUTHORIZED', 'STALE_CONNECTION'].includes(code) ? 401 : ['NOT_AUTHORIZED', 'WAKE_NOT_AUTHORIZED'].includes(code) ? 403 : code === 'PAYLOAD_TOO_LARGE' ? 413 : ['INVALID_INPUT', 'UNSUPPORTED_PROTOCOL', 'BUDGET_TOO_SMALL'].includes(code) ? 400 : 409;
function json(response, status, value) {
  response.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff' });
  response.end(JSON.stringify(value));
}
function body(request) {
  if (!String(request.headers['content-type'] ?? '').toLowerCase().startsWith('application/json')) return Promise.reject(new BridgeError('INVALID_INPUT', 'Expected application/json'));
  if (Number(request.headers['content-length'] ?? 0) > LIMIT) { request.resume(); return Promise.reject(new BridgeError('PAYLOAD_TOO_LARGE', 'Request body is too large')); }
  return new Promise((resolve, reject) => {
    const parts = []; let size = 0, failed = false;
    request.on('data', chunk => { if (failed) return; size += chunk.length; if (size > LIMIT) { failed = true; reject(new BridgeError('PAYLOAD_TOO_LARGE', 'Request body is too large')); } else parts.push(chunk); });
    request.on('end', () => { if (failed) return; try { resolve(JSON.parse(Buffer.concat(parts).toString('utf8'))); } catch { reject(new BridgeError('INVALID_INPUT', 'Malformed JSON')); } });
    request.on('error', reject);
  });
}

export function createBridgeServer({ core, adminToken, indexer, codex, dsh, port = 4776, logger = console }) {
  const administrator = request => {
    const value = bearer(request);
    if (!value || !timingSafeEqual(hash(value), hash(adminToken))) throw new BridgeError('UNAUTHORIZED', 'Administrator authentication required');
    return value;
  };
  const server = http.createServer(async (request, response) => {
    try {
      const address = server.address();
      const allowed = new Set([`127.0.0.1:${address.port}`, `localhost:${address.port}`, `[::1]:${address.port}`]);
      if (!allowed.has(request.headers.host)) { json(response, 403, { ok: false, error: 'INVALID_HOST' }); return; }
      if (request.headers.origin && ![...allowed].some(host => request.headers.origin === `http://${host}`)) { json(response, 403, { ok: false, error: 'INVALID_ORIGIN' }); return; }
      const url = new URL(request.url, 'http://' + request.headers.host), path = url.pathname;
      if (request.method === 'GET' && assets.has(path)) {
        const [name, type] = assets.get(path), data = await readFile(new URL('../web/' + name, import.meta.url));
        response.writeHead(200, { 'Content-Type': type, 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff',
          'Content-Security-Policy': "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'" });
        response.end(data); return;
      }
      if (request.method === 'GET' && path === '/api/health') { json(response, 200, { ok: true, name: 'local-bridge', protocol: PROTOCOL,
        features: { codex_hooks: Boolean(codex), codex_approval_redemption: Boolean(codex), dsh_host: Boolean(dsh), provided_endpoint_credentials: true, history_indexing: Boolean(indexer) } }); return; }
      let result;
      if (request.method === 'POST' && ['/api/codex/host-event', '/api/codex/exchange', '/api/codex/tool-call', '/api/codex/heartbeat'].includes(path)) {
        if (!codex) throw new BridgeError('CAPABILITY_MISSING', 'Codex gateway is not configured');
        const input = await body(request);
        if (path.endsWith('host-event')) result = await codex.hostEvent(bearer(request), input);
        else if (path.endsWith('exchange')) result = codex.exchange(bearer(request), input);
        else if (path.endsWith('heartbeat')) result = codex.hostHeartbeat(bearer(request));
        else { const value = await codex.call(bearer(request), input); result = codex.toolTransportResult(bearer(request), value); }
      }
      else if (request.method === 'POST' && path === '/api/codex/history-sync') {
        if (!codex) throw new BridgeError('CAPABILITY_MISSING', 'Codex host channel is not configured');
        const ticket = bearer(request);
        const value = await codex.historySyncCall(ticket, await body(request));
        result = codex.toolTransportResult(ticket, value);
      }
      else if (request.method === 'POST' && ['/api/dsh/pulse', '/api/dsh/tool-call', '/api/dsh/host-result'].includes(path)) {
        if (!dsh) throw new BridgeError('CAPABILITY_MISSING', 'DSH host channel is not configured');
        const input = await body(request);
        if (path.endsWith('pulse')) result = await dsh.pulse(bearer(request), input);
        else if (path.endsWith('tool-call')) result = await dsh.call(bearer(request), input);
        else result = dsh.result(bearer(request), input);
      }
      else if (request.method === 'GET' && path === '/api/endpoints') result = core.listEndpoints(administrator(request));
      else if (request.method === 'POST' && path === '/api/endpoints') result = core.register(administrator(request), await body(request));
      else if (request.method === 'PATCH' && /^\/api\/endpoints\/[^/]+\/settings$/.test(path)) result = core.setSettings(administrator(request), decodeURIComponent(path.split('/')[3]), await body(request));
      else if (request.method === 'PATCH' && /^\/api\/endpoints\/[^/]+\/capabilities$/.test(path)) result = core.setCapabilities(administrator(request), decodeURIComponent(path.split('/')[3]), await body(request));
      else if (request.method === 'POST' && path === '/api/grants') result = core.grant(administrator(request), await body(request));
      else if (request.method === 'POST' && /^\/api\/grants\/[^/]+\/revoke$/.test(path)) { result = core.revoke(administrator(request), decodeURIComponent(path.split('/')[3])) ?? { revoked: true }; }
      else if (request.method === 'POST' && path === '/api/archives') result = core.registerArchive(administrator(request), await body(request));
      else if (request.method === 'POST' && path === '/api/sources') result = core.attachSource(administrator(request), await body(request));
      else if (request.method === 'POST' && path === '/api/history-subscriptions') result = core.subscribeHistory(administrator(request), await body(request));
      else if (request.method === 'GET' && /^\/api\/sources\/[^/]+$/.test(path)) result = core.sourceState(administrator(request), decodeURIComponent(path.split('/')[3]));
      else if (request.method === 'POST' && /^\/api\/sources\/[^/]+\/scan$/.test(path)) {
        administrator(request);
        if (!indexer) throw new BridgeError('CAPABILITY_MISSING', 'Source indexer is not configured');
        result = await indexer.scan(decodeURIComponent(path.split('/')[3]), await body(request));
      }
      else if (request.method === 'POST' && path === '/api/connect') {
        const data = await body(request);
        result = core.connect(data.endpoint_id, bearer(request), { requireOffline: data.require_offline });
      }
      else if (request.method === 'POST' && path === '/api/disconnect') result = core.disconnect(connection(request));
      else if (request.method === 'GET' && path === '/api/session') result = core.sessionInfo(connection(request));
      else if (request.method === 'POST' && path === '/api/heartbeat') result = core.heartbeat(connection(request));
      else if (request.method === 'POST' && path === '/api/publish') result = core.publish(connection(request), await body(request));
      else if (request.method === 'POST' && path === '/api/receive') result = core.receive(connection(request), await body(request));
      else if (request.method === 'POST' && path === '/api/ack') result = core.acknowledge(connection(request), await body(request));
      else if (request.method === 'GET' && /^\/api\/messages\/[^/]+$/.test(path)) result = core.status(connection(request), decodeURIComponent(path.split('/')[3]));
      else if (request.method === 'GET' && path === '/api/cursor') {
        const archive = url.searchParams.get('archive_id'), generation = url.searchParams.get('generation'), requestId = url.searchParams.get('request_id');
        result = requestId === null ? { read_through: core.historyCursor(connection(request), archive, generation) }
          : core.historySyncStatus(connection(request), archive, generation, requestId);
      }
      else { json(response, 404, { ok: false, error: 'NOT_FOUND' }); return; }
      json(response, 200, { ok: true, result });
    } catch (error) {
      if (error instanceof BridgeError) json(response, statusCode(error.code), { ok: false, error: error.code, message: error.message });
      else { logger.error?.('Local bridge request failed', error.code ?? error.name); json(response, 500, { ok: false, error: 'INTERNAL_ERROR' }); }
    }
  });
  server.requestTimeout = 10_000; server.headersTimeout = 10_000;
  return {
    server,
    async listen() {
      await new Promise((resolve, reject) => { server.once('error', reject); server.listen(port, '127.0.0.1', () => { server.off('error', reject); resolve(); }); });
      return `http://127.0.0.1:${server.address().port}`;
    },
    async close() { if (!server.listening) return; await new Promise((resolve, reject) => { server.close(e => e ? reject(e) : resolve()); server.closeAllConnections(); }); },
  };
}
