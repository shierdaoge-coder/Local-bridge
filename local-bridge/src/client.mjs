export class BridgeClient {
  constructor(base, { adminToken, connection, timeoutMs = 5000 } = {}) {
    const url = new URL(base);
    if (url.protocol !== 'http:' || !['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname)) throw new Error('Bridge service must be a loopback HTTP URL');
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 30_000) throw new Error('Invalid local request timeout');
    this.base = url.origin; this.adminToken = adminToken; this.connection = connection; this.timeoutMs = timeoutMs;
  }
  async request(path, { method = 'GET', body, admin = false, credential } = {}) {
    const token = credential ?? (admin ? this.adminToken : this.connection?.token);
    const headers = { Authorization: `Bearer ${token}` };
    if (this.connection && !admin && !credential) { headers['X-Bridge-Endpoint'] = this.connection.endpoint_id; headers['X-Bridge-Epoch'] = String(this.connection.epoch); }
    if (body !== undefined) headers['Content-Type'] = 'application/json';
    const response = await fetch(this.base + path, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(this.timeoutMs) });
    const value = await response.json();
    if (!response.ok || !value.ok) { const error = new Error(value.message || value.error); error.code = value.error; error.status = response.status; throw error; }
    return path === '/api/health' ? value : value.result;
  }
  async connect(endpointId, credential, { requireOffline = false } = {}) {
    const body = { endpoint_id: endpointId, ...(requireOffline ? { require_offline: true } : {}) };
    this.connection = await this.request('/api/connect', { method: 'POST', body, credential });
    return this.connection;
  }
  publish(body) { return this.request('/api/publish', { method: 'POST', body }); }
  receive(body = {}) { return this.request('/api/receive', { method: 'POST', body }); }
  acknowledge(body) { return this.request('/api/ack', { method: 'POST', body }); }
  heartbeat() { return this.request('/api/heartbeat', { method: 'POST' }); }
  sessionInfo() { return this.request('/api/session'); }
  status(id) { return this.request('/api/messages/' + encodeURIComponent(id)); }
}
