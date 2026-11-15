import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { createBridgeServer } from '../src/server.mjs';
import { BridgeClient } from '../src/client.mjs';
import { ADMIN, fixture, history, message, ack } from './helpers.mjs';

async function host(t) {
  const f = fixture(), server = createBridgeServer({ core: f.core, adminToken: ADMIN, port: 0, logger: { error: () => {} } });
  const base = await server.listen(); t.after(async () => { await server.close(); f.core.close(); });
  return { ...f, base, server, admin: new BridgeClient(base, { adminToken: ADMIN }) };
}

test('settings UI assets are local and credentials never appear in endpoint metadata', async t => {
  const f = await host(t);
  const page = await fetch(f.base); assert.equal(page.status, 200); assert.match(page.headers.get('content-security-policy'), /script-src 'self'/);
  const html = await page.text(); assert.match(html, /同步内容/); assert.match(html, /即时通讯/); assert.match(html, /暂停本端自动推送/);
  assert.match(html, /name="receive_history" type="checkbox"/);
  assert.equal((await fetch(f.base + '/app.js')).status, 200); assert.equal((await fetch(f.base + '/style.css')).status, 200);
  assert.equal((await fetch(f.base + '/api/endpoints')).status, 401);
  const list = await f.admin.request('/api/endpoints', { admin: true });
  assert.equal(list.length, 3); assert.ok(list.every(e => !JSON.stringify(e).includes('credential_hash')));
  assert.ok(list.every(e => e.capabilities.can_wake === false));
});

test('both harness policies can be independently changed through the shared admin settings API', async t => {
  const f = await host(t);
  await f.admin.request('/api/endpoints/codex-a/settings', { method: 'PATCH', admin: true, body: { history_sync: 'idle_wake', paused: true, receive_history: false } });
  await f.admin.request('/api/endpoints/dsh-b/settings', { method: 'PATCH', admin: true, body: { message: 'next_turn' } });
  const list = await f.admin.request('/api/endpoints', { admin: true });
  assert.equal(list.find(e => e.id === 'codex-a').settings.paused, true);
  assert.equal(list.find(e => e.id === 'codex-a').settings.history_sync, 'idle_wake');
  assert.equal(list.find(e => e.id === 'codex-a').settings.receive_history, false);
  assert.equal(list.find(e => e.id === 'dsh-b').settings.paused, false);
  assert.equal(list.find(e => e.id === 'dsh-b').settings.receive_history, undefined);
  assert.equal(list.find(e => e.id === 'dsh-b').settings.message, 'next_turn');
  const forbidden = await fetch(f.base + '/api/endpoints/dsh-b/settings', { method: 'PATCH', headers: { Authorization: 'Bearer incorrect', 'Content-Type': 'application/json' }, body: '{}' });
  assert.equal(forbidden.status, 401);
});

test('real HTTP clients publish and acknowledge both content types through one interface', async t => {
  const f = await host(t), a = new BridgeClient(f.base), b = new BridgeClient(f.base);
  await a.connect(f.a.id, f.a.credential); await b.connect(f.b.id, f.b.credential);
  const h = await a.publish(history()), m = await a.publish(message());
  const batch = await b.receive(); assert.deepEqual(batch.items.map(x => x.id), [h.id, m.id]);
  assert.equal((await a.status(h.id)).delivery, 'queued');
  const receipt = await b.acknowledge(batch.items.map(item => ack(item, { outcome: 'completed' })));
  assert.ok(receipt.every(x => x.ok)); assert.equal((await a.status(m.id)).outcome, 'completed');
  const cursor = await b.request('/api/cursor?archive_id=archive-a&generation=g1'); assert.equal(cursor.read_through, 0);
  assert.ok((await b.heartbeat()).lease_until > f.clock.now);
});

test('foreign origins and hosts are rejected even when a valid local credential is supplied', async t => {
  const f = await host(t);
  const origin = await fetch(f.base + '/api/endpoints', { headers: { Authorization: `Bearer ${ADMIN}`, Origin: 'https://unrelated.example' } });
  assert.equal(origin.status, 403);
  const hostStatus = await new Promise((resolve, reject) => {
    const request = http.request(f.base + '/api/endpoints', { headers: { Host: 'unrelated.example', Authorization: `Bearer ${ADMIN}` } }, response => { response.resume(); resolve(response.statusCode); });
    request.on('error', reject); request.end();
  });
  assert.equal(hostStatus, 403);
});

test('malformed JSON, wrong media types, oversized requests and unknown routes have explicit failures', async t => {
  const f = await host(t), headers = { Authorization: `Bearer ${ADMIN}`, 'Content-Type': 'application/json' };
  assert.equal((await fetch(f.base + '/api/endpoints', { method: 'POST', headers, body: '{' })).status, 400);
  assert.equal((await fetch(f.base + '/api/endpoints', { method: 'POST', headers: { Authorization: `Bearer ${ADMIN}`, 'Content-Type': 'text/plain' }, body: '{}' })).status, 400);
  assert.equal((await fetch(f.base + '/api/endpoints', { method: 'POST', headers, body: JSON.stringify({ text: 'a'.repeat(20_000) }) })).status, 413);
  assert.equal((await fetch(f.base + '/api/unknown')).status, 404);
  assert.equal((await fetch(f.base + '/src/core.mjs')).status, 404);
});

test('session identity headers cannot borrow another endpoint connection token', async t => {
  const f = await host(t), a = new BridgeClient(f.base), b = new BridgeClient(f.base);
  await a.connect(f.a.id, f.a.credential); await b.connect(f.b.id, f.b.credential);
  const response = await fetch(f.base + '/api/receive', { method: 'POST', headers: { Authorization: `Bearer ${a.connection.token}`, 'X-Bridge-Endpoint': b.connection.endpoint_id, 'X-Bridge-Epoch': String(b.connection.epoch), 'Content-Type': 'application/json' }, body: '{}' });
  assert.equal(response.status, 401);
});
