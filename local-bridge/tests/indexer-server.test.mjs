import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { join, dirname, basename, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { fixture, ADMIN } from './helpers.mjs';
import { BridgeClient } from '../src/client.mjs';
import { createBridgeServer } from '../src/server.mjs';
import { SourceIndexer } from '../src/indexer.mjs';

async function setup(t) {
  const f = fixture(), directory = mkdtempSync(join(tmpdir(), 'local-bridge-index-http-')), path = join(directory, 'source.jsonl');
  writeFileSync(path, JSON.stringify({ type: 'response_item', payload: { type: 'message', role: 'user', content: [{ type: 'input_text', text: 'Only the operator selected this path.' }] } }) + '\n');
  const indexer = new SourceIndexer({ core: f.core, adminToken: ADMIN }), service = createBridgeServer({ core: f.core, adminToken: ADMIN, indexer, port: 0 });
  const base = await service.listen(), admin = new BridgeClient(base, { adminToken: ADMIN }), endpoint = new BridgeClient(base, { connection: f.ca });
  t.after(async () => { await service.close(); f.core.close(); assert.equal(dirname(directory), resolve(tmpdir())); assert.ok(basename(directory).startsWith('local-bridge-index-http-')); rmSync(directory, { recursive: true, force: true }); });
  return { ...f, path, admin, endpoint };
}

test('operator HTTP source registration, subscription and explicit scan recover idempotently', async t => {
  const f = await setup(t);
  const source = await f.admin.request('/api/sources', { method: 'POST', admin: true, body: { archive_id: 'archive-a', path: f.path, format: 'codex-jsonl' } });
  assert.equal(source.checkpoint, null);
  await f.admin.request('/api/history-subscriptions', { method: 'POST', admin: true, body: { id: 'http-subscription', archive_id: 'archive-a', to: 'dsh-b', grant_id: 'a-b' } });
  const scan = () => f.admin.request('/api/sources/archive-a/scan', { method: 'POST', admin: true, body: { maxPages: 5 } });
  assert.equal((await scan()).records, 1); assert.equal((await scan()).records, 0);
  const state = await f.admin.request('/api/sources/archive-a', { admin: true }); assert.equal(state.checkpoint.visible_next, 1);
  assert.equal(f.core.receive(f.cb).items[0].payload.records[0].text, 'Only the operator selected this path.');
  assert.equal((await f.endpoint.sessionInfo()).native_session_id, 'native-a');
});

test('endpoint connection cannot inspect paths, attach a source, subscribe or scan archives', async t => {
  const f = await setup(t);
  const requests = [
    ['/api/sources', { method: 'POST', body: { archive_id: 'archive-a', path: f.path, format: 'codex-jsonl' } }],
    ['/api/sources/archive-a', {}],
    ['/api/history-subscriptions', { method: 'POST', body: { id: 'forged', archive_id: 'archive-a', to: 'dsh-b', grant_id: 'a-b' } }],
    ['/api/sources/archive-a/scan', { method: 'POST', body: {} }],
  ];
  for (const [path, options] of requests) await assert.rejects(f.endpoint.request(path, options), error => error.code === 'UNAUTHORIZED');
  assert.equal(f.core.db.prepare('SELECT COUNT(*) AS n FROM archive_sources').get().n, 0);
});
