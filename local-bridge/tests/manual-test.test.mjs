import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readdir, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname, basename, resolve } from 'node:path';
import { manualTest } from '../scripts/manual-test.mjs';
import { SourceIndexer } from '../src/indexer.mjs';
import { createBridgeServer } from '../src/server.mjs';
import { BridgeClient } from '../src/client.mjs';
import { fixture, ADMIN, ack, code } from './helpers.mjs';

async function environment(t) {
  const runtime = await mkdtemp(join(tmpdir(), 'local-bridge-manual-plan-')), f = fixture(), indexer = new SourceIndexer({ core: f.core, adminToken: ADMIN });
  const service = createBridgeServer({ core: f.core, adminToken: ADMIN, indexer, port: 0 }), client = new BridgeClient(await service.listen(), { adminToken: ADMIN });
  t.after(async () => { await service.close(); f.core.close(); assert.equal(dirname(resolve(runtime)), resolve(tmpdir())); assert.ok(basename(runtime).startsWith('local-bridge-manual-plan-')); await rm(runtime, { recursive: true, force: true }); });
  return { ...f, runtime, client, options: { phase: 'pair', run: 'fixture-1', codex: f.a.id, dsh: f.b.id } };
}

test('manual test defaults to a no-write plan and rejects ambiguous or unsafe selections', async t => {
  const f = await environment(t), dependencies = { client: f.client, runtime: f.runtime };
  const plan = await manualTest(f.options, dependencies);
  assert.equal(plan.writes, false); assert.equal(plan.starts_model, false); assert.equal(plan.idle_wake_enabled, false);
  assert.equal(plan.operations.length, 6); assert.deepEqual(await readdir(f.runtime), []);
  assert.equal(f.core.db.prepare('SELECT COUNT(*) AS n FROM grants').get().n, 2);
  await assert.rejects(manualTest({ ...f.options, run: '../escape' }, dependencies), code('INVALID_INPUT'));
  await assert.rejects(manualTest({ ...f.options, source: '../escape' }, dependencies), code('INVALID_INPUT'));
  await assert.rejects(manualTest({ ...f.options, dsh: f.a.id }, dependencies), code('INVALID_INPUT'));
});

test('operator pairing enables natural turns only, seeds labelled history, and can revoke/pause the test', async t => {
  const f = await environment(t), dep = { client: f.client, runtime: f.runtime };
  const result = await manualTest({ ...f.options, write: true }, dep); assert.equal(result.state, 'complete');
  for (const e of [f.a, f.b]) {
    const state = f.core.endpoint(e.id); assert.equal(state.capabilities.can_wake, false); assert.equal(state.capabilities.can_enqueue, true);
    assert.equal(state.settings.message, 'next_turn');
  }
  const history = await manualTest({ ...f.options, phase: 'history', source: 'codex', write: true }, dep);
  assert.equal(history.state, 'complete');
  const received = f.core.receive(f.cb); assert.equal(received.items.length, 1);
  assert.equal(received.items[0].content_type, 'history_sync'); assert.match(received.items[0].payload.records[0].text, /SYNTHETIC TRANSPORT FIXTURE/);
  assert.equal(received.items[0].payload.records.length, 2);
  f.core.acknowledge(f.cb, received.items.map(i => ack(i)));
  assert.equal(f.core.historyCursor(f.cb, 'manual-fixture-1-fixture-codex', 'fixture-v1'), 1);
  await assert.rejects(manualTest({ ...f.options, write: true }, dep), code('CONFIG_CONFLICT'));
  await manualTest({ ...f.options, phase: 'stop', write: true }, dep);
  assert.equal(f.core.endpoint(f.a.id).settings.paused, true); assert.equal(f.core.endpoint(f.b.id).settings.paused, true);
  assert.equal(f.core.db.prepare('SELECT revoked FROM grants WHERE id=?').get('manual-fixture-1-codex-dsh').revoked, 1);
});

test('unknown operator mutation result retains a receipt instead of retrying', async t => {
  const f = await environment(t); let writes = 0;
  const client = { request: async (path, options) => { if (path === '/api/endpoints') return f.core.listEndpoints(ADMIN); writes++; if (writes === 1) return { id: 'first-grant' }; throw Error('response lost'); } };
  await assert.rejects(manualTest({ ...f.options, write: true }, { client, runtime: f.runtime }), code('MANUAL_RECONCILIATION_REQUIRED'));
  assert.equal(writes, 2);
  const receipt = JSON.parse(await readFile(join(f.runtime, f.options.run, 'pair.json'), 'utf8'));
  assert.equal(receipt.state, 'partial_or_uncertain'); assert.equal(receipt.completed.length, 1);
});
