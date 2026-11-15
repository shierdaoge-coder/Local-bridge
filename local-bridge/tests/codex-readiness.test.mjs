import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve, dirname, basename } from 'node:path';
import { readiness } from '../scripts/check-codex-readiness.mjs';
import { build } from '../scripts/build-codex.mjs';
import { fixture, ADMIN } from './helpers.mjs';
import { CodexGateway } from '../src/codex-gateway.mjs';
import { createBridgeServer } from '../src/server.mjs';

test('readiness validates fresh temporary package paths, service, and complete private bindings without exposing secrets', async t => {
  const tempRoot = mkdtempSync(join(tmpdir(), 'codex-readiness-package-'));
  t.after(() => { assert.equal(dirname(tempRoot), resolve(tmpdir())); assert.ok(basename(tempRoot).startsWith('codex-readiness-package-')); rmSync(tempRoot, { recursive: true, force: true }); });
  const f = fixture(), gateway = new CodexGateway({ core: f.core });
  const server = createBridgeServer({ core: f.core, adminToken: ADMIN, codex: gateway, port: 0 });
  const url = await server.listen(); t.after(async () => { await server.close(); f.core.close(); });
  const data = mkdtempSync(join(tmpdir(), 'codex-readiness-data-'));
  t.after(() => { assert.equal(dirname(data), resolve(tmpdir())); assert.ok(basename(data).startsWith('codex-readiness-data-')); rmSync(data, { recursive: true, force: true }); });
  const configPath = join(data, 'bridge.json');
  const marketplace = await build(tempRoot, { pluginData: data });
  const binding = { endpoint_id: 'codex-test', credential: 'never-print-this', cwd: resolve(tmpdir()), transcript_path: join(tmpdir(), 'session.jsonl') };
  writeFileSync(configPath, JSON.stringify({ service_url: url, bindings: { one: binding, two: {} } }));
  let result = await readiness({ marketplace, pluginData: data, serviceUrl: url });
  assert.equal(result.ready, false);
  assert.equal(result.native_validation, 'pending-manual-test');
  assert.match(result.checks.find(c => c.name === 'private_bindings').detail, /2 incomplete or invalid/);
  assert.doesNotMatch(JSON.stringify(result), /never-print-this/);

  writeFileSync(configPath, JSON.stringify({ service_url: url, bindings: { '12345678-1234-1234-1234-123456789abc': binding } }));
  result = await readiness({ marketplace, pluginData: data, serviceUrl: url });
  assert.equal(result.ready, true); assert.equal(result.read_only, true);
  assert.equal(result.starts_model, false); assert.equal(result.installs_plugin, false);
  assert.equal(result.checks.find(c => c.name === 'source_freshness').status, 'ok');
  assert.equal(result.checks.find(c => c.name === 'startup_paths').status, 'ok');
  assert.doesNotMatch(JSON.stringify(result), /never-print-this/);

  writeFileSync(join(marketplace, 'plugins', 'local-bridge', 'src', 'codex-hook.mjs'), 'stale');
  result = await readiness({ marketplace, pluginData: data, serviceUrl: url });
  assert.equal(result.checks.find(c => c.name === 'source_freshness').status, 'needs_attention');
});

test('readiness reports unavailable service, absent PLUGIN_DATA, and absent selected marketplace', async () => {
  const result = await readiness({ marketplace: join(tmpdir(), 'missing-codex-marketplace'), pluginData: undefined, serviceUrl: 'http://127.0.0.1:1' });
  assert.equal(result.ready, false);
  assert.equal(result.checks.find(c => c.name === 'local_package').status, 'needs_attention');
  assert.equal(result.checks.find(c => c.name === 'service_health').status, 'needs_attention');
  assert.match(result.checks.find(c => c.name === 'private_bindings').detail, /PLUGIN_DATA/);
});
