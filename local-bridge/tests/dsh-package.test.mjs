import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, writeFile, rm, realpath, readdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { buildDsh, DSH_PACKAGE } from '../scripts/build-dsh.mjs';
import { installDsh } from '../scripts/install-dsh.mjs';
import { provisionDsh } from '../src/dsh-setup.mjs';
import { fixture, ADMIN, code } from './helpers.mjs';
import { DshGateway } from '../src/dsh-gateway.mjs';
import { createBridgeServer } from '../src/server.mjs';
import { BridgeClient } from '../src/client.mjs';

async function directory(t) {
  const dir = await mkdtemp(path.join(tmpdir(), 'local-bridge-dsh-prep-'));
  t.after(async () => { assert.equal(path.dirname(path.resolve(dir)), path.resolve(tmpdir())); assert.ok(path.basename(dir).startsWith('local-bridge-dsh-prep-')); await rm(dir, { recursive: true, force: true }); });
  return dir;
}

test('DSH development package is reproducible, small, and refuses conflicting content', async t => {
  const dir = await directory(t), root = path.join(dir, 'bundle');
  const options = { sdkEntry: path.join(dir, 'fixture-sdk', 'index.js') };
  const built = await buildDsh(root, options); await buildDsh(root, options);
  const manifest = JSON.parse(await readFile(path.join(root, 'package.json'), 'utf8'));
  assert.equal(manifest.name, DSH_PACKAGE); assert.equal(manifest.dsh.bundle.patch, './cordis.patch.yml');
  const patch = await readFile(path.join(root, 'cordis.patch.yml'), 'utf8');
  assert.ok(patch.includes(JSON.stringify(path.join(root, 'PLUGIN_DATA/bridge.json'))));
  assert.ok(patch.includes(JSON.stringify(options.sdkEntry)));
  assert.ok(built.files < 14); assert.deepEqual((await readdir(root)).sort(), ['PLUGIN_DATA', 'README.md', 'cordis.patch.yml', 'package.json', 'src'].sort());
  assert.deepEqual(await readFile(path.join(root, 'src/history-sync.py')), await readFile(new URL('../src/history-sync.py', import.meta.url)));
  assert.deepEqual(await readFile(path.join(root, 'src/dsh-plugin.mjs')), await readFile(new URL('../src/dsh-plugin.mjs', import.meta.url)));
  await writeFile(path.join(root, 'src/dsh-plugin.mjs'), 'operator changed this');
  await assert.rejects(buildDsh(root, options), /BUILD_CONFLICT/);
  assert.equal(await readFile(path.join(root, 'src/dsh-plugin.mjs'), 'utf8'), 'operator changed this');
  await assert.rejects(buildDsh('relative'), /absolute/);
  await assert.rejects(buildDsh(root, { sdkEntry: 'relative-sdk' }), /absolute installed DSH SDK/);
});

test('DSH installer is dry by default and changes only a selected fixture profile with backups', async t => {
  const dir = await directory(t), source = path.join(dir, 'source'), profile = path.join(dir, 'profile');
  const options = { sdkEntry: path.join(dir, 'fixture-sdk', 'index.js') };
  await buildDsh(source, options); await mkdir(profile);
  const manifestPath = path.join(profile, 'package.json'), lock = path.join(profile, 'pnpm-lock.yaml');
  const original = JSON.stringify({ name: 'isolated-profile', dependencies: { keep: '1.0.0' }, dsh: { profile: { bundles: ['keep'] } } }, null, 2);
  await writeFile(manifestPath, original); await writeFile(lock, 'lockfileVersion: 9.0\nimporters:\n  .:\n    dependencies:\n      keep:\n        specifier: 1.0.0\n        version: 1.0.0\n');
  await writeFile(path.join(profile, 'cordis.patch.yml'), 'operator patch');
  const plan = await installDsh({ source, profile }); assert.equal(plan.writes, false);
  assert.equal(await readFile(manifestPath, 'utf8'), original); assert.equal((await readdir(profile)).includes('node_modules'), false);
  const result = await installDsh({ source, profile, write: true, backupRoot: path.join(dir, 'backups') });
  assert.equal(result.installed, true); assert.equal(await readFile(path.join(result.backup, 'package.json'), 'utf8'), original);
  const after = JSON.parse(await readFile(manifestPath, 'utf8')); assert.deepEqual(after.dsh.profile.bundles, ['keep', DSH_PACKAGE]);
  assert.equal(after.dependencies.keep, '1.0.0'); assert.equal(await realpath(result.target), await realpath(source));
  assert.equal(await readFile(path.join(profile, 'cordis.patch.yml'), 'utf8'), 'operator patch');
  await installDsh({ source, profile });
  const second = path.join(dir, 'another-bundle'); await buildDsh(second, options);
  await assert.rejects(installDsh({ source: second, profile, write: true }), /different source/);
});

test('DSH provisioning is dry by default, preserves secret recovery and does not create a grant', async t => {
  const dir = await directory(t), pluginData = path.join(dir, 'private'), f = fixture(), gateway = new DshGateway({ core: f.core });
  const server = createBridgeServer({ core: f.core, adminToken: ADMIN, dsh: gateway, port: 0 }), base = await server.listen();
  t.after(async () => { gateway.close(); await server.close(); f.core.close(); });
  const options = { session: 'session-test-only', pluginData }, preview = await provisionDsh(options);
  assert.equal(preview.writes, false); assert.equal((await readdir(dir)).includes('private'), false);
  const admin = new BridgeClient(base, { adminToken: ADMIN }), before = f.core.db.prepare('SELECT COUNT(*) AS n FROM grants').get().n;
  const result = await provisionDsh({ ...options, write: true, serviceUrl: base }, { client: admin });
  const config = JSON.parse(await readFile(path.join(pluginData, 'bridge.json'), 'utf8')), binding = config.bindings[options.session];
  assert.doesNotMatch(JSON.stringify(result), new RegExp(binding.credential));
  assert.equal(f.core.authenticateEndpoint(binding.endpoint_id, binding.credential).capabilities.can_enqueue, false);
  assert.equal(f.core.db.prepare('SELECT COUNT(*) AS n FROM grants').get().n, before);
  await assert.rejects(provisionDsh({ ...options, write: true, serviceUrl: base }, { client: admin }), code('ALREADY_REGISTERED'));
  const other = { ...options, session: 'uncertain-test' };
  await assert.rejects(provisionDsh({ ...other, write: true, serviceUrl: base }, { client: { request: async path => { if (path === '/api/health') return { features: { dsh_host: true, provided_endpoint_credentials: true } }; throw Error('response lost'); } } }), /response lost/);
  assert.ok((await readdir(pluginData)).some(name => name.startsWith('.bridge-pending-')));
  assert.equal(JSON.parse(await readFile(path.join(pluginData, 'bridge.json'), 'utf8')).bindings[other.session], undefined);
});
