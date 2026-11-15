import assert from 'node:assert/strict';
import { readFile, writeFile, mkdir, rename, unlink, realpath } from 'node:fs/promises';
import { dirname, join, resolve, basename, isAbsolute } from 'node:path';
import { fileURLToPath } from 'node:url';
import { randomUUID, createHash } from 'node:crypto';
import { DatabaseSync, backup } from 'node:sqlite';
import { setTimeout as delay } from 'node:timers/promises';
import { createCandidateService } from '../experimental/source-automation/candidate-service.mjs';
import { canonical } from '../src/protocol.mjs';
import { isDailyUseAutoStartHeld } from '../src/dsh-service-start.mjs';
import { dailyUseHealth, lockDailyUse, superviseDailyUse } from './daily-use-supervisor.mjs';

const project = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const runtime = join(project, '.runtime');
const json = async path => JSON.parse((await readFile(path, 'utf8')).replace(/^\uFEFF/, ''));
async function save(path, value) {
  const temp = path + '.' + randomUUID() + '.tmp';
  await writeFile(temp, JSON.stringify(value, null, 2) + '\n', { mode: 0o600, flag: 'wx', flush: true });
  try { await rename(temp, path); }
  catch (error) { await unlink(temp); throw error; }
}

async function checkedDirectory(input) {
  const path = await realpath(input);
  assert.equal(dirname(path).toLowerCase(), (await realpath(runtime)).toLowerCase());
  assert.match(basename(path), /^daily-use-\d+$/);
  return path;
}

export async function prepareDailyUse(configPath) {
  assert.ok(typeof configPath === 'string' && isAbsolute(configPath), 'Supply an absolute private configuration path');
  const input = await json(configPath);
  assert.ok(Number.isInteger(input.port) && input.port > 0 && input.port <= 65535 && input.port !== 4776, 'Choose a dedicated local service port');
  const endpoints = input.endpoints ?? [];
  assert.ok(Array.isArray(endpoints) && endpoints.every(endpoint => typeof endpoint.credential === 'string'), 'Provide a private credential for each configured endpoint');
  const directory = join(runtime, 'daily-use-' + Date.now());
  await mkdir(runtime, { recursive: true });
  await mkdir(directory);
  const config = { ...input, directory: join(directory, 'data'), nativeAdapters: input.nativeAdapters ?? true };
  const service = await createCandidateService({ ...config, port: 0 });
  await service.close();
  // Keep newly registered identities separate from settings that the running service may update.
  await save(join(directory, 'private-endpoints.json'), endpoints);
  await save(join(directory, 'config.json'), { ...config, endpoints: [] });
  await save(join(runtime, 'daily-use-active.json'), { directory });
  return { directory, port: input.port, endpoints: endpoints.length };
}

export async function snapshotLedger(directory, label) {
  const config = await json(join(directory, 'config.json'));
  const source = new DatabaseSync(join(config.directory, 'candidate.sqlite'), { readOnly: true });
  const target = join(directory, label + '-' + Date.now() + '.sqlite');
  try {
    source.exec('BEGIN');
    assert.equal(source.prepare('PRAGMA quick_check').get().quick_check, 'ok');
    await backup(source, target);
    const copy = new DatabaseSync(target, { readOnly: true });
    try {
      const tables = source.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").all();
      for (const { name } of tables) {
        assert.match(name, /^[a-z_]+$/);
        const fingerprint = db => {
          const hash = createHash('sha256');
          for (const row of db.prepare('SELECT * FROM ' + name + ' ORDER BY rowid').iterate()) hash.update(canonical(row) + '\n');
          return hash.digest('hex');
        };
        assert.equal(fingerprint(copy), fingerprint(source), 'Snapshot changed ' + name);
      }
    } finally { copy.close(); }
    return { path: target, checkedAt: new Date().toISOString(), messages: source.prepare('SELECT COUNT(*) AS n FROM messages').get().n,
      cursors: source.prepare('SELECT target_id,archive_id,read_through FROM history_cursors').all() };
  } finally { source.close(); }
}

async function finishDailyUseStop(directory, command) {
  const snapshot = await snapshotLedger(directory, 'preserved');
  await save(join(directory, 'stopped.json'), { at: new Date().toISOString(), pid: process.pid,
    action: command.action, revision: command.revision, snapshot });
}

export async function serveDailyUse(directory, { supervised = false } = {}) {
  const lock = lockDailyUse(directory, 'worker');
  try { await runDailyUse(directory, supervised); } finally { lock.close(); }
}

async function runDailyUse(directory, supervised) {
  const endpoints = await json(join(directory, 'private-endpoints.json'));
  const config = await json(join(directory, 'config.json'));
  assert.equal(await realpath(config.directory), join(directory, 'data'));
  let lastRevision = '';
  try {
    const control = await json(join(directory, 'control.json'));
    if (control.action === 'stop') lastRevision = control.revision;
    else lastRevision = (await json(join(directory, 'control-result.json'))).revision;
  } catch (error) { if (error.code !== 'ENOENT') throw error; }
  const service = await createCandidateService(config);
  try {
    await save(join(directory, 'ready.json'), { url: service.url, pid: process.pid, startedAt: new Date().toISOString(), directory });
  } catch (error) { await service.close(); throw error; }
  let closing = false, stopping;
  const stop = command => stopping ??= (async () => {
    closing = true;
    await service.close();
    await finishDailyUseStop(directory, command);
  })();
  const signalStop = () => { void stop({ action: 'stop' }).catch(() => {}); };
  const parentStop = message => {
    if (message?.command === 'daily-use-stop' && message.action === 'stop' && typeof message.revision === 'string') {
      void stop(message).catch(() => {});
    }
  };
  process.on('SIGINT', signalStop);
  process.on('SIGTERM', signalStop);
  if (supervised) {
    process.on('message', parentStop);
    process.on('disconnect', signalStop);
    if (process.connected) process.send({ event: 'daily-use-ready', pid: process.pid, url: service.url }, () => {});
    else signalStop();
  }
  try {
    while (!closing) {
      await delay(500);
      if (closing) break;
      let command;
      try { command = await json(join(directory, 'control.json')); } catch (error) { if (error.code === 'ENOENT') continue; throw error; }
      if (closing) break;
      if (command.revision === lastRevision) continue;
      lastRevision = command.revision;
      if (command.action === 'stop') { await stop(command); break; }
      assert.ok(['pause', 'resume', 'snapshot'].includes(command.action));
      if (command.action === 'snapshot') await save(join(directory, 'snapshot.json'), await snapshotLedger(directory, 'snapshot'));
      else {
        for (const endpoint of endpoints) service.core.setSettings(config.adminToken, endpoint.id, { paused: command.action === 'pause' });
      }
      await save(join(directory, 'control-result.json'), { revision: lastRevision, action: command.action, at: new Date().toISOString() });
    }
  } finally {
    try { await (stopping ?? stop({ action: 'stop' })); }
    finally {
      process.off('SIGINT', signalStop);
      process.off('SIGTERM', signalStop);
      if (supervised) {
        process.off('message', parentStop);
        process.off('disconnect', signalStop);
      }
    }
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const [action, input, workerFlag] = process.argv.slice(2);
    if (action === 'prepare') {
      console.log(JSON.stringify(await prepareDailyUse(input)));
    } else {
      const directory = await checkedDirectory(input ?? (await json(join(runtime, 'daily-use-active.json'))).directory);
      if (action === 'serve') await serveDailyUse(directory, { supervised: workerFlag === '--supervised' });
      else if (action === 'supervise') await superviseDailyUse(directory, { entry: fileURLToPath(import.meta.url),
        finishStop: finishDailyUseStop, autoStart: workerFlag === '--autostart' });
      else if (['pause', 'resume', 'stop', 'snapshot'].includes(action)) {
        const file = join(directory, 'control.json');
        const preserveHold = action === 'pause' || action === 'snapshot';
        const previous = preserveHold ? await json(file).catch(error => { if (error.code !== 'ENOENT') throw error; }) : null;
        const holdAutostart = isDailyUseAutoStartHeld({ action }) || isDailyUseAutoStartHeld(previous);
        await save(file, { action, revision: randomUUID(), at: new Date().toISOString(), ...(holdAutostart ? { hold_autostart: true } : {}) });
        console.log(JSON.stringify({ action, requested: true, directory }));
      } else if (action === 'status') {
        const ready = await json(join(directory, 'ready.json'));
        console.log(JSON.stringify({ ...ready, ...await dailyUseHealth(ready.url) }));
      } else throw new Error('Expected prepare, serve, supervise, pause, resume, snapshot, stop or status');
    }
  } catch (error) {
    const code = error.code ?? error.name;
    if (process.argv[4] === '--supervised' && process.connected) process.send({ event: 'daily-use-error', code }, () => {});
    console.error(JSON.stringify({ result: 'failed', error: code, message: error.message }));
    process.exitCode = 1;
  } finally {
    if (process.argv[4] === '--supervised' && process.connected) process.disconnect();
  }
}
