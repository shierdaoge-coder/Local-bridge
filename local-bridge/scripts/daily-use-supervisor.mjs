import { fork } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { appendFile, readFile, rename, unlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { setTimeout as delay } from 'node:timers/promises';
import { BridgeError, PROTOCOL, insist } from '../src/protocol.mjs';
import { isDailyUseAutoStartHeld } from '../src/dsh-service-start.mjs';

const terminalActions = new Set(['stop']);
const rejectedStartup = new Set([
  'ALREADY_RUNNING', 'EADDRINUSE', 'ERR_ASSERTION', 'INVALID_INPUT', 'NOT_AUTHORIZED', 'UNAUTHORIZED',
  'SOURCE_REPLACED', 'SOURCE_IDENTITY_MISMATCH', 'WORKSPACE_MISMATCH', 'READER_VERSION_MISMATCH',
]);

const errorCode = error => {
  const value = typeof error?.code === 'string' ? error.code : error?.cause?.code ?? error?.name;
  return typeof value === 'string' && /^[A-Za-z0-9_]+$/.test(value) ? value : 'UNKNOWN';
};

export async function saveState(directory, name, state) {
  const target = join(directory, name);
  const staged = target + '.' + randomUUID() + '.tmp';
  await writeFile(staged, JSON.stringify(state) + '\n', { flag: 'wx', mode: 0o600, flush: true });
  try { await rename(staged, target); }
  catch (error) { await unlink(staged); throw error; }
}

async function readState(directory, file, optional = false) {
  try { return JSON.parse((await readFile(join(directory, file), 'utf8')).replace(/^\uFEFF/, '')); }
  catch (error) { if (optional && error.code === 'ENOENT') return null; throw error; }
}

export function lockDailyUse(directory, role) {
  const db = new DatabaseSync(join(directory, `.${role}-lock.sqlite`));
  try { db.exec('PRAGMA busy_timeout=0; BEGIN IMMEDIATE'); }
  catch (error) {
    db.close();
    if (error.errcode === 5 || /database is locked/i.test(error.message)) throw new BridgeError('ALREADY_RUNNING', `A daily-use ${role} already owns this directory`);
    throw error;
  }
  return db;
}

export async function dailyUseHealth(url, timeoutMs = 1500) {
  const local = new URL(url);
  insist(local.protocol === 'http:' && local.hostname === '127.0.0.1' && !local.username && !local.password,
    'INVALID_INPUT', 'Daily-use health checks require the bound IPv4 loopback service');
  try {
    const response = await fetch(local.origin + '/api/health', { redirect: 'error', signal: AbortSignal.timeout(timeoutMs) });
    const body = await response.json();
    if (response.ok && body.ok === true && body.name === 'local-bridge' && body.protocol === PROTOCOL) return { healthy: true };
    return { healthy: false, error: 'UNEXPECTED_HEALTH_RESPONSE' };
  } catch (error) { return { healthy: false, error: errorCode(error) }; }
}

function startWorker(entry, directory) {
  const child = fork(entry, ['serve', directory, '--supervised'], { execArgv: [], windowsHide: true, stdio: ['ignore', 'ignore', 'ignore', 'ipc'] });
  const worker = { child, exit: null, url: null, error: null };
  worker.exited = new Promise(resolve => {
    const finish = (code, signal) => { worker.exit = { code, signal }; resolve(worker.exit); };
    child.once('exit', finish);
    child.on('error', error => { worker.error = errorCode(error); if (!child.pid) finish(null, null); });
  });
  child.on('message', message => {
    if (message?.event === 'daily-use-ready' && message.pid === child.pid) worker.url = message.url;
    if (message?.event === 'daily-use-error' && /^[A-Za-z0-9_]+$/.test(message.code)) worker.error = message.code;
  });
  return worker;
}

async function waitForExit(worker, ms) {
  await Promise.race([worker.exited, delay(ms, undefined, { ref: false })]);
  return worker.exit;
}

async function stopWorker(worker, command, graceMs) {
  if (worker.exit) return;
  if (command) {
    const deadline = Date.now() + graceMs;
    while (!worker.exit && !worker.url && Date.now() < deadline) await waitForExit(worker, 100);
    if (worker.exit) return;
    if (worker.child.connected) worker.child.send({ command: 'daily-use-stop', ...command }, () => {});
    if (await waitForExit(worker, Math.max(1, deadline - Date.now()))) return;
  }
  worker.child.kill('SIGKILL');
  if (!(await waitForExit(worker, 5000))) throw new BridgeError('WORKER_STOP_TIMEOUT', 'The supervised worker did not exit');
}

/** Supervise only the child started for this trial directory. Terminal controls keep it stopped. */
export async function superviseDailyUse(directory, { entry, finishStop, pollMs = 2000, startupMs = 30_000,
  healthTimeoutMs = 1500, stopGraceMs = 15_000, autoStart = false } = {}) {
  const launchedAt = Date.now();
  const lock = lockDailyUse(directory, 'supervisor');
  const log = (event, details = {}) => appendFile(join(directory, 'supervisor.jsonl'),
    JSON.stringify({ at: new Date().toISOString(), event, supervisor_pid: process.pid, ...details }) + '\n', { mode: 0o600, flush: true });
  let worker, requested, baseline, failures = 0;
  const requestStop = () => { requested ??= { action: 'stop', revision: randomUUID() }; };
  const checkControl = async () => {
    const control = await readState(directory, 'control.json', true);
    if (control?.revision && control.revision !== baseline && terminalActions.has(control.action)) requested ??= control;
    return requested;
  };
  process.on('SIGINT', requestStop);
  process.on('SIGTERM', requestStop);
  try {
    const previous = await readState(directory, 'control.json', true);
    baseline = previous?.revision;
    if (autoStart && isDailyUseAutoStartHeld(previous)) {
      await log('automatic_start_skipped', { action: previous.action, revision: previous.revision });
      return;
    }
    if (terminalActions.has(previous?.action) && previous.revision && Date.parse(previous.at) > launchedAt) requested ??= previous;
    await log('supervisor_started');
    while (!(await checkControl())) {
      const config = await readState(directory, 'config.json');
      insist(Number.isInteger(config.port) && config.port >= 0 && config.port <= 65535 && config.port !== 4776,
        'INVALID_INPUT', 'The supervised trial requires a valid service port');
      if (config.port) {
        const occupied = await dailyUseHealth('http://127.0.0.1:' + config.port, healthTimeoutMs);
        insist(!occupied.healthy && occupied.error === 'ECONNREFUSED', 'PORT_OCCUPIED', 'The configured trial port is already in use or could not be checked');
      }
      worker = startWorker(entry, directory);
      await log('worker_started', { worker_pid: worker.child.pid });
      const started = Date.now();
      let healthyAt = 0, misses = 0, reason = 'worker_exit';
      while (!worker.exit && !(await checkControl())) {
        if (worker.url) {
          const url = new URL(worker.url);
          insist(url.protocol === 'http:' && url.hostname === '127.0.0.1' && (!config.port || Number(url.port || 80) === config.port),
            'INVALID_INPUT', 'The worker did not bind its configured trial port');
          const health = await dailyUseHealth(worker.url, healthTimeoutMs);
          if (health.healthy) {
            if (!healthyAt) { healthyAt = Date.now(); await log('worker_healthy', { worker_pid: worker.child.pid, url: worker.url }); }
            misses = 0;
          } else if (healthyAt && ++misses >= 3) {
            reason = health.error;
            await log('worker_unhealthy', { worker_pid: worker.child.pid, error: reason, checks: misses });
            break;
          }
        }
        if (!healthyAt && Date.now() - started >= startupMs) { reason = 'STARTUP_TIMEOUT'; break; }
        await delay(pollMs);
      }
      await checkControl();
      await stopWorker(worker, requested, stopGraceMs);
      await log('worker_exited', { worker_pid: worker.child.pid, exit_code: worker.exit.code, signal: worker.exit.signal,
        reason: requested?.action ?? reason, error: worker.error });
      const failure = worker.error;
      worker = null;
      if (requested) break;
      if (rejectedStartup.has(failure)) throw new BridgeError(failure, 'The worker rejected the trial configuration');
      if (healthyAt && Date.now() - healthyAt >= 30_000) failures = 0;
      const retryMs = Math.min(60_000, 1000 * 2 ** Math.min(failures++, 6));
      await log('worker_restart_scheduled', { delay_ms: retryMs, reason, error: failure });
      const retryAt = Date.now() + retryMs;
      while (Date.now() < retryAt && !(await checkControl())) await delay(Math.min(pollMs, Math.max(1, retryAt - Date.now())));
    }
    const receipt = await readState(directory, 'stopped.json', true);
    if (receipt?.revision !== requested.revision || receipt.action !== requested.action) {
      const workerLock = lockDailyUse(directory, 'worker');
      try { await finishStop(directory, requested); } finally { workerLock.close(); }
    }
    await log('supervisor_stopped', { action: requested.action, revision: requested.revision });
  } catch (error) {
    await log('supervisor_failed', { error: errorCode(error) });
    throw error;
  } finally {
    try { if (worker && !worker.exit) await stopWorker(worker, { action: 'stop', revision: randomUUID() }, stopGraceMs); }
    finally {
      process.off('SIGINT', requestStop);
      process.off('SIGTERM', requestStop);
      lock.close();
    }
  }
}
