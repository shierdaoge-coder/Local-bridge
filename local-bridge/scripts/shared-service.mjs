import { randomUUID } from 'node:crypto';
import { appendFile, readFile, realpath, stat } from 'node:fs/promises';
import { isAbsolute, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createCandidateService } from '../experimental/source-automation/candidate-service.mjs';
import { isMainModule } from '../src/entrypoint.mjs';
import { insist } from '../src/protocol.mjs';
import { planSessionPairs } from '../src/session-pairs.mjs';
import { isDailyUseAutoStartHeld, readSharedServiceConfig } from '../src/service-start.mjs';
import { dailyUseHealth, lockDailyUse, saveState, superviseDailyUse } from './daily-use-supervisor.mjs';
import { startNativeRelay } from './relay-session.mjs';

export { saveState } from './daily-use-supervisor.mjs';

async function serve(directory) {
  insist(process.connected, 'INVALID_INPUT', 'Shared service workers must start under the supervisor');
  const lock = lockDailyUse(directory, 'worker');
  let finish;
  const stopped = new Promise(resolve => { finish = resolve; });
  const stop = () => finish();
  const onMessage = value => {
    if (value?.command === 'daily-use-stop') stop();
  };
  process.on('message', onMessage);
  process.once('disconnect', stop);
  process.once('SIGINT', stop);
  process.once('SIGTERM', stop);
  try {
    const config = await readSharedServiceConfig(directory);
    const service = await createCandidateService(config);
    let relay;
    try {
      if (config.nativeRelay) relay = startNativeRelay(directory, config, {
        onStatus: value => appendFile(join(directory, 'native-relay.jsonl'),
          JSON.stringify({ at: new Date().toISOString(), ...value }) + '\n', { mode: 0o600 }),
      });
      if (process.connected) process.send({ event: 'daily-use-ready', pid: process.pid, url: service.url }, error => { if (error) stop(); });
      else stop();
      await stopped;
    } finally {
      try { await relay?.close(); }
      finally { await service.close(); }
    }
  } finally {
    process.off('message', onMessage);
    process.off('disconnect', stop);
    process.off('SIGINT', stop);
    process.off('SIGTERM', stop);
    lock.close();
  }
}

async function main([action, input, flag]) {
  insist(typeof input === 'string' && isAbsolute(input), 'INVALID_INPUT', 'Use an absolute shared service directory');
  const directory = await realpath(input);
  if (action === 'serve' && flag === '--supervised') return serve(directory);
  if (action === 'stop' || action === 'resume') {
    if (action === 'resume') await readSharedServiceConfig(directory);
    else insist((await stat(join(directory, 'config.json'))).isFile(), 'INVALID_INPUT', 'Select a configured service directory');
    const control = { action, revision: randomUUID(), at: new Date().toISOString(), hold_autostart: action === 'stop' };
    await saveState(directory, 'control.json', control);
    console.log(JSON.stringify({ action, revision: control.revision }));
    return;
  }
  const config = await readSharedServiceConfig(directory);
  const { port } = config;
  if (action === 'pairs') {
    const plan = planSessionPairs(config);
    console.log(JSON.stringify({ grants: plan.grants.map(({ id, from, to, content_types }) => ({
      id, from, to, content_types, native_delivery: plan.nativeDirections.has(id),
    })), subscriptions: plan.subscriptions }));
    return;
  }
  if (action === 'supervise') return superviseDailyUse(directory, {
    entry: fileURLToPath(import.meta.url), autoStart: true,
    finishStop: async (dir, command) => saveState(dir, 'stopped.json', {
      action: command.action, revision: command.revision, at: new Date().toISOString(),
    }),
  });
  insist(action === 'status', 'INVALID_INPUT', 'Expected supervise, status, pairs, stop or resume');
  let control;
  try { control = JSON.parse((await readFile(join(directory, 'control.json'), 'utf8')).replace(/^\uFEFF/, '')); }
  catch (error) { if (error.code !== 'ENOENT') throw error; }
  const url = 'http://127.0.0.1:' + port;
  console.log(JSON.stringify({ url, ...await dailyUseHealth(url), hold_autostart: isDailyUseAutoStartHeld(control) }));
}

if (isMainModule(import.meta.url)) {
  const args = process.argv.slice(2);
  try { await main(args); }
  catch (error) {
    const code = error.code ?? error.name;
    if (process.connected && args[2] === '--supervised') process.send({ event: 'daily-use-error', code }, () => {});
    if (code !== 'ALREADY_RUNNING') { console.error('Bridge service: ' + code); process.exitCode = 1; }
  } finally { if (process.connected && args[2] === '--supervised') process.disconnect(); }
}
