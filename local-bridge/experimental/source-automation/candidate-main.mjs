import { readFile } from 'node:fs/promises';
import { isAbsolute } from 'node:path';
import { createCandidateService } from './candidate-service.mjs';
import { insist } from '../../src/protocol.mjs';
import { isMainModule } from '../../src/entrypoint.mjs';

export async function runCandidate(configPath) {
  insist(typeof configPath === 'string' && isAbsolute(configPath), 'INVALID_INPUT', 'An explicit absolute private candidate config path is required');
  const config = JSON.parse((await readFile(configPath, 'utf8')).replace(/^\uFEFF/, ''));
  const service = await createCandidateService(config);
  console.log('local-bridge settings: ' + service.url);
  console.log('Native delivery adapters: ' + (service.nativeRuntime ? 'natural turns only; idle wake disabled' : 'disabled'));
  let closing;
  const close = () => closing ??= (async () => {
    try { await service.close(); }
    finally { if (process.connected) process.disconnect(); }
  })();
  process.on('SIGINT', () => { void close().catch(() => { process.exitCode = 1; }); });
  process.on('SIGTERM', () => { void close().catch(() => { process.exitCode = 1; }); });
  // IPC exists only when an operator-owned test parent explicitly starts this process with it.
  if (process.send) {
    process.on('message', value => { if (value?.command === 'shutdown') void close().catch(() => { process.exitCode = 1; }); });
    process.send({ event: 'ready', url: service.url });
  }
  return { ...service, close };
}
if (isMainModule(import.meta.url)) {
  const args = process.argv.slice(2), index = args.indexOf('--config');
  runCandidate(index >= 0 ? args[index + 1] : undefined).catch(error => {
    console.error('local-bridge startup failed: ' + (error.code ?? error.name)); process.exitCode = 1;
    if (process.connected) process.disconnect();
  });
}
