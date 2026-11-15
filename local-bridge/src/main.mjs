import fs from 'node:fs';
import { randomBytes } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { join } from 'node:path';
import { TransportCore } from './core.mjs';
import { createBridgeServer } from './server.mjs';
import { DeliveryCoordinator } from './delivery.mjs';
import { SourceIndexer } from './indexer.mjs';
import { CodexGateway } from './codex-gateway.mjs';
import { CodexDeliveryAdapter, CodexQueue } from './adapters/codex.mjs';
import { DshGateway } from './dsh-gateway.mjs';

const runtime = fileURLToPath(new URL('../.runtime/', import.meta.url)); fs.mkdirSync(runtime, { recursive: true });
const configPath = join(runtime, 'service-config.json');
if (!fs.existsSync(configPath)) fs.writeFileSync(configPath, JSON.stringify({ admin_token: randomBytes(32).toString('base64url'), port: 4776, workspace: 'local-bridge' }, null, 2) + '\n', { mode: 0o600, flag: 'wx' });
const config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
const core = new TransportCore({ database: join(runtime, 'bridge.sqlite'), workspace: config.workspace,
  adminToken: config.admin_token, historyClaimMs: config.historyClaimMs });
if (process.argv.includes('--demo') && core.listEndpoints(config.admin_token).length === 0) {
  for (const harness of ['codex', 'dsh']) core.register(config.admin_token, { id: `demo-${harness}`, harness, native_session_id: `demo-unconnected-${harness}`, work_domains: harness === 'codex' ? ['text', 'engineering'] : ['text'] });
}
const codex = new CodexGateway({ core, settingsUrl: 'http://127.0.0.1:' + config.port });
const codexAdapter = new CodexDeliveryAdapter({ gateway: codex, queue: new CodexQueue(config.codex_queue) });
const dsh = new DshGateway({ core, settingsUrl: 'http://127.0.0.1:' + config.port });
const coordinator = new DeliveryCoordinator({ core, historyBatchNotices: config.historyBatchNotices === true,
  adapters: new Map([['codex', codexAdapter], ['dsh', dsh]]) });
codex.coordinator = coordinator;
dsh.coordinator = coordinator;
const indexer = new SourceIndexer({ core, adminToken: config.admin_token });
const service = createBridgeServer({ core, adminToken: config.admin_token, indexer, codex, dsh, port: config.port });
try { console.log('Local settings: ' + await service.listen()); console.log('Administrator credential file: ' + configPath); }
catch (error) { await coordinator.close(); core.close(); throw error; }
let closing = false;
async function close() { if (closing) return; closing = true; await coordinator.close(); dsh.close(); await service.close(); core.close(); }
process.on('SIGINT', () => close().catch(() => { process.exitCode = 1; }));
process.on('SIGTERM', () => close().catch(() => { process.exitCode = 1; }));
