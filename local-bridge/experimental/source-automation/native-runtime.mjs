import { CodexGateway } from '../../src/codex-gateway.mjs';
import { DshGateway } from '../../src/dsh-gateway.mjs';
import { DeliveryCoordinator } from '../../src/delivery.mjs';
import { CodexDeliveryAdapter, CodexQueue } from '../../src/adapters/codex.mjs';
import { insist } from '../../src/protocol.mjs';

/** Native protocol endpoints, fenced to natural turns; no queue CLI can be launched. */
export function createNaturalNativeRuntime({ core, timeoutMs = 5000, historyBatchNotices = false }) {
  const codex = new CodexGateway({ core }), dsh = new DshGateway({ core });
  const codexAdapter = new CodexDeliveryAdapter({ gateway: codex, queue: new CodexQueue({ enabled: false }) });
  const naturalOnly = adapter => ({ state: endpoint => adapter.state(endpoint), offer: input => {
    insist(input.wake === false, 'WAKE_NOT_AUTHORIZED', 'This candidate permits natural-turn delivery only');
    return adapter.offer(input);
  } });
  const coordinator = new DeliveryCoordinator({ core, timeoutMs, historyBatchNotices,
    adapters: new Map([['codex', naturalOnly(codexAdapter)], ['dsh', naturalOnly(dsh)]]) });
  codex.coordinator = coordinator; dsh.coordinator = coordinator;
  return { codex, dsh, coordinator, queueEnabled: false,
    setUrl(url) { codex.settingsUrl = url; dsh.settingsUrl = url; },
    async close() { await coordinator.close(); dsh.close(); } };
}
