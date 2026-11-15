// Pure JS host fixtures: no real Codex thread or DSH agent is contacted.
import { writeFile, appendFile } from 'node:fs/promises';
import { join } from 'node:path';
import { randomBytes, randomUUID } from 'node:crypto';
import { zstdCompressSync } from 'node:zlib';
import { setTimeout as delay } from 'node:timers/promises';
import assert from 'node:assert/strict';
import { createCandidateService } from './candidate-service.mjs';
import { apply as applyDshPlugin } from '../../src/dsh-plugin.mjs';
import { runCodexHook } from '../../src/codex-hook.mjs';
import { CodexMcpProxy } from '../../src/codex-mcp.mjs';
import { CODEX_TOOL_PREFIX } from '../../src/adapters/codex.mjs';
import { BridgeClient } from '../../src/client.mjs';
import { visibleMessage } from '../../src/history.mjs';
import { createHistorySync } from '../../src/history-sync-host.mjs';

const codexRow = text => ({ type: 'response_item', payload: { type: 'message', role: 'user', content: [{ type: 'input_text', text }] } });
const dshRow = (text, source = { kind: 'user' }) => ({ type: 'user/message', data: { id: randomUUID(), role: 'user', source, content: [{ type: 'text', text }] } });
async function until(test, reason) {
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) { if (test()) return; await delay(10); }
  throw new Error('Native fixture timeout: ' + reason);
}
export async function exerciseNativeCandidate({ directory, workspace = 'isolated-native-fixture', historyBatchNotices = false, historySyncOptions }) {
  const paths = { codex: join(directory, 'native-codex.jsonl'), dsh: join(directory, 'native-dsh.jsonl.zstd') };
  const ids = { codex: 'candidate-native-codex', dsh: 'candidate-native-dsh' };
  await writeFile(paths.codex, JSON.stringify({ type: 'session_meta', payload: { id: ids.codex } }) + '\n', { flag: 'wx' });
  await writeFile(paths.dsh, zstdCompressSync(Buffer.from(JSON.stringify({ type: 'session', version: 4, id: ids.dsh }) + '\n')), { flag: 'wx' });
  const credentials = { codex: randomBytes(32).toString('base64url'), dsh: randomBytes(32).toString('base64url') };
  const adminToken = randomBytes(32).toString('base64url');
  const config = { directory, workspace, adminToken, nativeAdapters: true, historyBatchNotices,
    endpoints: ['codex', 'dsh'].map(kind => ({ id: `candidate-${kind}`, harness: kind, native_session_id: ids[kind], credential: credentials[kind], capabilities: { can_enqueue: true } })),
    archives: ['codex', 'dsh'].map(kind => ({ id: `copy-${kind}`, owner: `candidate-${kind}`, generation: 'snapshot-v1' })),
    bindings: ['codex', 'dsh'].map(kind => ({ archive_id: `copy-${kind}`, path: paths[kind], format: kind === 'codex' ? 'codex-jsonl' : 'dsh-zstd', initial_mode: 'from_now', author_ref: 'native-fixture' })),
    grants: ['codex', 'dsh'].map(kind => ({ id: `candidate-${kind}-out`, from: `candidate-${kind}`, to: `candidate-${kind === 'codex' ? 'dsh' : 'codex'}`,
      content_types: ['history_sync', 'message'], allow_wake: false, author_ref: 'native-fixture' })),
    subscriptions: ['codex', 'dsh'].map(kind => ({ id: `sub-${kind}`, archive_id: `copy-${kind}`, to: `candidate-${kind === 'codex' ? 'dsh' : 'codex'}`, grant_id: `candidate-${kind}-out` })),
    readerOptions: { maxVisibleRecords: 16 }, pumpOptions: { delayMs: 5, pollMs: 25 } };
  await writeFile(join(directory, 'candidate-private-config.json'), JSON.stringify(config, null, 2) + '\n', { mode: 0o600, flag: 'wx' });
  const service = await createCandidateService(config);
  let unload = async () => {}, extraInjections = 0, extraTurns = 0;
  try {
    await until(() => ['codex', 'dsh'].every(k => !service.core.binding(adminToken, `copy-${k}`).seeding), 'initial boundary');
    const health = await fetch(service.url + '/api/health').then(r => r.json()); assert.equal(health.features.codex_hooks, true); assert.equal(health.features.dsh_host, true);
    const codexConfig = { service_url: service.url, bindings: { [ids.codex]: { endpoint_id: 'candidate-codex', credential: credentials.codex, cwd: directory, transcript_path: paths.codex } } };
    const codexSync = historySyncOptions && createHistorySync(historySyncOptions);
    const dshSync = historySyncOptions && createHistorySync(historySyncOptions);
    const proxy = new CodexMcpProxy({ client: new BridgeClient(service.url), config: codexConfig, historySync: codexSync });
    const hookEvent = (hook_event_name, patch = {}) => ({ session_id: ids.codex, cwd: directory, transcript_path: paths.codex,
      hook_event_name, turn_id: 'synthetic-native-turn', ...patch });
    async function codexCall(name, args = {}) {
      const result = await runCodexHook(hookEvent('PreToolUse', { tool_name: CODEX_TOOL_PREFIX + name, tool_use_id: randomUUID(), tool_input: args }), { config: codexConfig });
      assert.equal(result.hookSpecificOutput.permissionDecision, 'allow');
      return proxy.execute(name, result.hookSpecificOutput.updatedInput);
    }
    assert.equal((await codexCall('bridge_context')).endpoint.id, 'candidate-codex');
    const bindingFile = join(directory, 'dsh-fixture-binding.json');
    await writeFile(bindingFile, JSON.stringify({ service_url: service.url, bindings: { [ids.dsh]: { endpoint_id: 'candidate-dsh', credential: credentials.dsh } } }), { mode: 0o600 });
    const tools = new Map(), listeners = new Map();
    const agent = { session: { id: ids.dsh, log: [] }, status: 'idle',
      inject: () => { extraInjections++; throw new Error('Unexpected native injection'); },
      followup: () => { extraTurns++; throw new Error('Unexpected native turn'); },
      ctx: { on: (event, fn) => { listeners.set(event, fn); return () => listeners.delete(event); } } };
    unload = await applyDshPlugin({ agents: new Map([[ids.dsh, agent]]), tools: { register: tool => { tools.set(tool.name, tool); return () => tools.delete(tool.name); } } },
      { binding_file: bindingFile, historySync: dshSync,
        loadNative: async () => ({ createUserMessage: input => Object.freeze({ id: randomUUID(), role: 'user', ...input }), adoptSessionEvent: event => event }) });
    const dshCall = async (name, args = {}) => {
      const value = JSON.parse(await tools.get(name).execute(args, { agent })); assert.equal(value.ok, true); return value.result;
    };
    async function receiveBoth(call, source, notice) {
      const archive_id = 'copy-' + source;
      const cursor = async () => (await call('bridge_history_cursor', { archive_id, generation: 'snapshot-v1' })).read_through;
      const acknowledgement = item => ({ id: item.id, claim_token: item.claim.token });
      let event, historyAck;
      if (historySyncOptions) {
        assert.ok(notice.includes('bridge_sync_history') && notice.includes(archive_id));
        const expected = 'SYNTHETIC native ' + source + ' increment';
        [event] = (await call('bridge_receive', { ids: ['native-event-' + source] })).items;
        const history = await call('bridge_sync_history', { archive_id });
        assert.deepEqual(history.runs, [{ role: 'user', messages: [[0, expected]] }]);
        assert.equal(history.state, 'read'); assert.ok(history.batch_id); assert.equal(history.receipt, undefined);
        historyAck = { batch_id: history.batch_id };
      } else if (historyBatchNotices) {
        const advertised = JSON.parse(/^<local_bridge_transport>([^]*?)<\/local_bridge_transport>/.exec(notice)[1]);
        assert.deepEqual(advertised, { history_sync: [{ archive_id, unread_packets: 1 }], message_ids: ['native-event-' + source] });
        [event] = (await call('bridge_receive', { ids: advertised.message_ids })).items;
        const history = await call('bridge_receive', { archive_id });
        assert.deepEqual(history.runs, [{ role: 'user', messages: [[0, 'SYNTHETIC native ' + source + ' increment']] }]);
        historyAck = { receipt: history.receipt };
      } else {
        const { items } = await call('bridge_receive');
        assert.equal(items.length, 2);
        event = items.find(item => item.content_type === 'message');
        const history = items.find(item => item.content_type === 'history_sync');
        assert.equal(history.payload.archive_id, archive_id);
        assert.equal(history.payload.records[0].text, 'SYNTHETIC native ' + source + ' increment');
        historyAck = { acknowledgements: [acknowledgement(history)] };
      }
      assert.equal(event.payload.text, 'SYNTHETIC native ' + source);
      const cursors = [await cursor()];
      await call('bridge_ack', { acknowledgements: [acknowledgement(event)] });
      cursors.push(await cursor());
      if (historySyncOptions) assert.equal((await call('bridge_sync_history', historyAck)).state, 'confirmed');
      else await call('bridge_ack', historyAck);
      cursors.push(await cursor());
      assert.deepEqual(cursors, [-1, -1, 0]);
      await call('bridge_ack', { acknowledgements: [{ ...acknowledgement(event), outcome: 'completed' }] });
      return cursors;
    }
    await dshCall('bridge_context');
    service.core.setCapabilities(adminToken, 'candidate-dsh', { can_enqueue: true, can_confirm_read: true, can_wake: true });
    assert.equal(service.core.endpoint('candidate-dsh').capabilities.can_wake, false);
    const event = source => ({ id: `native-event-${source}`, to: `candidate-${source === 'codex' ? 'dsh' : 'codex'}`,
      grant_id: `candidate-${source}-out`, payload: { kind: 'event', topic: 'native.fixture', correlation_id: 'native-fixture', text: `SYNTHETIC native ${source}`, source_refs: [] } });
    await codexCall('bridge_publish', event('codex'));
    await appendFile(paths.codex, JSON.stringify(codexRow('SYNTHETIC native codex increment')) + '\n');
    await until(() => service.core.sourceState(adminToken, 'copy-codex').checkpoint.visible_next === 1, 'Codex source indexed');
    agent.status = 'running';
    const human = { id: randomUUID(), role: 'user', source: { kind: 'user' }, content: [{ type: 'text', text: 'SYNTHETIC natural turn' }] };
    const preStep = listeners.get('agent/pre-step');
    const input = { step: 1, messages: [human], signal: new AbortController().signal };
    const enter = async () => ({ kind: 'enter', messages: [human] });
    const decision = await preStep(input, enter);
    assert.equal(decision.messages.length, historySyncOptions ? 3 : 2);
    const dshNotice = decision.messages[1];
    if (historySyncOptions) {
      const continued = await preStep({ ...input, step: 2, messages: [] }, async () => ({ kind: 'enter', messages: [] }));
      assert.ok(!continued.messages.some(message => message.content[0].text.includes('bridge_sync_history')));
    }
    assert.equal(dshNotice.source.kind, 'local-bridge');
    assert.equal(visibleMessage({ type: 'user/message', data: dshNotice }, 'dsh-zstd'), null);
    const dshCursors = await receiveBoth(dshCall, 'codex', dshNotice.content[0].text);
    await appendFile(paths.dsh, zstdCompressSync(Buffer.from(JSON.stringify({ type: 'user/message', data: dshNotice }) + '\n')));
    await dshCall('bridge_publish', event('dsh'));
    await appendFile(paths.dsh, zstdCompressSync(Buffer.from(JSON.stringify(dshRow('SYNTHETIC native dsh increment')) + '\n')));
    await until(() => service.core.sourceState(adminToken, 'copy-dsh').checkpoint.visible_next === 1, 'DSH source indexed without machine echo');
    const natural = await runCodexHook(hookEvent('UserPromptSubmit'), { config: codexConfig, historySync: codexSync });
    const codexNotice = natural.hookSpecificOutput?.additionalContext; assert.ok(codexNotice?.includes('<local_bridge_transport>'));
    const developer = { type: 'response_item', payload: { type: 'message', role: 'developer', content: [{ type: 'input_text', text: codexNotice }] } };
    assert.equal(visibleMessage(developer, 'codex-jsonl'), null);
    assert.ok(visibleMessage(codexRow(codexNotice), 'codex-jsonl'), 'An author quotation must remain visible');
    assert.ok(visibleMessage(dshRow(dshNotice.content[0].text), 'dsh-zstd'));
    await appendFile(paths.codex, JSON.stringify(developer) + '\n');
    const codexCursors = await receiveBoth(codexCall, 'dsh', codexNotice);
    for (const k of ['codex', 'dsh']) { const scan = await service.indexer.scan(`copy-${k}`); assert.equal(scan.records, 0); assert.equal(scan.queued.length, 0); }
    assert.equal(extraTurns, 0); assert.equal(extraInjections, 0);
    assert.equal(service.core.db.prepare('SELECT count(*) AS n FROM offers WHERE wake=1').get().n, 0);
    return { checkedAt: new Date().toISOString(), result: 'passed', directory, url: service.url, workspace,
      codexCursors, dshCursors, naturalAdapterPaths: ['Codex hook + private exchange + MCP proxy', 'loaded DSH plugin pre-step + tools'],
      machineNoticesExcludedFromHistory: 2, authorQuotationsRetained: true, unmanagedInjections: extraInjections,
      newNativeModelTurns: extraTurns, realNativeSessionsContacted: 0, wakeOffers: 0, codexQueueEnabled: false,
      syntheticHistoryIncrements: 2, syntheticBusinessEvents: 2, historyGeneration: 'snapshot-v1',
      recipientCursors: 0, nativeAdapters: true, processesClosed: true };
  } finally { await unload(); await service.close(); }
}
