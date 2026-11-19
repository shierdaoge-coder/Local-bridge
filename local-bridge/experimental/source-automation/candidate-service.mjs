import { resolve, isAbsolute, dirname, join } from 'node:path';
import { realpath } from 'node:fs/promises';
import { CandidateCore } from './candidate-core.mjs';
import { inspectSourceHeader } from './bounded-reader.mjs';
import { createNaturalNativeRuntime } from './native-runtime.mjs';
import { BoundedSourceIndexer, SourcePump } from './source-pump.mjs';
import { createBridgeServer } from '../../src/server.mjs';
import { insist, canonical, settings } from '../../src/protocol.mjs';
import { planNativeRelay, planSessionPairs } from '../../src/session-pairs.mjs';
const setValue = values => canonical(values.map(canonical).sort());
const grantValue = input => ({ id: input.id, from_id: input.from, to_id: input.to,
  content_types: [...input.content_types].sort(), kinds: [...(input.kinds ?? ['event', 'request', 'result', 'cancel'])].sort(),
  work_domains: [...(input.work_domains ?? ['text'])].sort(), allow_wake: 0, author_ref: input.author_ref });

/** An independent service using explicitly bound archives, directions, and endpoints. */
export async function createCandidateService({ directory, adminToken, workspace = 'local-bridge',
  endpoints = [], archives = [], bindings = [], grants = [], subscriptions = [], sessionPairs = [], nativeTargets = {}, nativeRelay = false,
  queueLimits = {}, readerOptions = {}, pumpOptions = {}, nativeAdapters = false, historyBatchNotices = false,
  historyClaimMs, port = 0, logger = { error() {} } }, { batchScope } = {}) {
  insist(isAbsolute(directory), 'INVALID_INPUT', 'Candidate directory must be explicit and absolute');
  insist(Number.isInteger(port) && port >= 0 && port <= 65535 && port !== 4776, 'INVALID_INPUT', 'Candidate cannot use the active service port');
  insist(Array.isArray(bindings) && bindings.length <= 64, 'INVALID_INPUT', 'Service accepts at most 64 explicit history sources');
  insist(typeof nativeAdapters === 'boolean', 'INVALID_INPUT', 'Native candidate mode must be explicit');
  insist(typeof historyBatchNotices === 'boolean', 'INVALID_INPUT', 'History batch notification mode must be boolean');
  const pairPlan = planSessionPairs({ sessionPairs, endpoints, archives, bindings, grants, subscriptions });
  ({ grants, subscriptions } = pairPlan);
  const relayTargets = planNativeRelay({ nativeRelay, nativeAdapters, nativeTargets }, pairPlan.nativeDirections);
  const database = resolve(directory, 'candidate.sqlite');
  const activeDatabase = resolve(import.meta.dirname, '../../.runtime/bridge.sqlite');
  insist(database.toLowerCase() !== activeDatabase.toLowerCase(), 'CANDIDATE_DATABASE_REQUIRED', 'Candidate cannot open the active database');
  const core = new CandidateCore({ database, workspace, adminToken, allowedPaths: bindings.map(b => b.path), batchScope, historyClaimMs });
  let server, pump, nativeRuntime;
  try {
    await core.configureStartup(async () => {
    if (Object.keys(queueLimits).length) core.setQueueLimits(adminToken, queueLimits);
    for (const input of endpoints) {
      const relayTarget = relayTargets.has(input.id);
      const initialSettings = settings({ history_sync: 'next_turn', message: 'next_turn', ...input.settings });
      insist(!input.capabilities?.can_wake && (relayTarget
        || initialSettings.history_sync !== 'idle_wake' && initialSettings.message !== 'idle_wake'),
      'WAKE_NOT_AUTHORIZED', 'Only a configured independent relay target can use idle wake');
      const existing = core.db.prepare('SELECT * FROM endpoints WHERE id=?').get(input.id);
      if (existing) {
        insist(existing.harness === input.harness && existing.native_session_id === input.native_session_id, 'SOURCE_IDENTITY_MISMATCH', 'Persisted endpoint identity differs');
        core.authenticateEndpoint(input.id, input.credential);
        const { receive_history: savedReceive, ...savedPolicy } = settings(JSON.parse(existing.settings));
        const { receive_history: initialReceive, ...initialPolicy } = initialSettings;
        insist(canonical([...JSON.parse(existing.work_domains)].sort()) === canonical([...(input.work_domains ?? ['text'])].sort())
          && [savedReceive, initialReceive].every(value => value === undefined || typeof value === 'boolean')
          && (relayTarget || canonical(savedPolicy) === canonical(initialPolicy))
          && canonical(JSON.parse(existing.capabilities)) === canonical({ can_confirm_read: true, can_enqueue: false, can_wake: false, ...input.capabilities }),
        'NOT_AUTHORIZED', 'Persisted endpoint policy differs from explicit candidate configuration');
      } else core.register(adminToken, { ...input, settings: initialSettings,
        capabilities: { can_confirm_read: true, can_enqueue: false, can_wake: false, ...input.capabilities } });
    }
    for (const input of archives) {
      const existing = core.db.prepare('SELECT * FROM archives WHERE id=?').get(input.id);
      if (existing) insist(existing.owner_id === input.owner && existing.generation === input.generation, 'SOURCE_IDENTITY_MISMATCH', 'Persisted archive differs');
      else core.registerArchive(adminToken, input);
    }
    for (const input of bindings) {
      const exists = core.db.prepare('SELECT 1 FROM candidate_bindings WHERE archive_id=?').get(input.archive_id);
      if (!exists) await core.bindSource(adminToken, input);
      else {
        const b = core.binding(adminToken, input.archive_id), path = await realpath(input.path);
        insist(b.path === path && b.format === input.format && b.initial_mode === input.initial_mode,
          'SOURCE_IDENTITY_MISMATCH', 'Persisted source binding differs; use a new archive/generation explicitly');
        const current = await inspectSourceHeader(path, b.format);
        insist(current.native_session_id === b.native_session_id && current.header_sha256 === b.header_sha256
          && canonical(current.file_identity) === canonical(b.file_identity), 'SOURCE_REPLACED', 'Bound native source identity changed; require a new explicit archive/generation');
      }
    }
    for (const input of grants) {
      insist(input.allow_wake === false, 'WAKE_NOT_AUTHORIZED', 'Candidate grants cannot enable native wake');
      const exists = core.db.prepare('SELECT 1 FROM grants WHERE id=?').get(input.id);
      if (!exists) core.grant(adminToken, input);
    }
    const activeGrants = core.db.prepare('SELECT * FROM grants WHERE revoked=0').all().map(r => ({ id: r.id, from_id: r.from_id, to_id: r.to_id,
      content_types: JSON.parse(r.content_types).sort(), kinds: JSON.parse(r.kinds).sort(), work_domains: JSON.parse(r.work_domains).sort(), allow_wake: r.allow_wake, author_ref: r.author_ref }));
    const requestedGrantIds = new Set(grants.map(g => g.id));
    const preservedGrants = core.preservedScope(adminToken, 'grants').filter(g => !g.revoked && !requestedGrantIds.has(g.id)).map(r => ({ id: r.id, from_id: r.from_id, to_id: r.to_id,
      content_types: JSON.parse(r.content_types).sort(), kinds: JSON.parse(r.kinds).sort(), work_domains: JSON.parse(r.work_domains).sort(), allow_wake: r.allow_wake, author_ref: r.author_ref }));
    insist(setValue(activeGrants) === setValue([...grants.map(grantValue), ...preservedGrants]), 'NOT_AUTHORIZED', 'Persisted active grant scope differs from this complete configuration');
    insist(canonical(core.db.prepare('SELECT archive_id FROM candidate_bindings').all().map(r => r.archive_id).sort())
      === canonical(bindings.map(b => b.archive_id).sort()), 'SOURCE_BINDING_REQUIRED', 'Persisted and configured source lists differ; no implicit removal');
    for (const input of subscriptions) {
      const existing = core.db.prepare('SELECT * FROM history_subscriptions WHERE id=?').get(input.id);
      if (existing) insist(existing.archive_id === input.archive_id && existing.target_id === input.to
        && existing.grant_id === input.grant_id && existing.enabled === 1, 'SOURCE_IDENTITY_MISMATCH', 'Persisted subscription differs');
      else core.subscribeHistory(adminToken, input);
    }
    const activeSubscriptions = core.db.prepare('SELECT id,archive_id,target_id,grant_id FROM history_subscriptions WHERE enabled=1 ORDER BY id').all();
    const expectedSubscriptions = subscriptions.map(s => ({ id: s.id, archive_id: s.archive_id, target_id: s.to, grant_id: s.grant_id }));
    const requestedSubscriptionIds = new Set(subscriptions.map(s => s.id));
    const preservedSubscriptions = core.preservedScope(adminToken, 'history_subscriptions').filter(s => s.enabled && !requestedSubscriptionIds.has(s.id))
      .map(s => ({ id: s.id, archive_id: s.archive_id, target_id: s.target_id, grant_id: s.grant_id }));
    insist(setValue(activeSubscriptions) === setValue([...expectedSubscriptions, ...preservedSubscriptions]), 'NOT_AUTHORIZED', 'Persisted active subscription directions differ from this explicit configuration');
    });
    const indexer = new BoundedSourceIndexer({ core, adminToken, readerOptions: { maxVisibleRecords: 16, maxDecodedBytes: 64 * 1024, ...readerOptions,
      ...(Object.keys(nativeTargets).length ? { nativeReceiptDirectory: join(dirname(directory), 'native-receipts') } : {}) } });
    if (nativeAdapters) nativeRuntime = createNaturalNativeRuntime({ core, historyBatchNotices });
    server = createBridgeServer({ core, adminToken, indexer, codex: nativeRuntime?.codex, dsh: nativeRuntime?.dsh, port, logger });
    const url = await server.listen();
    nativeRuntime?.setUrl(url);
    const latest = new Map(), errors = new Map();
    if (bindings.length) {
      pump = new SourcePump({ ...pumpOptions, indexer,
        sources: bindings.map(input => { const b = core.binding(adminToken, input.archive_id); return { archive_id: b.archive_id, path: b.path }; }),
        onResult: (id, result) => { latest.set(id, result); errors.delete(id); },
        onError: (id, error) => { errors.set(id, error.code ?? 'UNKNOWN'); } });
      core.on('capacity', target => {
        for (const row of core.db.prepare("SELECT s.archive_id FROM history_subscriptions s JOIN archive_sources a ON a.archive_id=s.archive_id WHERE s.target_id=? AND s.enabled=1 AND a.status='paused_backpressure'").all(target))
          pump.resume(row.archive_id);
      });
      await pump.startWatching();
    }
    let closed = false;
    return { core, server, indexer, pump, url, database, latest, errors, nativeRuntime,
      async close() { if (closed) return; closed = true; await pump?.close(); await nativeRuntime?.close(); await server.close(); core.close(); } };
  } catch (error) { await pump?.close(); await nativeRuntime?.close(); await server?.close(); core.close(); throw error; }
}
