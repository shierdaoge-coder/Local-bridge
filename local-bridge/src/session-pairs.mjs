import { canonical, choices, digest, insist, keys, object, text } from './protocol.mjs';

/** Expand operator-declared directions into the existing grants and subscriptions. */
export function planSessionPairs({ sessionPairs = [], endpoints = [], archives = [], bindings = [], grants = [], subscriptions = [] }) {
  insist(Array.isArray(sessionPairs), 'INVALID_INPUT', 'sessionPairs must be an array');
  const plannedGrants = [...grants];
  const plannedSubscriptions = [...subscriptions];
  const nativeDirections = new Map();
  const endpointById = new Map(endpoints.map(endpoint => [endpoint.id, endpoint]));
  const archiveById = new Map(archives.map(archive => [archive.id, archive]));
  const bound = new Set(bindings.map(binding => binding.archive_id));
  const grantIds = new Set(grants.map(grant => grant.id));
  const subscriptionIds = new Set(subscriptions.map(subscription => subscription.id));
  const pairIds = new Set();

  for (const pair of sessionPairs) {
    object(pair, 'session pair');
    keys(pair, ['id', 'author_ref', 'directions'], 'session pair');
    text(pair.id, 'pair id', 64);
    text(pair.author_ref, 'author_ref', 1024);
    insist(!pairIds.has(pair.id), 'CONFIG_CONFLICT', 'Duplicate session pair ID');
    pairIds.add(pair.id);
    insist(Array.isArray(pair.directions) && pair.directions.length >= 1 && pair.directions.length <= 2,
      'INVALID_INPUT', 'A pair needs one or two explicit directions');
    const participants = new Set();
    for (const direction of pair.directions) {
      object(direction, 'pair direction');
      keys(direction, ['from', 'to', 'archive_id', 'kinds', 'work_domains', 'native_delivery'], 'pair direction');
      const { from, to, archive_id, native_delivery = false } = direction;
      const sender = endpointById.get(from), receiver = endpointById.get(to);
      insist(sender && receiver && from !== to, 'INVALID_INPUT', 'Select two distinct registered endpoint identities');
      participants.add(from);
      participants.add(to);
      insist(typeof native_delivery === 'boolean' && (!native_delivery || sender.harness !== receiver.harness),
        'INVALID_INPUT', 'Native delivery is an explicit cross-harness option');
      if (archive_id !== undefined) {
        insist(archiveById.get(archive_id)?.owner === from && bound.has(archive_id),
          'SOURCE_IDENTITY_MISMATCH', 'The direction must use a registered original source owned by its sender');
      }
      const id = `pair-${pair.id}-${digest(canonical([from, to])).slice(0, 16)}`;
      insist(!grantIds.has(id), 'CONFIG_CONFLICT', 'Duplicate pairing direction or grant ID');
      grantIds.add(id);
      plannedGrants.push({ id, from, to, author_ref: pair.author_ref, allow_wake: false,
        content_types: archive_id === undefined ? ['message'] : ['message', 'history_sync'],
        ...(direction.kinds === undefined ? {} : { kinds: choices(direction.kinds, ['event', 'request', 'result', 'cancel'], 'pair message kinds') }),
        ...(direction.work_domains === undefined ? {} : { work_domains: choices(direction.work_domains, ['text', 'engineering'], 'pair work domains') }),
      });
      if (archive_id !== undefined) {
        const subscriptionId = id + '-history';
        insist(!subscriptionIds.has(subscriptionId), 'CONFIG_CONFLICT', 'Duplicate pairing subscription ID');
        subscriptionIds.add(subscriptionId);
        plannedSubscriptions.push({ id: subscriptionId, archive_id, to, grant_id: id });
      }
      if (native_delivery) nativeDirections.set(id, { from, to, archive_id });
    }
    insist(participants.size === 2, 'INVALID_INPUT', 'Pair directions must use the same two endpoints');
  }
  return { grants: plannedGrants, subscriptions: plannedSubscriptions, nativeDirections };
}

/** Only recipients of explicitly declared native directions can use the independent relay. */
export function planNativeRelay({ nativeRelay = false, nativeAdapters, nativeTargets = {} }, nativeDirections) {
  insist(typeof nativeRelay === 'boolean', 'INVALID_INPUT', 'nativeRelay must be boolean');
  object(nativeTargets, 'nativeTargets');
  const recipients = new Set([...nativeDirections.values()].map(direction => direction.to));
  const targets = new Set([...recipients].filter(id => nativeAdapters && Object.hasOwn(nativeTargets, id)));
  insist(!nativeRelay || targets.size > 0 && targets.size === recipients.size,
    'CONFIG_CONFLICT', 'Automatic native delivery needs a configured runtime for each native direction');
  return targets;
}
