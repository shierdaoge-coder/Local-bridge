import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join, dirname, basename, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { TransportCore } from '../src/core.mjs';
import { ADMIN, fixture, message, history, ack, code } from './helpers.mjs';

test('both content types share a persistent recipient sequence and retain their type', t => {
  const f = fixture(); t.after(() => f.core.close());
  const one = f.core.publish(f.ca, history()), two = f.core.publish(f.ca, message());
  assert.equal(one.queue_seq, 1); assert.equal(two.queue_seq, 2);
  const got = f.core.receive(f.cb);
  assert.deepEqual(got.items.map(m => m.content_type), ['history_sync', 'message']);
  assert.equal(f.core.status(f.ca, one.id).delivery, 'queued', 'Interface delivery alone is not an acknowledged read');
});

test('publisher identity is authenticated; foreign status and ID retries reveal no message', t => {
  const f = fixture(); t.after(() => f.core.close()); const msg = message(); f.core.publish(f.ca, msg);
  assert.throws(() => f.core.publish(f.ca, { ...message(), from: 'spoofed-user' }), code('INVALID_INPUT'));
  assert.throws(() => f.core.publish(f.ca, { ...message(), created_at: 'fake' }), code('INVALID_INPUT'));
  const fourth = f.core.register(ADMIN, { id: 'fourth', harness: 'fixture', native_session_id: 'fourth' });
  const other = f.core.connect(fourth.id, fourth.credential);
  assert.throws(() => f.core.status(other, msg.id), code('NOT_FOUND'));
  assert.throws(() => f.core.publish(other, msg), code('ID_CONFLICT'));
});

test('same ID is idempotent; body conflicts do not overwrite the committed event', t => {
  const f = fixture(); t.after(() => f.core.close()); const msg = message();
  const first = f.core.publish(f.ca, msg); assert.equal(f.core.publish(f.ca, structuredClone(msg)).duplicate, true);
  assert.throws(() => f.core.publish(f.ca, { ...msg, payload: { ...msg.payload, text: 'changed' } }), code('ID_CONFLICT'));
  assert.equal(f.core.receive(f.cb).items.length, 1); assert.equal(f.core.status(f.ca, first.id).payload.text, msg.payload.text);
});

test('malformed history, hidden roles, over-budget payload and unsupported versions are rejected atomically', t => {
  const f = fixture(); t.after(() => f.core.close());
  const gap = history(0, 1); gap.payload.records.pop(); assert.throws(() => f.core.publish(f.ca, gap), code('INVALID_INPUT'));
  const hidden = history(); hidden.payload.records[0].role = 'system'; assert.throws(() => f.core.publish(f.ca, hidden), code('INVALID_INPUT'));
  assert.throws(() => f.core.publish(f.ca, message({}, { text: '汉'.repeat(1000) })), code('PAYLOAD_TOO_LARGE'));
  assert.throws(() => f.core.publish(f.ca, message({ protocol: 'unknown' })), code('UNSUPPORTED_PROTOCOL'));
  assert.equal(f.core.receive(f.cb).items.length, 0);
});

test('endpoint credentials, epochs and leases reject old instances', t => {
  const f = fixture(); t.after(() => f.core.close());
  assert.throws(() => f.core.connect(f.b.id, 'bad'), code('UNAUTHORIZED'));
  const next = f.core.connect(f.b.id, f.b.credential);
  assert.throws(() => f.core.receive(f.cb), code('STALE_CONNECTION'));
  f.clock.now += 30_001; assert.throws(() => f.core.receive(next), code('STALE_CONNECTION'));
  const live = f.core.connect(f.b.id, f.b.credential); assert.ok(f.core.heartbeat(live).lease_until > f.clock.now);
});

test('claims and partial acknowledgements do not advance unread historical gaps', t => {
  const f = fixture(); t.after(() => f.core.close());
  f.core.publish(f.ca, history(2, 3)); f.core.publish(f.ca, history(0, 1));
  const batch = f.core.receive(f.cb).items;
  const results = f.core.acknowledge(f.cb, [ack(batch[0]), { id: batch[1].id, claim_token: 'forged' }]);
  assert.deepEqual(results.map(r => r.ok), [true, false]);
  assert.equal(f.core.historyCursor(f.cb, 'archive-a', 'g1'), -1);
  f.core.acknowledge(f.cb, [ack(batch[1])]);
  assert.equal(f.core.historyCursor(f.cb, 'archive-a', 'g1'), 3);
  assert.equal(f.core.acknowledge(f.cb, [ack(batch[1])])[0].ok, true);
});

test('ordinary message references cannot advance a history cursor', t => {
  const f = fixture(); t.after(() => f.core.close());
  f.core.publish(f.ca, message({}, { source_refs: [{ archive_id: 'archive-a', start_line: 1, end_line: 100 }] }));
  f.core.acknowledge(f.cb, f.core.receive(f.cb).items.map(m => ack(m, { outcome: 'completed' })));
  assert.equal(f.core.historyCursor(f.cb, 'archive-a', 'g1'), -1);
});

test('history ownership, generation and conflicting source records are enforced', t => {
  const f = fixture(); t.after(() => f.core.close());
  const wrong = history(); wrong.payload.generation = 'other'; assert.throws(() => f.core.publish(f.ca, wrong), code('SOURCE_MISMATCH'));
  f.core.publish(f.ca, history());
  const conflict = history(); conflict.payload.records[0].text = 'rewritten'; assert.throws(() => f.core.publish(f.ca, conflict), code('SOURCE_CONFLICT'));
  assert.equal(f.core.receive(f.cb).items.length, 1);
});

test('declared work scope and recipient domain changes are checked before delivery', t => {
  const f = fixture(); t.after(() => f.core.close());
  const request = message({}, { kind: 'request', work_domain: 'engineering', authority_ref: { grant_id: 'a-b', source_ref: 'fixture-user-instruction' } });
  assert.throws(() => f.core.publish(f.ca, request), code('WORK_DOMAIN_MISMATCH'));
  request.payload.work_domain = 'text'; f.core.publish(f.ca, request);
  f.core.setWorkDomains(ADMIN, 'dsh-b', ['engineering']);
  assert.equal(f.core.receive(f.cb).items.length, 0);
  assert.equal(f.core.status(f.ca, request.id).reason, 'WORK_DOMAIN_MISMATCH');
});

test('revocation and expiration prevent unread work, without erasing an already received task', t => {
  const f = fixture(); t.after(() => f.core.close());
  const expires = message({ expires_at: new Date(f.clock.now + 100).toISOString() }); f.core.publish(f.ca, expires);
  f.clock.now += 101; assert.equal(f.core.receive(f.cb).items.length, 0); assert.equal(f.core.status(f.ca, expires.id).delivery, 'delivery_failed');
  const msg = message(); f.core.publish(f.ca, msg); const item = f.core.receive(f.cb).items[0];
  f.core.acknowledge(f.cb, [ack(item, { outcome: 'accepted' })]); f.core.revoke(ADMIN, 'a-b');
  assert.equal(f.core.receive(f.cb).items.length, 0);
  const status = f.core.status(f.ca, msg.id); assert.equal(status.delivery, 'received'); assert.equal(status.outcome, 'needs_input');
});

test('receive budgets never truncate a payload or silently skip the first pending item', t => {
  const f = fixture(); t.after(() => f.core.close());
  for (let i = 0; i < 5; i++) f.core.publish(f.ca, message());
  assert.throws(() => f.core.receive(f.cb, { budgetBytes: 1 }), code('BUDGET_TOO_SMALL'));
  const received = f.core.receive(f.cb); assert.equal(received.items.length, 4); assert.equal(received.remaining, 1);
  assert.equal(f.core.receive(f.cb).items.length, 1);
});

test('read confirmation, task completion and immutable terminal outcomes are separate', t => {
  const f = fixture(); t.after(() => f.core.close()); const msg = message(); f.core.publish(f.ca, msg);
  const item = f.core.receive(f.cb).items[0]; f.core.acknowledge(f.cb, [ack(item)]);
  assert.equal(f.core.status(f.ca, msg.id).outcome, null);
  f.core.acknowledge(f.cb, [ack(item, { outcome: 'completed' })]);
  assert.equal(f.core.acknowledge(f.cb, [ack(item, { outcome: 'failed' })])[0].error, 'TERMINAL_STATE');
  assert.equal(f.core.status(f.ca, msg.id).outcome, 'completed');
});

test('restart retains the queue, receipts and settings; old live connections and claims expire', t => {
  const directory = mkdtempSync(join(tmpdir(), 'local-bridge-core-'));
  const path = join(directory, 'bridge.sqlite'), f = fixture({ database: path });
  let core;
  t.after(() => { core?.close(); f.core.close(); assert.equal(dirname(directory), resolve(tmpdir())); assert.ok(basename(directory).startsWith('local-bridge-core-')); rmSync(directory, { recursive: true, force: true }); });
  const one = f.core.publish(f.ca, history()), two = f.core.publish(f.ca, message());
  const batch = f.core.receive(f.cb).items; f.core.acknowledge(f.cb, [ack(batch[0], { outcome: 'completed' })]);
  f.core.setSettings(ADMIN, 'dsh-b', { history_sync: 'idle_wake', message: 'next_turn', paused: true }); f.core.close();
  core = new TransportCore({ database: path, adminToken: ADMIN, now: () => f.clock.now });
  assert.throws(() => core.receive(f.cb), code('STALE_CONNECTION'));
  const connection = core.connect('dsh-b', f.b.credential);
  assert.equal(core.historyCursor(connection, 'archive-a', 'g1'), 0);
  assert.equal(core.endpoint('dsh-b').settings.paused, true);
  assert.equal(core.status(connection, one.id).delivery, 'received');
  const pending = core.receive(connection).items; assert.deepEqual(pending.map(x => x.id), [two.id]);
  assert.equal(core.acknowledge(connection, [ack(batch[1])])[0].error, 'INVALID_CLAIM');
});

test('observer failures cannot roll back a committed publish', t => {
  const f = fixture(); t.after(() => f.core.close()); f.core.on('activity', () => { throw Error('observer failed'); });
  const msg = message(); f.core.publish(f.ca, msg); assert.equal(f.core.status(f.ca, msg.id).delivery, 'queued');
});

test('a second service cannot invalidate live connections by opening the same database', t => {
  const directory = mkdtempSync(join(tmpdir(), 'local-bridge-owner-')), database = join(directory, 'bridge.sqlite');
  const f = fixture({ database });
  t.after(() => { f.core.close(); assert.equal(dirname(directory), resolve(tmpdir())); assert.ok(basename(directory).startsWith('local-bridge-owner-')); rmSync(directory, { recursive: true, force: true }); });
  assert.throws(() => new TransportCore({ database, adminToken: ADMIN }), code('CORE_ALREADY_RUNNING'));
  assert.ok(f.core.heartbeat(f.ca).lease_until > f.clock.now);
});

test('results must retain request correlation, work domain and authorized reverse direction', t => {
  const f = fixture(); t.after(() => f.core.close());
  const request = message({}, { kind: 'request', work_domain: 'text', authority_ref: { grant_id: 'a-b', source_ref: 'fixture-user-instruction' } });
  f.core.publish(f.ca, request);
  const reply = message({ to: 'codex-a', grant_id: 'b-a' }, { kind: 'result', reply_to: request.id, work_domain: 'text' });
  assert.equal(f.core.publish(f.cb, reply).to, 'codex-a');
  const wrong = message({ to: 'codex-a', grant_id: 'b-a' }, { kind: 'result', reply_to: request.id, work_domain: 'engineering' });
  assert.throws(() => f.core.publish(f.cb, wrong), code('INVALID_REFERENCE'));
});
