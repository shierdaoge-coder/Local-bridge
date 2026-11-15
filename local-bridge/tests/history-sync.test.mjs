import test from 'node:test';
import assert from 'node:assert/strict';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { mkdtemp, readFile, rmdir, unlink, writeFile } from 'node:fs/promises';
import { BridgeError } from '../src/protocol.mjs';
import { createHistorySync } from '../src/history-sync-host.mjs';
import { fixture, history, ADMIN, code } from './helpers.mjs';

const archive_id = 'archive-a';
const generation = 'g1';
const python = process.env.LOCAL_BRIDGE_TEST_PYTHON;

test('Python absence leaves enabled auto-sync quiet after one warning', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'local-bridge-sync-probe-'));
  const script = join(directory, 'history-sync.py');
  const source = await readFile(new URL('../src/history-sync.py', import.meta.url), 'utf8');
  await writeFile(script, source.replace('AUTO_SYNC_HISTORY = False', 'AUTO_SYNC_HISTORY = True'));
  t.after(async () => { await unlink(script); await rmdir(directory); });
  let warnings = 0;
  const sync = createHistorySync({ python: join(directory, 'missing-python'), script, onUnavailable: () => warnings++ });
  assert.deepEqual(await Promise.all([sync.autoEnabled(), sync.autoEnabled()]), [false, false]);
  assert.equal(await sync.autoEnabled(), false);
  assert.equal(warnings, 1);
  const manual = createHistorySync({ python: join(directory, 'missing-python') });
  assert.equal(await manual.autoEnabled(), false);
  await assert.rejects(manual.execute({ archive_id }, { call: () => assert.fail('No tool call without Python'), scope: 'test' }),
    code('PYTHON_UNAVAILABLE'));
});

test('history replay and confirmation status are bound to the target, request and source', t => {
  const { core, ca, cb, b } = fixture();
  t.after(() => core.close());
  const request_id = 'abc123'.repeat(5) + 'aa';
  core.publish(ca, history());
  const first = core.receive(cb, { archive_id, request_id });
  core.publish(ca, history(1));
  assert.deepEqual(core.receive(cb, { archive_id, request_id }), first);
  assert.throws(() => core.receive(cb, { archive_id, request_id, budgetBytes: 2048 }), code('CALL_CONFLICT'));
  assert.equal(core.historySyncStatus(ca, archive_id, generation, request_id).batch_state, 'unknown');
  assert.throws(() => core.acknowledge(ca, { receipt: first.receipt, request_id }), code('INVALID_CLAIM'));
  assert.equal(core.historySyncStatus(cb, archive_id, generation, request_id).batch_state, 'pending');
  core.setCapabilities(ADMIN, b.id, { can_enqueue: true });
  const attempt = core.beginOffer(b.id, core.pending(b.id).map(row => row.id).slice(0, 1), false);
  core.finishOffer(attempt.id, { accepted: true, native_ref: 'synthetic' });
  assert.deepEqual(core.historySourcesForSync(cb).map(row => ({ ...row })), [{ archive_id, unread_packets: 2 }]);
  core.setSettings(ADMIN, b.id, { receive_history: false });
  assert.throws(() => core.acknowledge(cb, { receipt: first.receipt, request_id }), code('NOT_AUTHORIZED'));
  core.setSettings(ADMIN, b.id, { receive_history: true });
  const saved = core.acknowledge(cb, { receipt: first.receipt, request_id });
  assert.equal(saved.read_through, 0);
  const next = core.connect(b.id, b.credential);
  assert.equal(core.historySyncStatus(next, archive_id, generation, request_id).batch_state, 'confirmed');
  assert.equal(core.acknowledge(next, { receipt: first.receipt, request_id }).duplicate, true);
  assert.throws(() => core.receive(next, { archive_id, request_id }), code('SYNC_ALREADY_CONFIRMED'));
  assert.deepEqual(core.receive(next, { archive_id }).range, [1, 1]);
});

test('Python retries the same receive and resolves an ambiguous ack with its batch status', { skip: !python }, async t => {
  const { core, ca, cb, b, clock } = fixture();
  t.after(() => core.close());
  // A gap keeps the contiguous cursor at -1 even when this batch was read.
  core.publish(ca, history(1));
  const sync = createHistorySync({ python });
  assert.equal(await sync.autoEnabled(), false);
  const calls = [], received = [];
  let connection = cb;
  const call = async (name, args) => {
    calls.push(name);
    if (name === 'bridge_context') return { endpoint: core.sessionInfo(connection) };
    if (name === 'bridge_receive') {
      const value = core.receive(connection, args);
      received.push({ id: args.request_id, value });
      if (received.length === 1) throw new BridgeError('ETIMEDOUT', 'Lost the first receive response');
      return value;
    }
    if (name === 'bridge_ack') {
      const value = core.acknowledge(connection, args);
      assert.equal(value.read_through, -1);
      clock.now += core.leaseMs;
      connection = core.connect(b.id, b.credential);
      throw new BridgeError('ETIMEDOUT', 'Lost the committed acknowledgement response');
    }
    assert.equal(name, 'bridge_history_cursor');
    return core.historySyncStatus(connection, args.archive_id, args.generation, args.request_id);
  };
  const result = await sync.execute({ archive_id }, { call, scope: 'receiver' });
  assert.deepEqual(received[0], received[1]);
  assert.equal(result.state, 'read');
  assert.equal(result.receipt, undefined);
  assert.deepEqual(calls, ['bridge_context', 'bridge_receive', 'bridge_receive']);
  assert.equal(core.historySyncStatus(cb, archive_id, generation, received[0].id).batch_state, 'pending');
  await assert.rejects(sync.execute({ batch_id: result.batch_id }, { call, scope: 'foreign' }), code('INVALID_CLAIM'));
  clock.now += 20_000;
  core.heartbeat(connection);
  clock.now += 20_000;
  const confirmed = await sync.execute({ batch_id: result.batch_id }, { call, scope: 'receiver' });
  assert.equal(confirmed.state, 'confirmed');
  assert.equal(confirmed.result.read_through, -1);
  assert.deepEqual(calls.slice(3), ['bridge_ack', 'bridge_ack', 'bridge_ack', 'bridge_history_cursor']);
  assert.deepEqual(core.historySourcesForSync(connection), []);
});
