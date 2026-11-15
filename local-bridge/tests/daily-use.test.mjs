import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { snapshotLedger } from '../scripts/daily-use.mjs';
import { TransportCore, PROTOCOL } from '../src/core.mjs';

test('daily-use preservation keeps newly queued work, read receipts and history cursors', async t => {
  const directory = mkdtempSync(join(tmpdir(), 'local-bridge-daily-'));
  const database = join(directory, 'candidate.sqlite');
  const adminToken = 'daily-test-admin';
  const core = new TransportCore({ database, workspace: 'daily-fixture', adminToken });
  t.after(() => {
    core.close();
    assert.equal(dirname(directory), resolve(tmpdir()));
    assert.ok(directory.startsWith(join(resolve(tmpdir()), 'local-bridge-daily-')));
    rmSync(directory, { recursive: true, force: true });
  });
  writeFileSync(join(directory, 'config.json'), JSON.stringify({ directory }));
  const sender = core.register(adminToken, { id: 'daily-a', harness: 'fixture', native_session_id: 'a' });
  const recipient = core.register(adminToken, { id: 'daily-b', harness: 'fixture', native_session_id: 'b' });
  core.grant(adminToken, { id: 'daily-grant', from: sender.id, to: recipient.id,
    content_types: ['message', 'history_sync'], kinds: ['event'], allow_wake: false, author_ref: 'daily-fixture' });
  core.registerArchive(adminToken, { id: 'daily-history', owner: sender.id, generation: 'v1' });
  const a = core.connect(sender.id, sender.credential), b = core.connect(recipient.id, recipient.credential);
  const event = id => ({ protocol: PROTOCOL, id, to: recipient.id, content_type: 'message', grant_id: 'daily-grant',
    payload: { kind: 'event', topic: 'daily', correlation_id: 'fixture', text: id, source_refs: [] } });
  core.publish(a, event('read-business'));
  core.publish(a, event('pending-business'));
  core.publish(a, { protocol: PROTOCOL, id: 'read-history', to: recipient.id, content_type: 'history_sync', grant_id: 'daily-grant',
    payload: { archive_id: 'daily-history', generation: 'v1', start_seq: 0, end_seq: 0, records: [{ seq: 0, role: 'user', text: 'actual work' }] } });
  const items = core.receive(b, { ids: ['read-business', 'read-history'] }).items;
  const result = core.acknowledge(b, items.map(item => ({ id: item.id, claim_token: item.claim.token })));
  assert.ok(result.every(row => row.ok));
  const snapshot = await snapshotLedger(directory, 'preserved');
  assert.equal(snapshot.messages, 3);
  const saved = new DatabaseSync(snapshot.path, { readOnly: true });
  try {
    assert.equal(saved.prepare('SELECT delivery FROM messages WHERE id=?').get('pending-business').delivery, 'queued');
    assert.equal(saved.prepare('SELECT delivery FROM messages WHERE id=?').get('read-business').delivery, 'received');
    assert.equal(saved.prepare('SELECT read_through FROM history_cursors').get().read_through, 0);
    assert.equal(saved.prepare('PRAGMA quick_check').get().quick_check, 'ok');
  } finally { saved.close(); }
});
