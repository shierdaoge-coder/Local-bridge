import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { exerciseNativeCandidate } from './native-fixture.mjs';

async function exercise(historyBatchNotices, python) {
  const directory = await mkdtemp(join(tmpdir(), 'local-bridge-native-candidate-'));
  try {
    let historySyncOptions;
    if (python) {
      const script = join(directory, 'history-sync.py');
      const text = await readFile(new URL('../../src/history-sync.py', import.meta.url), 'utf8');
      await writeFile(script, text.replace('AUTO_SYNC_HISTORY = False', 'AUTO_SYNC_HISTORY = True'));
      historySyncOptions = { script, python };
    }
    const report = await exerciseNativeCandidate({ directory, historyBatchNotices, historySyncOptions });
    assert.deepEqual(report.codexCursors, [-1, -1, 0]); assert.deepEqual(report.dshCursors, [-1, -1, 0]);
    assert.equal(report.newNativeModelTurns, 0); assert.equal(report.machineNoticesExcludedFromHistory, 2);
  } finally {
    assert.equal(dirname(directory), resolve(tmpdir())); assert.ok(directory.startsWith(join(resolve(tmpdir()), 'local-bridge-native-candidate-')));
    await rm(directory, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 });
  }
}

test('candidate native gateways deliver both types on simulated natural host turns without history echo or wake', () => exercise(false));
test('both simulated native hosts use compact source notices and batch receipts', () => exercise(true));
const python = process.env.LOCAL_BRIDGE_TEST_PYTHON;
test('both simulated native hosts trigger the Python history sync and confirm only after a second tool call',
  { skip: !python }, () => exercise(true, python));
