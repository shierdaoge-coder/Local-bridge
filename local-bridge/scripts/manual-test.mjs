import { readFile, writeFile, mkdir, open, rename } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { BridgeClient } from '../src/client.mjs';
import { insist } from '../src/protocol.mjs';

const project = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
async function record(file, value) { const temp = file + '.tmp'; await writeFile(temp, JSON.stringify(value, null, 2) + '\n', { mode: 0o600 }); await rename(temp, file); }

/** Explicit operator steps only: never starts a model or enables idle wake. */
export async function manualTest(options, { client, runtime = path.join(project, '.runtime', 'manual-tests') } = {}) {
  insist(['pair', 'history', 'stop'].includes(options.phase), 'INVALID_INPUT', 'phase must be pair, history or stop');
  insist(typeof options.run === 'string' && /^[a-zA-Z0-9_-]{1,48}$/.test(options.run), 'INVALID_INPUT', 'A short unique run id is required');
  insist(options.source === undefined || ['codex', 'dsh'].includes(options.source), 'INVALID_INPUT', 'Invalid source harness');
  if (!client) {
    insist(typeof options.serviceConfig === 'string' && path.isAbsolute(options.serviceConfig), 'INVALID_INPUT', 'Absolute service config required');
    const config = JSON.parse(await readFile(options.serviceConfig, 'utf8'));
    client = new BridgeClient('http://127.0.0.1:' + config.port, { adminToken: config.admin_token });
  }
  const all = await client.request('/api/endpoints', { admin: true });
  const codex = all.find(e => e.id === options.codex && e.harness === 'codex'), dsh = all.find(e => e.id === options.dsh && e.harness === 'dsh');
  insist(codex && dsh && ![codex, dsh].some(e => e.id.startsWith('demo-')), 'INVALID_INPUT', 'Select exactly one registered real Codex endpoint and one DSH endpoint');
  const prefix = 'manual-' + options.run, dir = path.join(path.resolve(runtime), options.run);
  const directions = [{ from: codex, to: dsh, suffix: 'codex-dsh' }, { from: dsh, to: codex, suffix: 'dsh-codex' }];
  const operations = [];
  if (options.phase === 'pair') {
    insist([codex, dsh].every(e => !e.capabilities.can_enqueue && !e.capabilities.can_wake), 'CONFIG_CONFLICT', 'Use fresh endpoints with native delivery disabled; this helper does not replace an active setup');
    for (const direction of directions) operations.push({ path: '/api/grants', method: 'POST', body: {
      id: prefix + '-' + direction.suffix, from: direction.from.id, to: direction.to.id, content_types: ['history_sync', 'message'], kinds: ['event'], work_domains: ['text'], allow_wake: false, author_ref: 'operator-manual-test:' + options.run,
    } });
    // Set policies before enabling enqueue; this preparation cannot wake an idle model.
    for (const e of [codex, dsh]) operations.push({ path: '/api/endpoints/' + encodeURIComponent(e.id) + '/settings', method: 'PATCH', body: { history_sync: 'next_turn', message: 'next_turn', paused: false } });
    for (const e of [codex, dsh]) operations.push({ path: '/api/endpoints/' + encodeURIComponent(e.id) + '/capabilities', method: 'PATCH', body: { can_enqueue: true, can_wake: false, can_confirm_read: true } });
  } else if (options.phase === 'stop') {
    for (const e of [codex, dsh]) operations.push({ path: '/api/endpoints/' + encodeURIComponent(e.id) + '/settings', method: 'PATCH', body: { paused: true } });
    for (const direction of directions) operations.push({ path: '/api/grants/' + prefix + '-' + direction.suffix + '/revoke', method: 'POST', body: {} });
  } else {
    insist(['codex', 'dsh'].includes(options.source), 'INVALID_INPUT', 'history requires --source codex or dsh');
    const direction = directions.find(d => d.from.harness === options.source), archive = prefix + '-fixture-' + options.source;
    const file = path.join(dir, options.source + '-synthetic-history.jsonl');
    insist([codex, dsh].every(e => !e.capabilities.can_wake && e.settings.history_sync === 'next_turn' && e.settings.message === 'next_turn'), 'CONFIG_CONFLICT', 'History fixtures require both endpoints to remain in no-wake natural-turn test mode');
    if (options.write) {
      const pair = JSON.parse(await readFile(path.join(dir, 'pair.json'), 'utf8'));
      insist(pair.state === 'complete' && pair.codex === codex.id && pair.dsh === dsh.id, 'CONFIG_CONFLICT', 'A completed pairing receipt for these exact endpoints is required');
    }
    // Use synthetic text for the fixture source.
    operations.push({ path: '/api/archives', method: 'POST', body: { id: archive, owner: direction.from.id, generation: 'fixture-v1' } });
    operations.push({ path: '/api/sources', method: 'POST', body: { archive_id: archive, path: file, format: 'codex-jsonl' } });
    operations.push({ path: '/api/history-subscriptions', method: 'POST', body: { id: archive + '-subscription', archive_id: archive, to: direction.to.id, grant_id: prefix + '-' + direction.suffix } });
    operations.push({ path: '/api/sources/' + archive + '/scan', method: 'POST', body: { maxPages: 1 } });
  }
  const plan = { run: options.run, phase: options.phase, writes: Boolean(options.write), codex: codex.id, dsh: dsh.id, starts_model: false, idle_wake_enabled: false, operations };
  if (!options.write) return plan;
  await mkdir(dir, { recursive: true });
  const log = path.join(dir, options.phase + (options.source ? '-' + options.source : '') + '.json');
  const exclusive = await open(log, 'wx', 0o600); await exclusive.close();
  const progress = { ...plan, completed: [], state: 'in_progress' }; await record(log, progress);
  try {
    if (options.phase === 'history') {
      const file = path.join(dir, options.source + '-synthetic-history.jsonl');
      await writeFile(file, [
        { type: 'response_item', payload: { type: 'message', role: 'user', content: [{ type: 'input_text', text: '[SYNTHETIC TRANSPORT FIXTURE] ' + options.run + ' visible record zero; no instruction.' }] } },
        { type: 'response_item', payload: { type: 'message', role: 'assistant', phase: 'final', content: [{ type: 'output_text', text: '[SYNTHETIC TRANSPORT FIXTURE] ' + options.run + ' visible record one.' }] } },
      ].map(JSON.stringify).join('\n') + '\n', { flag: 'wx' });
    }
    for (const operation of operations) {
      const result = await client.request(operation.path, { method: operation.method, body: operation.body, admin: true });
      progress.completed.push({ path: operation.path, result }); await record(log, progress);
    }
    progress.state = 'complete'; await record(log, progress); return { ...plan, receipt_file: log, state: progress.state };
  } catch (e) {
    progress.state = 'partial_or_uncertain'; progress.error = e.code ?? 'REQUEST_FAILED'; await record(log, progress);
    // Deliberately do not retry mutations after an unknown HTTP outcome.
    const error = new Error('Inspect the saved operator receipt before retrying: ' + log); error.code = 'MANUAL_RECONCILIATION_REQUIRED'; throw error;
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const argv = process.argv.slice(2), flags = { '--phase': 'phase', '--run': 'run', '--codex': 'codex', '--dsh': 'dsh', '--source': 'source', '--service-config': 'serviceConfig' }, options = {};
  try {
    if (argv.includes('--help')) process.stdout.write('Manual operator helper (dry by default): --phase pair|history|stop --run UNIQUE_ID --codex ENDPOINT --dsh ENDPOINT --service-config ABS_FILE [--source codex|dsh] [--write]\n');
    else {
      for (let i = 0; i < argv.length; i++) { if (argv[i] === '--write') options.write = true; else { insist(flags[argv[i]] && argv[i + 1] && !argv[i + 1].startsWith('--'), 'INVALID_INPUT', 'Unknown or incomplete option'); options[flags[argv[i]]] = argv[++i]; } }
      process.stdout.write(JSON.stringify(await manualTest(options), null, 2) + '\n');
    }
  } catch (e) { process.stderr.write((e.code ?? 'OPERATOR_STEP_FAILED') + ': ' + e.message + '\n'); process.exitCode = 1; }
}
