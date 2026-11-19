import test from 'node:test';
import assert from 'node:assert/strict';
import { randomBytes, randomUUID } from 'node:crypto';
import { appendFile, mkdtemp, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { zstdCompressSync } from 'node:zlib';
import { createCandidateService } from '../experimental/source-automation/candidate-service.mjs';
import { planSessionPairs } from '../src/session-pairs.mjs';
import { apply as applyDsh } from '../src/dsh-plugin.mjs';
import { runCodexHook } from '../src/codex-hook.mjs';
import { CodexMcpProxy } from '../src/codex-mcp.mjs';
import { CODEX_TOOL_PREFIX } from '../src/adapters/codex.mjs';
import { BridgeClient } from '../src/client.mjs';
import { code } from './helpers.mjs';

const secret = () => randomBytes(32).toString('base64url');
const textBlock = text => [{ type: 'text', text }];
async function until(probe) {
  const deadline = Date.now() + 4000;
  while (!await probe()) {
    assert.ok(Date.now() < deadline, 'Isolated host did not become ready');
    await delay(20);
  }
}

test('pair declarations preserve explicit directions and reject wrong ownership or a third participant', () => {
  const endpoints = ['a', 'b', 'c'].map(id => ({ id, harness: 'codex' }));
  const base = { endpoints, archives: [{ id: 'archive-a', owner: 'a' }], bindings: [{ archive_id: 'archive-a' }] };
  const pair = { id: 'only-one-direction', author_ref: 'fixture', directions: [{ from: 'a', to: 'b', archive_id: 'archive-a' }] };
  const plan = pair => planSessionPairs({ ...base, sessionPairs: [pair] });
  assert.deepEqual(plan(pair).grants.map(({ from, to, allow_wake }) => [from, to, allow_wake]), [['a', 'b', false]]);
  assert.equal(plan(pair).subscriptions[0].archive_id, 'archive-a');
  assert.throws(() => plan({ ...pair, directions: [{ from: 'b', to: 'a', archive_id: 'archive-a' }] }), code('SOURCE_IDENTITY_MISMATCH'));
  assert.throws(() => plan({ ...pair, directions: [...pair.directions, { from: 'b', to: 'c' }] }), code('INVALID_INPUT'));
});

for (const harness of ['codex', 'dsh']) test(`one ${harness} plugin pairs two exact sessions and carries messages and history both ways`, async t => {
  const directory = await mkdtemp(join(tmpdir(), 'local-bridge-session-pairs-'));
  let service, unload;
  t.after(async () => {
    await unload?.(); await service?.close();
    const actual = await realpath(directory);
    assert.equal(dirname(actual), resolve(tmpdir()));
    assert.match(basename(actual), /^local-bridge-session-pairs-/);
    await rm(actual, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 });
  });
  const names = ['left', 'right'];
  const endpoints = names.map(id => ({ id, harness, native_session_id: randomUUID(), credential: secret(), capabilities: { can_enqueue: true } }));
  const archives = names.map(owner => ({ id: 'archive-' + owner, owner, generation: 'g1' }));
  const isCodex = harness === 'codex';
  const encode = row => {
    const bytes = Buffer.from(JSON.stringify(row) + '\n');
    return isCodex ? bytes : zstdCompressSync(bytes);
  };
  const config = { directory, workspace: 'same-harness-' + harness, adminToken: secret(), nativeAdapters: true, historyBatchNotices: true,
    endpoints, archives, bindings: archives.map(({ id }) => ({ archive_id: id, path: join(directory, id + (isCodex ? '.jsonl' : '.jsonl.zstd')),
      format: isCodex ? 'codex-jsonl' : 'dsh-zstd', initial_mode: 'from_now', author_ref: 'fixture' })),
    sessionPairs: [{ id: 'same-host', author_ref: 'fixture', directions: names.map((from, i) => ({ from, to: names[1 - i], archive_id: archives[i].id })) }],
    pumpOptions: { delayMs: 5, pollMs: 40 } };
  const pair = planSessionPairs(config);
  await Promise.all(config.bindings.map(({ path }, i) => writeFile(path, encode(isCodex
    ? { type: 'session_meta', payload: { id: endpoints[i].native_session_id } }
    : { type: 'session', version: 4, id: endpoints[i].native_session_id }))));
  service = await createCandidateService(config);
  await until(() => archives.every(({ id }) => !service.core.binding(config.adminToken, id).seeding));
  const side = { service_url: service.url, bindings: Object.fromEntries(endpoints.map(({ id, native_session_id, credential }, i) =>
    [native_session_id, { endpoint_id: id, credential, ...(isCodex ? { cwd: directory, transcript_path: config.bindings[i].path } : {}) }])) };
  let call, natural;
  if (isCodex) {
    const proxy = new CodexMcpProxy({ client: new BridgeClient(service.url), config: side });
    const hook = (i, hook_event_name, extra) => runCodexHook({ session_id: endpoints[i].native_session_id,
      cwd: directory, transcript_path: config.bindings[i].path, hook_event_name, turn_id: 'turn-' + i, ...extra }, { config: side });
    call = async (i, name, args = {}) => {
      const { hookSpecificOutput } = await hook(i, 'PreToolUse', { tool_name: CODEX_TOOL_PREFIX + name, tool_input: args, tool_use_id: randomUUID() });
      assert.equal(hookSpecificOutput.permissionDecision, 'allow');
      return proxy.execute(name, hookSpecificOutput.updatedInput);
    };
    natural = async i => (await hook(i, 'UserPromptSubmit')).hookSpecificOutput?.additionalContext;
  } else {
    const tools = new Map(), listeners = new Map();
    const agents = endpoints.map(({ native_session_id }) => ({ status: 'idle', session: { id: native_session_id },
      followup: () => assert.fail('Pairing cannot wake a model'),
      ctx: { on: (_event, fn) => { listeners.set(native_session_id, fn); return () => listeners.delete(native_session_id); } } }));
    const bindingFile = join(directory, 'bridge.json');
    await writeFile(bindingFile, JSON.stringify(side));
    unload = await applyDsh({ agents: new Map(agents.map(agent => [agent.session.id, agent])), tools: { register(tool) {
      tools.set(tool.name, tool); return () => tools.delete(tool.name);
    } } }, { binding_file: bindingFile, loadNative: async () => ({ createUserMessage: input => ({ id: randomUUID(), role: 'user', ...input }), adoptSessionEvent: event => event }) });
    await until(() => listeners.size === 2);
    call = async (i, name, args = {}) => {
      const response = JSON.parse(await tools.get(name).execute(args, { agent: agents[i] }));
      assert.equal(response.ok, true, JSON.stringify(response));
      return response.result;
    };
    natural = async i => {
      agents[i].status = 'running';
      const result = await listeners.get(agents[i].session.id)({ step: 1, signal: new AbortController().signal }, async () => ({ kind: 'enter', messages: [] }));
      agents[i].status = 'idle';
      return result.messages[0]?.content[0].text;
    };
  }
  for (const i of [0, 1]) assert.equal((await call(i, 'bridge_context')).endpoint.id, names[i]);
  for (const i of [0, 1]) {
    const target = 1 - i, grant = pair.grants.find(g => g.from === names[i]);
    const eventId = 'notice-' + names[i], archive_id = archives[i].id;
    const text = 'original ' + names[i];
    await call(i, 'bridge_publish', { id: eventId, to: names[target], grant_id: grant.id,
      payload: { kind: 'event', topic: 'pair.fixture', correlation_id: eventId, text: 'message ' + names[i], source_refs: [] } });
    await appendFile(config.bindings[i].path, encode(isCodex ? { type: 'response_item', payload: { type: 'message', role: 'user', content: textBlock(text) } }
      : { type: 'user/message', data: { id: randomUUID(), role: 'user', source: { kind: 'user' }, content: textBlock(text) } }));
    await until(() => service.core.sourceState(config.adminToken, archive_id).checkpoint.visible_next === 1);
    assert.ok((await natural(target))?.includes(archive_id));
    const { items: [event] } = await call(target, 'bridge_receive', { ids: [eventId] });
    assert.equal(event.payload.text, 'message ' + names[i]);
    const { runs, receipt } = await call(target, 'bridge_receive', { archive_id });
    assert.deepEqual(runs, [{ role: 'user', messages: [[0, text]] }]);
    await call(target, 'bridge_ack', { acknowledgements: [{ id: event.id, claim_token: event.claim.token }] });
    assert.equal((await call(target, 'bridge_history_cursor', { archive_id, generation: 'g1' })).read_through, -1);
    assert.equal((await call(target, 'bridge_ack', { receipt })).read_through, 0);
  }
  assert.equal(service.core.db.prepare('SELECT count(*) AS n FROM offers WHERE wake=1').get().n, 0);
});
