import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { open, realpath } from 'node:fs/promises';
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { createInterface } from 'node:readline';
import { setTimeout as delay } from 'node:timers/promises';
import { BridgeError, insist, integer, keys, object, text } from './protocol.mjs';

function rpcChannel(transmit, signal, diagnose) {
  const requests = new Map(), events = [];
  let serial = 0, failure;
  const fail = error => {
    failure ??= error;
    for (const { reject } of requests.values()) reject(failure);
  };
  const abort = () => fail(signal.reason);
  signal?.addEventListener('abort', abort, { once: true });
  function send(frame) {
    if (failure) throw failure;
    transmit(frame);
  }
  function receive(line) {
    try {
      const frame = JSON.parse(line);
      if (frame.id !== undefined && frame.method) {
        send({ id: frame.id, error: { code: -32601, message: 'The bridge is not an approval authority' } });
      } else if (frame.id !== undefined) {
        const request = requests.get(frame.id);
        if (frame.error) {
          diagnose?.('rpc error', frame.error);
          request?.reject(new BridgeError('NATIVE_REJECTED', request.method + ' was rejected by the native host'));
        }
        else request?.resolve(frame.result);
      } else if (frame.method && frame.params) {
        events.push({ method: frame.method, ...frame.params });
        if (frame.method === 'error' || frame.method === 'turn/completed') diagnose?.('status', frame.params.error ?? frame.params.turn?.error);
      }
    } catch { fail(new BridgeError('INVALID_RESPONSE', 'Native host emitted an invalid reply')); }
  }
  return {
    fail, receive,
    dispose() { signal?.removeEventListener('abort', abort); },
    events, notify: (method, params) => send({ method, params }),
    async request(method, params, timeoutMs) {
      if (failure) throw failure;
      const id = ++serial;
      let timer;
      try {
        return await new Promise((resolve, reject) => {
          requests.set(id, { resolve, reject, method });
          timer = setTimeout(() => reject(new BridgeError('NATIVE_TIMEOUT', method + ' did not return a receipt')), timeoutMs);
          send({ id, method, params });
        });
      } finally { clearTimeout(timer); requests.delete(id); }
    },
    async wait(find, deadline) {
      for (;;) {
        const match = find(events);
        if (match) return match;
        if (failure) throw failure;
        insist(Date.now() < deadline, 'NATIVE_TIMEOUT', 'Native turn completion was not observed');
        await delay(30);
      }
    },
  };
}

/** A private stdio client owns the native child. Unhandled approval requests fail closed. */
export function nativeRpc(executable, argv, { signal, ...options } = {}, diagnose) {
  signal?.throwIfAborted();
  const child = spawn(executable, argv, { ...options, shell: false, windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'] });
  const reader = createInterface({ input: child.stdout });
  const channel = rpcChannel(frame => child.stdin.write(JSON.stringify({ jsonrpc: '2.0', ...frame }) + '\n'), signal, diagnose);
  let closed = false;
  const exited = new Promise(done => {
    child.once('error', error => { closed = true; channel.fail(error); done(); });
    child.once('exit', () => { closed = true; channel.fail(new BridgeError('NATIVE_EXITED', 'Native host exited')); done(); });
  });
  child.stdin.on('error', channel.fail);
  reader.on('line', channel.receive);
  if (diagnose) child.stderr.on('data', chunk => diagnose('stderr', chunk.toString('utf8')));
  else child.stderr.resume();
  return {
    ...channel,
    async close() {
      channel.dispose();
      child.stdin.end();
      await Promise.race([exited, delay(3000, undefined, { ref: false })]);
      if (!closed) { child.kill(); await Promise.race([exited, delay(3000, undefined, { ref: false })]); }
      reader.close();
      insist(closed, 'NATIVE_EXIT_TIMEOUT', 'The owned native process did not exit');
    },
  };
}

function requestSignal(signal, ms) {
  const deadline = AbortSignal.timeout(ms);
  return signal ? AbortSignal.any([signal, deadline]) : deadline;
}

async function connectSocket(url, headers, signal) {
  const abort = requestSignal(signal, 5000);
  abort.throwIfAborted();
  const socket = new WebSocket(url, { headers });
  try {
    await new Promise((resolve, reject) => {
      const clean = () => {
        socket.removeEventListener('open', opened);
        socket.removeEventListener('error', failed);
        abort.removeEventListener('abort', stopped);
      };
      function opened() { clean(); resolve(); }
      function failed() { clean(); reject(new BridgeError('NATIVE_HOST_UNAVAILABLE', 'Could not connect to the authorized native host')); }
      function stopped() { clean(); reject(abort.reason); }
      socket.addEventListener('open', opened);
      socket.addEventListener('error', failed);
      abort.addEventListener('abort', stopped, { once: true });
    });
    return socket;
  } catch (error) { socket.close(); throw error; }
}

async function readConnection(target) {
  const config = object(target.connection, 'desktop connection');
  keys(config, ['url', 'credential_file'], 'desktop connection');
  text(config.url, 'desktop connection URL', 256);
  const url = new URL(config.url);
  insist(url.protocol === (target.harness === 'codex' ? 'ws:' : 'http:') && url.hostname === '127.0.0.1' && url.port
    && url.pathname === '/' && !url.username && !url.password && !url.search && !url.hash,
  'INVALID_INPUT', 'Use the explicitly authorized IPv4 loopback host origin');
  insist(typeof config.credential_file === 'string' && isAbsolute(config.credential_file),
    'INVALID_INPUT', 'Supply an absolute operator credential file path');
  const file = await open(config.credential_file, 'r');
  try {
    const info = await file.stat();
    insist(info.isFile() && info.size > 0 && info.size <= 1024, 'INVALID_INPUT', 'Expected a bounded operator credential file');
    const token = (await file.readFile('utf8')).trim();
    insist(/^[A-Za-z0-9_-]{32,512}$/.test(token), 'INVALID_INPUT', 'Invalid operator credential format');
    return { url, token };
  } finally { await file.close(); }
}

function namespacePath(path) {
  return process.platform === 'win32' && path.startsWith('\\\\?\\') ? path.slice(4) : path;
}
const samePath = (a, b) => process.platform === 'win32' ? a.toLowerCase() === b.toLowerCase() : a === b;
const contentText = content => (content ?? []).filter(block => block.type === 'text').map(block => block.text).join('\n');
export const codexNativeArgs = ['--sandbox', 'read-only', '--ask-for-approval', 'never', '--disable', 'plugins', 'app-server', '--stdio'];

function dshEvidence(rows, input, expected, queueRef) {
  if (!input) return;
  const start = rows.findLast(row => row.type === 'turn/start' && row.seq < input.seq);
  const end = start && rows.find(row => row.type === 'turn/end' && row.data?.turn === start.data?.turn);
  if (!end) return;
  const answers = rows.filter(row => row.type === 'assistant/message' && row.data?.turn === start.data.turn);
  const tools = rows.some(row => row.type === 'tool/call' && row.data?.turn === start.data.turn);
  insist(end.data.reason?.kind === 'completed' && !tools && answers.length === 1 && contentText(answers[0].data.message?.content).trim() === expected,
    'NATIVE_RECEIPT_MISMATCH', 'DSH completed without the exact read receipt');
  return { queue_ref: queueRef, turn_ref: String(start.data.turn), native_start: start.seq, native_complete: end.seq };
}

async function dshTurn({ rpc, target, prompt, expected, dispatch, deadline, timeoutMs }) {
  const sessionId = target.native_session_id;
  const init = await rpc.request('initialize', { cwd: target.cwd, provider: target.provider, model: target.model, maxTokens: 64 }, timeoutMs);
  insist(init.serverInfo?.name === 'deepseek-harness-sdk-runtime', 'CALLER_MISMATCH', 'Unexpected DSH SDK server');
  await rpc.request('session/resume', { sessionId }, timeoutMs);
  await dispatch();
  const { messageId } = await rpc.request('session/prompt', { sessionId, contentBlocks: [{ type: 'text', text: prompt }] }, timeoutMs);
  text(messageId, 'DSH enqueue receipt', 256);
  const evidence = await rpc.wait(events => {
    const rows = events.filter(event => event.method === 'session.event' && event.sessionId === sessionId).map(event => event.event);
    const input = rows.find(row => row.type === 'user/message' && row.data?.id === messageId);
    return dshEvidence(rows, input, expected, messageId);
  }, deadline);
  return evidence;
}

async function codexTurn({ rpc, target, prompt, expected, dispatch, deadline, timeoutMs }) {
  const threadId = target.native_session_id, connected = Boolean(target.connection);
  await rpc.request('initialize', { clientInfo: { name: 'local-bridge-native', version: '0.1.0' } }, timeoutMs);
  rpc.notify('initialized');
  const { thread } = await rpc.request('thread/read', { threadId, includeTurns: false }, timeoutMs);
  insist(thread?.id === threadId && typeof thread.path === 'string'
    && samePath(await realpath(namespacePath(thread.path)), target.archive),
  'CALLER_MISMATCH', 'Select the exact Codex thread and its archive');
  insist(thread.status?.type === (connected ? 'idle' : 'notLoaded'), 'NATIVE_TARGET_NOT_READY',
    connected ? 'The connected Codex thread must already be loaded and idle' : 'The independent Codex thread must be unloaded');
  const resumed = await rpc.request('thread/resume', { threadId, excludeTurns: true,
    ...(target.provider ? { modelProvider: target.provider } : {}), ...(target.model ? { model: target.model } : {}) }, timeoutMs);
  insist(resumed.thread?.id === threadId && (!target.provider || resumed.modelProvider === target.provider)
    && (!target.model || resumed.model === target.model), 'CALLER_MISMATCH', 'Codex resumed a different thread or model route');
  if (connected) {
    insist(resumed.thread.status?.type === 'idle', 'NATIVE_TARGET_NOT_READY', 'The loaded Codex thread is no longer idle');
  }
  await dispatch();
  const { turn } = await rpc.request('turn/start', { threadId, input: [{ type: 'text', text: prompt }] }, timeoutMs);
  text(turn?.id, 'Codex turn receipt', 256);
  return rpc.wait(events => {
    const native = events.filter(event => event.threadId === threadId);
    const end = native.find(event => event.method === 'turn/completed' && event.turn?.id === turn.id);
    if (!end) return;
    const start = native.find(event => event.method === 'turn/started' && event.turn?.id === turn.id);
    const items = native.filter(event => event.method === 'item/completed' && event.turnId === turn.id).map(event => event.item);
    const answers = items.filter(item => item.type === 'agentMessage');
    const tools = items.some(item => !['userMessage', 'agentMessage', 'reasoning', 'plan'].includes(item.type));
    const read = !connected || items.some(item => item.type === 'userMessage' && contentText(item.content) === prompt);
    insist(start && read && end.turn.status === 'completed' && !tools && answers.length === 1 && answers[0].text.trim() === expected,
      'NATIVE_RECEIPT_MISMATCH', 'Codex completed without the exact read receipt');
    return { queue_ref: turn.id, turn_ref: turn.id, native_start: start.turn.status, native_complete: end.turn.status };
  }, deadline);
}

async function dshDesktopTurn({ target, prompt, expected, dispatch, signal, deadline }, { url, token }) {
  const connect = new URL('/connect', url);
  connect.searchParams.set('token', token);
  connect.searchParams.set('interactions', 'none');
  const login = await fetch(connect, { redirect: 'manual', signal: requestSignal(signal, 5000) });
  const cookie = login.headers.getSetCookie()[0]?.split(';', 1)[0];
  insist(login.status === 302 && cookie?.startsWith('dsh-auth-'), 'NATIVE_UNAUTHORIZED', 'DSH did not issue an API operator credential');
  const muxUrl = new URL('/api/remote.mux', url);
  muxUrl.protocol = 'ws:';
  const socket = await connectSocket(muxUrl, { Cookie: cookie }, signal);
  async function call(method, request) {
    const rpcId = randomUUID();
    const response = await fetch(new URL('/api/' + method, url), {
      method: 'POST', redirect: 'error', headers: { Cookie: cookie, 'Content-Type': 'application/json' },
      body: JSON.stringify({ type: 'client-request', rpcId, method, payload: { args: { request } } }),
      signal: requestSignal(signal, 5000),
    });
    insist(response.ok, 'NATIVE_REJECTED', method + ' returned an unsuccessful response');
    const reply = await response.json();
    insist(reply.rpcId === rpcId && reply.result?.ok === true, 'NATIVE_REJECTED', method + ' was rejected by the authorized DSH host');
    return reply.result.value;
  }
  const sessionId = target.native_session_id;
  try {
    const before = await call('session/state', { sessionId });
    insist(before?.active === true && before.running === false && !before.queue?.length && !before.pendingInput?.length,
      'NATIVE_TARGET_NOT_READY', 'The connected DSH session must already be active and idle');
    await dispatch();
    const requestId = randomUUID();
    const accepted = await call('session/prompt', { requestId, sessionId, mode: 'queue', content: [{ type: 'text', text: prompt }] });
    insist(accepted?.accepted === true, 'NATIVE_REJECTED', 'DSH did not accept the prompt');
    while (Date.now() < deadline) {
      const state = await call('session/state', { sessionId });
      if (!state.running && state.cursor > before.cursor) {
        const page = await call('session/events', { address: { kind: 'session', sessionId }, maxMessages: 8 });
        const rows = page.records.filter(record => record.type === 'event').map(record => record.event);
        const input = rows.find(row => row.type === 'user/message' && row.data?.source?.rpcId === requestId
          && contentText(row.data?.content) === prompt);
        const evidence = dshEvidence(rows, input, expected, requestId);
        if (evidence) return evidence;
      }
      await delay(250, undefined, { signal });
    }
    throw new BridgeError('NATIVE_TIMEOUT', 'The connected DSH turn did not return a read receipt');
  } finally { socket.close(); }
}

async function desktopTurn(input, diagnose) {
  const auth = await readConnection(input.target);
  if (input.target.harness === 'dsh') return dshDesktopTurn(input, auth);
  const socket = await connectSocket(auth.url, { Authorization: 'Bearer ' + auth.token }, input.signal);
  const rpc = rpcChannel(frame => socket.send(JSON.stringify(frame)), input.signal, diagnose);
  socket.addEventListener('message', event => rpc.receive(event.data));
  const failed = () => rpc.fail(new BridgeError('NATIVE_DISCONNECTED', 'The authorized App Server disconnected'));
  socket.addEventListener('error', failed);
  socket.addEventListener('close', failed);
  try { return await codexTurn({ ...input, rpc }); }
  finally { rpc.dispose(); socket.close(); }
}

/** Request a receipt-only turn on a configured independent or already loaded native session. */
export async function deliverNativeSession(target, { prompt, expected, dispatch, timeoutMs = 150_000, diagnose, signal } = {}) {
  object(target, 'native target');
  insist(['codex', 'dsh'].includes(target.harness), 'INVALID_INPUT', 'Unsupported native host');
  integer(timeoutMs, 'native timeoutMs', 1000, 300_000);
  text(target.native_session_id, 'native session ID', 192);
  const deadline = Date.now() + timeoutMs;
  if (target.connection !== undefined) {
    insist(typeof target.archive === 'string' && isAbsolute(target.archive), 'INVALID_INPUT', 'Supply the exact desktop session archive path');
    const archive = await realpath(target.archive);
    return desktopTurn({ target: { ...target, archive }, prompt, expected, dispatch, timeoutMs, deadline,
      signal: requestSignal(signal, timeoutMs) }, diagnose);
  }
  for (const key of ['executable', 'home', 'cwd', 'archive']) insist(typeof target[key] === 'string' && isAbsolute(target[key]),
    'INVALID_INPUT', 'Supply an absolute native ' + key);
  const home = await realpath(target.home), archive = await realpath(target.archive), executable = await realpath(target.executable);
  const isDsh = target.harness === 'dsh';
  const inHome = relative(home, archive);
  insist(inHome.startsWith('sessions' + sep) && (!isDsh || basename(dirname(archive)) === target.native_session_id),
    'CALLER_MISMATCH', 'The native archive must belong to the configured session store');
  const name = isDsh ? /^(DeepSeek Harness(?:\.exe)?)$/i : /^codex(?:\.exe)?$/i;
  insist(name.test(basename(executable)), 'INVALID_INPUT', 'Select the installed native executable');
  const env = { ...process.env };
  delete env.LOCAL_BRIDGE_CONFIG;
  delete env.LOCAL_BRIDGE_URL;
  delete env.PLUGIN_DATA;
  let argv;
  if (isDsh) {
    insist(basename(archive) === 'session.v4.jsonl.zstd', 'INVALID_INPUT', 'Select a current DSH archive');
    text(target.provider, 'DSH provider', 128); text(target.model, 'DSH model', 128);
    Object.assign(env, { ELECTRON_RUN_AS_NODE: '1', DSH_HOME: home, DSH_PRIMARY_RUNTIME: '' });
    const entry = join(dirname(executable), 'resources', 'app.asar', 'dsh', 'node_modules', '@deepseek-ai', 'dsh-desktop-host', 'lib', 'cli.js');
    argv = ['--expose-internals', entry, '--profile', 'sdk'];
  } else {
    env.CODEX_HOME = home;
    argv = codexNativeArgs;
  }
  const rpc = nativeRpc(executable, argv, { cwd: resolve(target.cwd), env, signal }, diagnose);
  const input = { rpc, target: { ...target, archive }, prompt, expected, dispatch, timeoutMs, deadline };
  try { return await (isDsh ? dshTurn(input) : codexTurn(input)); }
  finally { await rpc.close(); }
}
