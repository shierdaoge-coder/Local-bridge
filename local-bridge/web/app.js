const $ = selector => document.querySelector(selector);
let credential = sessionStorage.getItem('bridge-admin') || '';
async function api(path, options = {}) {
  const response = await fetch(path, { ...options, headers: { Authorization: `Bearer ${credential}`, ...(options.body ? { 'Content-Type': 'application/json' } : {}), ...options.headers } });
  const value = await response.json();
  if (!response.ok || !value.ok) throw new Error(response.status === 401 ? '管理凭据无效，请重新连接。' : value.message || '请求失败，请重试。');
  return value.result;
}
function notice(text) { $('#notice').textContent = text; }
function card(endpoint) {
  const fragment = $('#endpoint-template').content.cloneNode(true), node = fragment.querySelector('article');
  node.querySelector('.harness').textContent = endpoint.harness === 'codex' ? 'Codex' : endpoint.harness === 'dsh' ? 'DSH' : endpoint.harness;
  node.querySelector('.identity').textContent = endpoint.id;
  const status = node.querySelector('.status'); status.textContent = endpoint.online ? '在线' : '尚未连接'; status.classList.toggle('online', endpoint.online);
  for (const key of ['history_sync', 'message']) node.querySelector(`[name=${key}]`).value = endpoint.settings[key];
  const receiveHistory = node.querySelector('[name=receive_history]');
  const historyTiming = node.querySelector('[name=history_sync]');
  const updateHistoryTiming = () => { historyTiming.disabled = !receiveHistory.checked; };
  receiveHistory.checked = endpoint.settings.receive_history !== false;
  receiveHistory.addEventListener('change', updateHistoryTiming);
  updateHistoryTiming();
  node.querySelector('[name=paused]').checked = endpoint.settings.paused;
  node.querySelector('.capability').textContent = endpoint.capabilities.can_wake ? '主动唤醒能力已登记。实际投递仍按授权和忙碌状态处理。' : '当前端尚未验证主动唤醒能力。策略可以先保存，消息仍会留在队列中。';
  node.querySelector('.save').addEventListener('click', async event => {
    event.target.disabled = true; const result = node.querySelector('.saved'); result.textContent = '正在保存…';
    try {
      const policy = { receive_history: receiveHistory.checked, history_sync: historyTiming.value,
        message: node.querySelector('[name=message]').value, paused: node.querySelector('[name=paused]').checked };
      await api(`/api/endpoints/${encodeURIComponent(endpoint.id)}/settings`, { method: 'PATCH', body: JSON.stringify(policy) });
      result.textContent = '已保存';
    } catch (error) { result.textContent = error.message; }
    finally { event.target.disabled = false; }
  });
  return fragment;
}
async function load() {
  const endpoints = await api('/api/endpoints');
  $('#endpoints').replaceChildren(...endpoints.map(card));
  $('#summary').textContent = endpoints.length ? `${endpoints.length} 个已登记端点` : '尚未登记端点；接入适配器后在这里设置。';
  $('#workspace').hidden = false; $('#login').hidden = true; notice('');
  sessionStorage.setItem('bridge-admin', credential);
}
$('#login-form').addEventListener('submit', async event => {
  event.preventDefault(); credential = $('#credential').value.trim();
  try { await load(); $('#credential').value = ''; } catch (error) { notice(error.message); }
});
$('#refresh').addEventListener('click', () => load().catch(error => notice(error.message)));
$('#logout').addEventListener('click', () => { credential = ''; sessionStorage.removeItem('bridge-admin'); $('#endpoints').replaceChildren(); $('#workspace').hidden = true; $('#login').hidden = false; notice('已断开设置页。后台队列继续保留。'); });
if (credential) load().catch(error => { sessionStorage.removeItem('bridge-admin'); notice(error.message); });
