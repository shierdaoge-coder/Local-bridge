# local-bridge 接入 Codex

先按 [README](README.md) 准备共享服务。选择一个要接入的会话，核对其 UUID、工作目录和原生会话归档路径。三者应与 Codex 报给 Hooks 的值一致。

1. 从项目根目录运行 `node scripts/build-codex.mjs --plugin-data "ABSOLUTE_CODEX_PLUGIN_DATA"`。`ABSOLUTE_CODEX_PLUGIN_DATA` 是 Codex 为 `local-bridge@local-bridge` 提供的 `PLUGIN_DATA` 目录的绝对路径。默认生成 `dist/codex`。Windows 的构建路径需避开空格和命令行元字符。构建配置使用当前 Node 的绝对路径，移动构建后需重建。
2. 通过 Codex 的本地市场安装。运行 `codex plugin marketplace add "ABSOLUTE_CODEX_MARKETPLACE_DIRECTORY"`，再运行 `codex plugin add local-bridge@local-bridge`。`ABSOLUTE_CODEX_MARKETPLACE_DIRECTORY` 是生成的市场根目录的绝对路径，默认对应 `dist/codex`，其中包含 `.agents/plugins/marketplace.json`。
3. 在 `ABSOLUTE_CODEX_PLUGIN_DATA` 目录下新建 `bridge.json`。用共享服务配置中 Codex 端点的 `native_session_id` 替换对象的键，填写同一端点的地址和凭据。`ABSOLUTE_WORKSPACE_DIRECTORY` 和 `ABSOLUTE_CODEX_TRANSCRIPT_PATH` 分别使用所选会话 Hooks 提供的 `cwd` 和 `transcript_path`。

```json
{
  "service_url": "http://127.0.0.1:4780",
  "bindings": {
    "CODEX_SESSION_UUID": {
      "endpoint_id": "codex-local",
      "credential": "REPLACE_WITH_CODEX_ENDPOINT_CREDENTIAL",
      "cwd": "ABSOLUTE_WORKSPACE_DIRECTORY",
      "transcript_path": "ABSOLUTE_CODEX_TRANSCRIPT_PATH"
    }
  }
}
```

4. 在 Codex 里按正常流程审阅并启用该插件的 Hooks，再启用 `local_bridge` MCP 服务。更改了构建路径或绑定时，刷新 MCP 服务以读取新配置。
5. 可以运行 `node scripts/check-codex-readiness.mjs --plugin-data "ABSOLUTE_CODEX_PLUGIN_DATA" --service-url http://127.0.0.1:4780`，检查生成文件、私密绑定和服务状态。检查不显示凭据。会话内的 `bridge_context` 可核对实际绑定。

在该端构建使用的 `src/history-sync.py` 中设 `AUTO_SYNC_HISTORY = True` 可启用自动原文同步。需要指定解释器时，在 `ABSOLUTE_CODEX_PLUGIN_DATA` 目录下创建 `history-sync.json`，填写 `{"python_path": "ABSOLUTE_PYTHON_EXECUTABLE"}`，把占位符换成目标机器上 Python 3 可执行文件的绝对路径。保留默认开关时，仍可手动领取原文或使用通讯。

## 由 Codex 启动共享服务

在该端的私密绑定目录按 [共享服务](SHARED-SERVICE.md) 配置 `service-start.json`。MCP 在本地服务拒绝连接时检查归属并尝试启动监督进程。
