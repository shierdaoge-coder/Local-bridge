# local-bridge 接入 DeepSeek Harness

先按 [README](README.md) 准备共享服务，并选定 DSH 会话及其实际使用的 profile。插件调用 DSH 安装内的 `@deepseek-ai/dsh-session-persistence`，构建时需提供该 SDK 的实际入口。Windows 安装的入口通常以 `resources/app.asar/dsh/node_modules/@deepseek-ai/dsh-session-persistence/lib/index.js` 结尾。

1. 从项目根目录运行构建命令。`ABSOLUTE_DSH_SDK_ENTRY` 是上面所述 SDK 入口文件的绝对路径。`ABSOLUTE_DSH_PRIVATE_BRIDGE_JSON` 是准备用来保存私密绑定的 `bridge.json` 文件的完整绝对路径。省略 `--binding-file` 时，构建使用输出目录内的 `PLUGIN_DATA/bridge.json`。
2. 运行安装命令预览。`ABSOLUTE_BUILT_DSH_DIRECTORY` 是构建输出目录的绝对路径，默认对应 `dist/dsh`。`ABSOLUTE_DSH_PROFILE` 是目标 profile 目录的绝对路径，其中包含该 profile 的 `package.json`。确认预览后加 `--write`，脚本会登记本地包并保存 profile 配置备份。
3. 在指定的 `bridge.json` 中填写共享服务地址和 DSH 端点绑定，用真实会话 ID 作键。重新加载 DSH 插件后，可用 `bridge_context` 核对绑定。

```text
node scripts/build-dsh.mjs --sdk-entry "ABSOLUTE_DSH_SDK_ENTRY" --binding-file "ABSOLUTE_DSH_PRIVATE_BRIDGE_JSON"
node scripts/install-dsh.mjs --source "ABSOLUTE_BUILT_DSH_DIRECTORY" --profile-dir "ABSOLUTE_DSH_PROFILE"
```

```json
{
  "service_url": "http://127.0.0.1:4780",
  "bindings": {
    "DSH_SESSION_ID": {
      "endpoint_id": "dsh-local",
      "credential": "REPLACE_WITH_DSH_ENDPOINT_CREDENTIAL"
    }
  }
}
```

持久待发账本位于绑定文件旁的 `bridge-outbox.sqlite`。连接恢复后，待发消息沿用原 ID 重试。

## 由 DSH 启动共享服务

支持 DSH 0.2.1-rc.3 与 0.2.2-rc.3。新安装使用 [共享服务](SHARED-SERVICE.md) 的 `service_directory` 配置。已有安装可继续使用下面的 `trial_directory` 设置。

在 DSH 的 `bridge.json` 所在目录创建私密 `service-start.json`。`ABSOLUTE_PREPARED_SERVICE_DIRECTORY` 取自共享服务 `prepare` 命令输出的 `directory` 字段。`ABSOLUTE_NODE_EXECUTABLE` 是 Node 24 可执行文件的绝对路径，可用 `node -p "process.execPath"` 查看。

```json
{
  "node_path": "ABSOLUTE_NODE_EXECUTABLE",
  "trial_directory": "ABSOLUTE_PREPARED_SERVICE_DIRECTORY"
}
```

重载插件后，绑定会话的心跳在连接被拒绝时可拉起本地监督进程。可用 `node scripts/daily-use.mjs stop` 停止服务与自动拉起，用 `resume` 允许再次拉起。监督进程在自己的服务目录记录运行状态。

要开启自动原文同步，在该端构建使用的 `src/history-sync.py` 中设 `AUTO_SYNC_HISTORY = True`。需要指定解释器时，在 `bridge.json` 所在目录创建 `history-sync.json`，填写 `{"python_path": "ABSOLUTE_PYTHON_EXECUTABLE"}`，把占位符换成目标机器上 Python 3 可执行文件的绝对路径。
