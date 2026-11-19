# 从任一端启动共享服务

Codex 的 MCP 和 DSH 的心跳都能启动本地桥接服务。两个安装包各带一份运行入口，均使用 Node 24。启用自启动的宿主在本机服务拒绝连接时尝试拉起监督进程。双端共用同一个服务目录和账本。

按发布源码 README 的步骤准备服务。`ABSOLUTE_SHARED_SERVICE_DIRECTORY` 对应 `prepare` 返回的 `directory`。目录内保存私密 `config.json` 和它指定的 `data` 子目录。配置使用固定的 IPv4 回环端口并启用 `nativeAdapters`。宿主的 `bridge.json` 使用同一地址和本端已登记的会话绑定。

在每个启用自启动的宿主中，把 `service-start.json` 放在该端 `bridge.json` 旁，填入以下两个绝对路径。

```json
{
  "node_path": "ABSOLUTE_NODE_EXECUTABLE",
  "service_directory": "ABSOLUTE_SHARED_SERVICE_DIRECTORY"
}
```

已有安装的 `trial_directory` 设置继续使用原服务入口。使用包内共享服务入口时，在 `service-start.json` 填写 `service_directory`。`ABSOLUTE_NODE_EXECUTABLE` 可通过 `node -p "process.execPath"` 获取。共享目录保存私密配置、原文索引、游标、消息和停机状态。升级或更换插件包时沿用该目录。

可在任一安装包目录执行下面的命令。`stop` 把停机意图写入共享目录，重载宿主也继续保持停机。`resume` 再次允许已配置的宿主自动启动服务。

```powershell
node scripts/shared-service.mjs status 'ABSOLUTE_SHARED_SERVICE_DIRECTORY'
node scripts/shared-service.mjs stop 'ABSOLUTE_SHARED_SERVICE_DIRECTORY'
node scripts/shared-service.mjs resume 'ABSOLUTE_SHARED_SERVICE_DIRECTORY'
node scripts/shared-service.mjs supervise 'ABSOLUTE_SHARED_SERVICE_DIRECTORY'
```

## 配对两个会话

在共享服务的私密 `config.json` 里填写 `sessionPairs`。每条 `directions` 使用 `endpoints` 已配置的精确端点 ID。只声明实际授权的方向。需要同步原文的方向填写发端自己的 `archive_id`，并在 `archives` 与 `bindings` 登记该来源。纯通讯方向省略此项。

```json
{
  "sessionPairs": [
    {
      "id": "PAIR_ID",
      "author_ref": "OPERATOR_AUTHORIZATION_REFERENCE",
      "directions": [
        {"from": "ENDPOINT_A", "to": "ENDPOINT_B", "archive_id": "ARCHIVE_A"},
        {"from": "ENDPOINT_B", "to": "ENDPOINT_A"}
      ]
    }
  ]
}
```

使用 `node scripts/shared-service.mjs pairs 'ABSOLUTE_SHARED_SERVICE_DIRECTORY'` 预览方向、授权 ID 和订阅。服务启动时把配置应用到原账本。同宿主的两个会话使用该宿主的一份插件，在私密 `bridge.json` 分别绑定各自的原生会话 ID。

## 向未装插件的另一宿主投递

需要原生投递的跨宿主方向显式设 `native_delivery: true`。在同一份私密配置的 `nativeTargets` 下按接收端点 ID 登记独立运行时，填写本机可执行文件、独立宿主数据目录、工作目录和目标会话原档的绝对路径。目标档所在的会话 ID 需和 `endpoints` 一致。Codex App Server 和 DSH SDK 使用各自原生的恢复入口。服务器接收的是程序核对过的已读回执。

```json
{
  "nativeTargets": {
    "TARGET_ENDPOINT_ID": {
      "executable": "ABSOLUTE_NATIVE_EXECUTABLE",
      "home": "ABSOLUTE_STANDALONE_HARNESS_HOME",
      "cwd": "ABSOLUTE_TARGET_WORKSPACE",
      "archive": "ABSOLUTE_TARGET_SESSION_ARCHIVE",
      "provider": "NATIVE_PROVIDER_ID",
      "model": "NATIVE_MODEL_ID"
    }
  }
}
```

DSH 的 `provider` 和 `model` 为其 SDK profile 中已配置的值。Codex 可沿用独立运行时配置，填写时须吻合已存会话的提供商。此入口接管独立运行时里的会话，在线桌面会话另走宿主自己的授权入口。

```powershell
node scripts/relay-session.mjs 'ABSOLUTE_SHARED_SERVICE_DIRECTORY' TARGET_ENDPOINT_ID --list
node scripts/relay-session.mjs 'ABSOLUTE_SHARED_SERVICE_DIRECTORY' TARGET_ENDPOINT_ID EXACT_PACKET_ID --deliver
```

`--list` 只列待投递条目的 ID 和类型。`--deliver` 由操作者明确触发一次原生回合。程序分别核对入队、起轮和完成，收到正确回执后确认同一个包。回复确认提交失败时可复用已保存的原生证据。调用结果不明时停在待查状态，避免重复起轮。原生投递和固定回执按档案中的对应范围从反向原文同步里排除。

### 连接已加载的会话

操作者可把跨宿主方向的接收目标配置为 `archive` 和 `connection`。`archive` 是已绑定的原档，`connection.credential_file` 是操作者提供的本地授权文件，内容为一行令牌。连接仅接受明确端口的 `127.0.0.1`。Codex 填 `ws://127.0.0.1:PORT` 和持有该会话的同一 App Server 的能力令牌。DSH 填 `http://127.0.0.1:PORT` 和该网关提供的启动令牌。程序向 DSH 换取独立 API 操作员凭据，使用 `interactions=none`。

```json
{
  "nativeTargets": {
    "TARGET_ENDPOINT_ID": {
      "archive": "ABSOLUTE_TARGET_SESSION_ARCHIVE",
      "connection": {
        "url": "ws://127.0.0.1:PORT",
        "credential_file": "ABSOLUTE_OPERATOR_TOKEN_FILE"
      }
    }
  }
}
```

已加载且空闲的目标可接受投递。Codex 会话必须属于配置的 App Server。DSH 会话必须在所选网关中处于活动状态。连接现有宿主时沿用该会话的模型和权限。服务只有在宿主完成回合且匹配的输入与回执均已写入绑定原档后，才确认队列。Windows 本地桌面的现有会话需要具备可授权的同宿主入口。

### 自动投递与回发

在私密 `config.json` 顶层设 `"nativeRelay": true`。用 `shared-service.mjs supervise` 启动，或者从带有 `service_directory` 设置的宿主自动拉起。后台会检查已声明 `native_delivery` 的跨宿主方向。目标需要独立运行时或已授权的宿主连接。`idle_wake` 的内容可在目标会话可用时自动投递，每次处理一个包。`next_turn` 的内容留在队列，可由已有宿主入口或显式的 `--deliver` 处理。接收原文开关和暂停状态即时生效，重启后继续沿用。已连接的桥接客户端保有自己的队列连接。

服务恢复后先补交已保存的原生回执。提交前的临时故障会退避重试。提交结果不明时，该目标等待核对，其他目标照常处理。队列 ID 与状态写入私密目录的 `native-relay.jsonl`，可配合 `native-receipts` 查看提交状态和已读证据。

未安装插件的会话可通过脚本发回业务消息。用 `shared-service.mjs pairs` 查方向的授权 ID，把接收端点、授权 ID、稳定消息 ID 和 `payload` 写成一份 JSON，通过 stdin 交给 `--publish`。脚本只使用同一个已配对的独立源端点。重试时复用同一消息 ID 和正文。原文仍由已登记的档案自动索引。

```powershell
Get-Content -Raw 'ABSOLUTE_MESSAGE_JSON' | node scripts/relay-session.mjs 'ABSOLUTE_SHARED_SERVICE_DIRECTORY' SOURCE_ENDPOINT_ID --publish
```

```json
{
  "id": "STABLE_MESSAGE_ID",
  "to": "TARGET_ENDPOINT_ID",
  "grant_id": "PAIR_DIRECTION_GRANT_ID",
  "payload": {
    "kind": "event",
    "topic": "example.update",
    "correlation_id": "STABLE_MESSAGE_ID",
    "text": "MESSAGE_TEXT",
    "source_refs": []
  }
}
```
