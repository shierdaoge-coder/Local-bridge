# local-bridge

local-bridge 在已绑定的 Codex 与 DeepSeek Harness（DSH）会话之间同步新增原文并传递消息。两类内容共用本地队列，分别领取和确认。共享服务监听 `127.0.0.1`，在绑定会话的自然轮次送出通知。接收端可以在服务设置页独立关闭原文接收。

需要 Node 24（含 `node:sqlite` 和 Zstandard 支持）。两端分别使用各自安装的宿主。启用 Python 自动原文同步时还需要 Python 3。

## 路径写法

以包含 `README.md` 和 `package.json` 的目录为项目根目录。本文和两端接入文档中的相对命令路径从这里解析。执行命令或保存 JSON 之前，把 `ABSOLUTE_...` 换成目标机器上的实际绝对路径，并保留引号。在 JSON 中，Windows 路径可以使用正斜线 `/`，也可以将每个反斜线写成 `\\`。

## 准备共享服务

在项目外的私密目录创建一个启动配置。以下是仅开启双向通讯的配置骨架。将每个 `native_session_id` 换成目标宿主中的实际会话 ID，并为 `adminToken` 和两个 `credential` 各自生成一个随机值。可多次运行 `node -e "console.log(require('node:crypto').randomBytes(32).toString('base64url'))"`。

```json
{
  "port": 4780,
  "adminToken": "REPLACE_WITH_ADMIN_TOKEN",
  "nativeAdapters": true,
  "historyBatchNotices": true,
  "endpoints": [
    {
      "id": "codex-local",
      "harness": "codex",
      "native_session_id": "CODEX_SESSION_UUID",
      "credential": "REPLACE_WITH_CODEX_ENDPOINT_CREDENTIAL",
      "capabilities": { "can_enqueue": true }
    },
    {
      "id": "dsh-local",
      "harness": "dsh",
      "native_session_id": "DSH_SESSION_ID",
      "credential": "REPLACE_WITH_DSH_ENDPOINT_CREDENTIAL",
      "capabilities": { "can_enqueue": true }
    }
  ],
  "grants": [
    {
      "id": "codex-to-dsh",
      "from": "codex-local",
      "to": "dsh-local",
      "content_types": ["message"],
      "allow_wake": false,
      "author_ref": "local-operator"
    },
    {
      "id": "dsh-to-codex",
      "from": "dsh-local",
      "to": "codex-local",
      "content_types": ["message"],
      "allow_wake": false,
      "author_ref": "local-operator"
    }
  ]
}
```

从项目根目录运行以下命令，将 `ABSOLUTE_PRIVATE_CONFIG_PATH` 换成刚保存的启动配置文件的绝对路径。第一条命令输出的 `directory` 是 DSH 自动启动设置中 `ABSOLUTE_PREPARED_SERVICE_DIRECTORY` 的实际值。服务控制命令默认选取最近准备的目录，也可以显式传入对应目录。

```text
node scripts/daily-use.mjs prepare "ABSOLUTE_PRIVATE_CONFIG_PATH"
node scripts/daily-use.mjs supervise
```

查看 `http://127.0.0.1:4780/` 的设置页。按 [Codex 接入](CODEX-SETUP.md) 和 [DSH 接入](DSH-SETUP.md) 将同一配置中的地址、会话 ID 和各端凭据写入对应宿主。服务控制命令还有 `status`、`pause`、`resume`、`snapshot` 和 `stop`。

## 选择原文同步来源

准备服务前，把需要同步的方向加入 `history_sync` 授权，并在配置中增加 `archives`、`bindings`、`subscriptions` 三个数组。以下示例从 Codex 同步到 DSH。`ABSOLUTE_CODEX_TRANSCRIPT_PATH` 与 Codex 绑定中的 `transcript_path` 指向同一文件。反向使用独立的档案和订阅，格式为 `dsh-zstd`，`path` 填 `ABSOLUTE_DSH_TRANSCRIPT_PATH`，即已选 DSH 会话的原生归档文件的绝对路径。使用 `from_now` 时，索引从绑定时的来源边界接收增量。

```json
{
  "archives": [
    { "id": "codex-archive", "owner": "codex-local", "generation": "v1" }
  ],
  "bindings": [
    {
      "archive_id": "codex-archive",
      "path": "ABSOLUTE_CODEX_TRANSCRIPT_PATH",
      "format": "codex-jsonl",
      "initial_mode": "from_now",
      "author_ref": "local-operator"
    }
  ],
  "subscriptions": [
    { "id": "codex-history-to-dsh", "archive_id": "codex-archive", "to": "dsh-local", "grant_id": "codex-to-dsh" }
  ]
}
```

## 构建与数据

两端插件在目标机器上构建，具体命令见各自的接入说明。运行 `npm run build:service` 可生成独立的共享服务代码包，启动参数见其 README。服务目录、宿主私密配置、会话原文和生成的安装包都按本机数据保管。公开分发使用源码。

[API 和会话工具](API.md) · [Codex 接入](CODEX-SETUP.md) · [DSH 接入](DSH-SETUP.md)
