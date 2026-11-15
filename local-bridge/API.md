# local-bridge API

只监听127.0.0.1；Host和Origin限定本服务的环回地址，JSON请求上限16 KiB。接口返回 `{ok:true,result}` 或有错误码的失败结果。

## 身份

管理员接口使用 `Authorization: Bearer <本地管理凭据>`。这是服务管理员权限，不向模型工具暴露。

端点先用永久端点凭据调用 `POST /api/connect`，正文为 `{endpoint_id}`。响应返回临时连接令牌和递增epoch。随后会话接口使用：

```text
Authorization: Bearer <连接令牌>
X-Bridge-Endpoint: <已绑定地址>
X-Bridge-Epoch: <当前连接代次>
```

旧连接、过期租约和伪造端点身份被拒绝。服务重启不自动恢复旧活跃连接。

## 接口

| 方法与路径 | 身份 | 用途 |
| --- | --- | --- |
| GET /api/health | 无 | 服务标识，不启动模型 |
| GET /api/endpoints | 管理员 | 查看端点、能力和设置，不返回凭据 |
| POST /api/endpoints | 管理员 | 绑定具体harness／原生会话，首次返回端点凭据 |
| PATCH /api/endpoints/:id/settings | 管理员 | 分别保存同步、通讯策略及暂停状态 |
| PATCH /api/endpoints/:id/capabilities | 管理员 | 验收后登记原生入队、唤醒及确认能力 |
| POST /api/grants | 管理员 | 登记授权出处、方向、类别、分工和唤醒范围 |
| POST /api/grants/:id/revoke | 管理员 | 撤销该方向授权 |
| POST /api/archives | 管理员 | 登记源端所有权和档案代次 |
| POST /api/sources | 管理员 | 给已登记档案绑定原文路径、格式和解码预算 |
| GET /api/sources/:id | 管理员 | 查询来源检查点、状态及错误 |
| POST /api/history-subscriptions | 管理员 | 登记来源→接收端的已有授权并回填已索引原文 |
| POST /api/sources/:id/scan | 管理员 | 显式扫描有限页，原子提交索引／扇出／检查点 |
| POST /api/connect | 端点永久凭据 | 建立当前连接 |
| GET /api/session | 当前连接 | 查询当前绑定的原生会话和设置，不返回凭据 |
| POST /api/heartbeat | 当前连接 | 延续租约 |
| POST /api/publish | 当前连接 | 两类内容共用的持久入队入口 |
| POST /api/receive | 当前连接 | 按预算领取，可指定通知中的ids |
| POST /api/ack | 当前连接 | 按实际返回的ID和领取令牌逐条确认 |
| GET /api/messages/:id | 发送／接收端当前连接 | 查询传输、处理和宿主尝试，不启动模型 |
| GET /api/cursor?archive_id=...&generation=... | 接收端当前连接 | 查询连续已读的可见原文序号 |
| POST /api/codex/host-event | 端点永久凭据 | 由原生hook提交会话事件，必要时领取一次性工具票据 |
| POST /api/codex/tool-call | 一次性票据 | 执行该原生调用绑定的工具与参数 |
| POST /api/codex/heartbeat | 独立liveness能力 | 延续已有连接，不创建会话、签发票据或改变忙闲状态 |
| POST /api/dsh/pulse | DSH端点永久凭据 | 原生插件心跳、当前pre-step槽及单次宿主命令领取 |
| POST /api/dsh/tool-call | DSH端点永久凭据 | 绑定实际exec会话的6工具调用，与pulse共用连接 |
| POST /api/dsh/host-result | DSH端点永久凭据 | 提交本机宿主命令结果，不等于模型已读 |

通用信封字段：`protocol=local-bridge.session-transport/0.1`、稳定`id`、`to`、`content_type`、`grant_id`、`payload`，可选`expires_at`。服务从连接填写来源和提交时间，拒绝正文自报的`from`、`created_at`。

`message`正文含业务kind、topic、correlation_id、text和source_refs。请求声明work_domain及与管理员授权匹配的authority_ref；result/cancel保留原请求关联与分工，回信仍须有相应方向的授权。标签只能帮助机械检查，接收方仍要核对任务内容，不能通过把工程标成text绕过分工。

`history_sync`正文包含archive_id、generation、连续可见序号范围及records。单包最多2 KiB，接收批次最多4包／8 KiB。长记录的fragment给出index、count与全文校验值；缺片、校验不符或未确认任意一片都不能越过该记录推进历史游标。

`receive`可传 `{ids:[通知中的消息ID],limit:4,budgetBytes:8192}`。明确ID领取允许及时通讯与先前积压的历史共同进入有限批次，保留原队列序号。历史连续性另行计算。

`ack`正文为数组：`[{id,claim_token,outcome?}]`。只有真实领取得到的当前令牌可确认；失败不连带跳过其他项。省略outcome只确认读入，`accepted`和`completed`分别表示接下与完成；不能把宿主排队响应当作任何一种业务结果。

### 按窗口批量领取原文

用 `bridge_receive({archive_id, budgetBytes?})` 领取一个已授权来源在本次快照中可取的未读原文。HTTP 客户端将同一对象发给 `POST /api/receive`。默认返回预算为 16 KiB，最高 64 KiB，按紧凑结果的 UTF-8 字节数计算。

`runs` 按原序号排列。相邻的同角色消息共用一个 `role`，`messages` 中每条为 `[序号, 正文]`。长消息的条目还会带第三项 `[片号, 总片数]`，片号从 1 开始。预算用完时在完整传输包边界停下。`remaining_packets` 是快照中尚未领取的排队包数，`indexed_through` 是已提交的索引末尾，`source_status` 表示上一次索引状态。若更早的包正被领取，结果可给出 `blocked_until`。

完整读入后用 `bridge_ack({receipt})` 确认，HTTP 客户端将同一对象发给 `POST /api/ack`。确认号绑定实际领取的接收连接和全部包，一次事务内确认。游标只跨过连续读齐的记录。新增内容、缺片和失效的领取保留后续处理机会。

原文批次回执默认从领取起有效 3 分钟，仍须由同一接收连接确认。服务配置中的 `historyClaimMs` 以毫秒为单位，范围为 100 到 600000。重连后可从未确认的位置重新领取。

在私密服务配置设 `"historyBatchNotices": true` 后，原文通知提供来源和未读包数，混合通知中的通讯事件继续用消息 ID 领取。

### Python 自动同步

Codex、DSH 各带一份 `src/history-sync.py`，只用 Python 3 标准库。在想启用的一端把 `AUTO_SYNC_HISTORY = False` 改成 `True`。Windows 默认用 `py -3`，其他系统用 `python3`。也可在该端私密 `bridge.json` 所在目录创建 `history-sync.json`，填写 `{"python_path": "ABSOLUTE_PYTHON_EXECUTABLE"}`，将占位符替换为 Python 3 解释器的绝对路径。脚本的 `--probe` 只检查解释器和开关。

开启后，绑定会话在用户开始自然对话时，如果允许原文接收且有积压，宿主插入一次同步提示。智能体调用 `bridge_sync_history({archive_id})` 领取。完整读入后，再调用 `bridge_sync_history({batch_id})` 确认。正文仍按现有批量格式返回，原领取凭据由宿主保管。解释器不可用时可继续使用 `bridge_receive` 和 `bridge_ack`。

脚本先核对会话再领取。领取超时用同一请求号重试，确认超时重试原批，然后用游标工具核对批次是否已提交。连续游标可因缺片停在更早的位置。每个工具最多尝试三次，每次脚本调用最多等待 16 秒。当前批过期且确认结果不可核实时，下一次仍从未读内容接续。

## 宿主投递

适配器的`offer`使用统一批次接口，包含稳定batch_id、接收地址、消息ids、内容类型、wake及机器来源标记。成功只表示宿主接下排队；超时／异常转为uncertain并暂停盲重试。

接收插件按通知提供的 `ids` 或 `archive_id` 领取，再据实际读取确认。共享服务只在绑定会话的自然轮次投递。同一方向的记录和通讯共用队列与投递机制。

## 来源扫描与恢复

登记来源正文为`{archive_id,path,format,max_decoded_bytes?}`，其中path必须为管理员提供的绝对路径，format为`codex-jsonl`或`dsh-zstd`。默认每页解码预算64 MiB，最大512 MiB。订阅正文为`{id,archive_id,to,grant_id}`，要求已有方向正确的history_sync授权。

扫描正文为 `{maxPages?:1..1000}`，默认 1 页。响应含本次 records、queued 消息 ID、has_more、incomplete_tail 和 checkpoint。共享服务在启动配置中登记来源，按页扫描，并通过文件监听和定时检查补扫。

checkpoint是已经索引的物理来源位置，不表示模型已经读过；模型已读位置仍只由实际领取／确认推进。索引、所有片段入队、接收端来源去重标记与检查点共同提交。事务失败不发活动通知；提交成功后每个目标只通知一次。被撤销的订阅记录错误，不妨碍其他有效方向。

来源前缀改变会返回`SOURCE_REPLACED`并标记`needs_reindex`，旧检查点与待处理队列保留。当前没有原地重置接口；须后续实现明确的代次迁移流程，不能手工猜改检查点。

## 会话工具与MCP

`session-tools.mjs` 暴露会话工具与执行器。DSH 插件按真实执行上下文查询预登记端点。自动同步工具由两端宿主本地执行。

| 工具 | 输入要点 | 作用 |
| --- | --- | --- |
| bridge_context | 空对象 | 查看当前端点及设置 |
| bridge_receive | ids?、limit?、budgetBytes? | 最多领取4条／8 KiB |
| bridge_ack | acknowledgements数组 | 逐条确认实际读入和处理结果 |
| bridge_status | id | 查询传输／宿主尝试／业务证据 |
| bridge_publish | id、to、grant_id、payload、expires_at? | 仅发布message；不可自报from |
| bridge_history_cursor | archive_id、generation | 查询当前接收端连续已读位置 |
| bridge_sync_history | archive_id 或 batch_id | 分两次调用领取原文和确认批次 |

`mcp.mjs`提供`McpToolSession`和`serveMcpStdio`；固定协议版本2025-11-25，先initialize，再notifications/initialized，再tools/list或tools/call。每行一条UTF-8 JSON-RPC，单输入帧默认16 KiB。协议错误返回JSON-RPC error；业务或调用身份错误返回工具结果isError=true。工具调用通知（无id）不会执行操作。

可信宿主上下文由外部回调给出。`_meta`、工具参数、历史正文及 MCP 启动环境均不能证明调用方身份。无可信上下文时可以发现工具，执行会被拒绝。

## Codex hook／MCP适配

`/api/health`增加features：codex_hooks、provided_endpoint_credentials、history_indexing。配置工具在写入前检查前两项；管理员登记端点可提供32—256字符的base64url形式credential，供操作者先持久保存恢复候选，再提交登记。模型工具没有此接口权限。

原生hook读取stdin事件，并按私密配置核对session_id、cwd、transcript_path；携带agent_id的子代理事件拒绝作为父会话调用。PreToolUse凭端点秘密向host-event提交`{endpoint_id,event}`；网关核对登记身份、工具名、调用ID及参数后返回30秒的一次性票据。hook用updatedInput注入`_bridge_ticket`字段，MCP代理剥离该字段，再以Bearer票据请求tool-call。

票据限定同一端点、连接代次、工具及参数；同一原生调用ID不能跨连接重用。MCP工具不接受模型自报的session_id作为身份。tool-call响应的传输封装包含value与独立liveness能力，MCP代理只向模型返回value；liveness不能用于调用工具、签发票据或改变忙闲状态。

MCP进程每10秒维持已建立的连接，结束时停止定时器；SessionEnd或Interrupt失效连接及未消费票据。每次原生hook维持同一连接，避免每个工具重新connect而使领取令牌失效。领取后应先及时ack实际读入，再开展长时间任务推理。

UserPromptSubmit打开一次自然轮次通知槽，由共同DeliveryCoordinator投递history_sync/message的有限批次。成功只表示通知已交给hook输出路径，不代表模型已读；领取与ack仍独立。Codex CLI queue仅在操作者开启后使用精确UUID调用，返回0或出现恢复警告仍保留uncertain，等待目标确认；未核实前不重复唤醒。

## DSH 宿主通道

health.features.dsh_host表示服务含DSH网关，不证明DSH插件已装入真实profile。插件从私密文件读取操作者预登记的endpoint/credential；原生工具使用exec.agent.session.id选择绑定，HTTP正文由宿主构造，不从模型参数读取身份。pulse、tool-call、host-result都核对endpoint/native_session_id/host_instance，统一使用一条连接，避免心跳轮换epoch导致claim失效。同一端点不接受另一个仍在线的host实例。

pulse正文为`{endpoint_id,native_session_id,host_instance,state,natural_turn?}`，state为idle/busy/offline；natural_turn仅允许busy的原生pre-step。普通心跳不会打开自然槽。返回command可能含command_id、endpoint、batch_id、ids、content_types、wake和expires_at；命令只领取一次，迟到／不明不盲重发。expires_at是**最迟开始原生交接的时间**，不是成功证据的提交期限；已在期限前开始的交接可在coordinator总超时前提交真实native_ref。

原生插件先核对命令目标、开始期限与当前状态。非wake通知只追加到官方agent/pre-step的decision.messages；没有当前槽则拒绝，不留到inbox迫使多一次模型step。主动wake才调用DshDeliveryAdapter的followup；它先用官方createUserMessage和adoptSessionEvent验证role/id/source，并检查实际session log证据。pre-step槽交接结果用dsh-pre-step:message-id标记，只证明已交给当前槽；框架后续落盘和实际领取/ack另行确认。

host-result提交`{endpoint_id,native_session_id,host_instance,command_id,accepted?,native_ref?,error?}`；只能结算同一连接已领取的当前命令。缺少明确结果、服务重启或超时仍记uncertain。卸载等待本插件正在进行的请求结束后发送offline，注销工具、监听器和定时器。

宿主配置步骤见 [DSH-SETUP.md](DSH-SETUP.md)。
