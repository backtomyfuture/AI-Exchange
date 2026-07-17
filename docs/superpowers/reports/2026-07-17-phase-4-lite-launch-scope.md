# Phase 4-Lite 精简上线范围

日期：2026-07-17

## 决策

本项目按全新系统部署，不迁移或兼容测试阶段的历史数据。当前唯一上线形态是单
Exchange 账户、单飞书账户、单应用进程：Exchange Webhook 先提交 Durable Inbox，
同一进程内的一个 Worker 再调用现有 LangGraph、ContentStore、Exchange 客户端和飞书
审批链路。

`DURABLE_INBOX_ENABLED=true` 是唯一处理开关。只有在数据库 bootstrap、greenfield
initialize 和真实凭据配置完成后才可打开。`INGESTION_SHADOW_ENABLED=false` 与
`SYNC_RECONCILIATION_ENABLED=false` 是该形态的固定约束。

## 启动与关闭契约

应用启动顺序为：数据库边界门禁、checkpoint maintenance fence、现有 AppContext 与
Graph、保持 intake 关闭的飞书初始化/WebSocket、Durable Inbox runtime，最后才开放飞书
intake。只有全部成功后才发布 readiness。关闭时先撤销 intake，再停止 runtime/Worker，
排空飞书动作，关闭 AppContext，最后释放 checkpoint fence；任一无法证明完成的阶段均
失败关闭，并保留未能安全释放的所有权或 fence 供进程级 fail-stop 处理。

飞书线程存活不等于回调通道可用：启动阶段必须在固定时限内确认 SDK 底层 WebSocket
已经真实连接，之后才启动 Worker 并开放 intake。运行期短暂重连允许在 30 秒宽限期内
恢复；持续断连或数据库 runtime heartbeat 丢失会先关闭 intake，再让进程硬退出，由
容器监督器重启。Worker claim/process 控制面或过期 lease recovery 任务一旦异常退出，
processing readiness 会立即失败，并由 runtime heartbeat 在有界时间内执行同样的
fail-stop。readiness 在断连或处理面丢失期间立即失败。

## 全新部署门禁

正式 Compose 仅面向全新专用 PostgreSQL cluster/volume。先运行显式
`database-provision` one-shot 创建四个互相隔离的受限角色并收紧跨库、schema、默认权限
与 large-object 边界，再运行 schema bootstrap 和 greenfield initialize。provisioner 会
拒绝含业务对象的数据库，不能用于共享 cluster 或历史库改造。ContentStore 与 Qdrant
都使用全新的独立 volume；ContentStore 使用新的 32-byte key，不迁移测试数据。

生产 `INGESTION_INSTANCE_ID` 固定为 `ai-exchange-web`，不得改名或 `scale`。数据库会话
唯一性负责拒绝第二个同名进程；不同 identity 则在配置门禁阶段直接失败，而不是扩展成
多实例模式。

Exchange TLS 必须保持验证开启。若服务通过私网 IP 访问而证书只覆盖域名，部署必须使用
可被证书覆盖的主机别名、精确 IP 映射和只读 PEM 信任文件；仓库提供可选 Compose TLS
覆盖层，不接受以关闭证书验证作为上线办法。

应用镜像本身只提供 HTTP；正式 `EXTERNAL_URL`、证书终止和到
`/webhooks/exchange` 的 HTTPS 路由由现有外部反向代理/入口提供，本阶段不再内置第二套
代理。没有真实可达的 HTTPS 地址时只能做隔离部署冒烟，不能宣称外部回调已经上线。
在 Exchange 扩展中启用订阅前，必须先以 `is_active=false` 保存订阅，并用同一份至少
16 字节高熵 secret 完成签名 TestEvent 冒烟。验收必须检查扩展返回的目标 status 为
200，且 AI 响应为 `{"status":"ok","test":true}`，不能只看管理 API 的外层 200；通过后
才启用订阅。AI policy manifest 固定真实 Inbox opaque ID，扩展订阅固定单账户与
`NewMailEvent`。扩展配置变更留到本项目验证完成之后执行。

初始化 manifest 绑定已提交且干净的 Git tree、固定单账户拓扑和操作员 release 记录。
该记录用于可追溯初始化，不是签名 CI provenance，也不声称运行时能自行证明容器镜像；
精简上线的实际门禁仍是从当前 HEAD 显式构建镜像、运行测试、记录镜像摘要并完成真实
Compose 冒烟。镜像身份的运行时强绑定属于已取消的完整 cutover/Phase 5–6 范围。

## 已知的保守恢复语义

当前适配器把进入 Exchange 详情读取、模型、飞书或 Exchange 写操作等外部边界后出现的
未知结果统一送入人工复核，不自动重试。这样可能把一次普通的瞬时读取失败也升级为人工
处理，但可避免在无法证明外部效果是否发生时重复发卡或发信。单用户上线阶段接受这一
保守取舍；只有操作员确认没有重复副作用后，才可重新排队。细分只读与写入效果的恢复
策略属于后续优化，不在本次 Phase 4-Lite 中扩展。

## 明确不包含

- 历史数据迁移、旧 schema/旧卡/旧运行时兼容；
- Shadow、backfill、cutover、legacy generation 切换；
- 多账户、多飞书账户、多实例和滚动发布；
- Notification/Mailbox/Projection Outbox，以及 Graph/模型/ContentStore 重写；
- Phase 5、Phase 6；
- 主动轮询。当前 Exchange 扩展尚未提供已验证的游标式 Sync v2 契约，因此 Webhook 是
  唯一收件主通道。未来若需要补漏，应新增独立、幂等的低频 cursor reconciliation，
  不能直接复用无高水位证明的“每 5 分钟列邮件”任务。

Exchange 扩展当前已经持久化 Webhook delivery 并交给 ARQ 有界重试，但“delivery 已写入、
Redis 入队失败”后没有自动扫描残留 pending 记录；该服务端仓库本阶段保持只读。这是
当前主通道的已知残余风险，待本项目上线后在服务端补 pending scanner/重投门禁，而不是
在本项目中用无游标轮询掩盖。

本记录是对原六阶段计划的有意精简，不表示原计划中的完整
`production_ready`/cutover 证据链已经实现。
