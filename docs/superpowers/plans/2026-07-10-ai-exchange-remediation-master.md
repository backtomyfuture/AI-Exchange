# AI-Exchange Full Remediation Master Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 AI-Exchange 从依赖进程内队列和大型 LangGraph checkpoint 的现状，渐进改造成 PostgreSQL 事实源、Webhook + Sync 补偿、审批与发送可审计且不会自动重复发送的生产级邮件助手。

**Architecture:** 实施分成六个可独立验收的子计划，严格按依赖顺序执行。阶段 1 先止血并建立 Alembic 与最小 ContentStore；随后分别交付持久化收件、审批发送、Graph/模型/Qdrant、安全治理，最后完成可观测性、CI、历史清理和切换收缩。

**Tech Stack:** Python 3.12、FastAPI、Pydantic 2、psycopg 3、Alembic、PostgreSQL 15、LangGraph 1.x、Qdrant、httpx、lark-oapi、Prometheus、structlog、pytest、uv、Docker Compose。

## Global Constraints

- 本计划只修改 `/Users/jarod/Documents/AI-Exchange`；`/Users/jarod/Documents/exchange-feishu-extension` 在全部当前项目验收前保持只读。
- PostgreSQL 是唯一业务事实来源；LangGraph checkpoint、Qdrant 和飞书卡片均为可恢复执行或投影视图。
- 收件采用已批准的 Webhook 实时触发 + 每 5 分钟 `/emails/sync` 增量补偿。
- reply/forward 在超时、断连、发送开始后进程崩溃或可能发生在远端执行后的 5xx 时进入 `send_unknown`，禁止自动重试。
- `send_unknown` 重新发送必须有新的人工授权、新的 `send_intents` 和新的 Send Outbox；旧尝试保持不可变。
- 普通 `/send` 的 HTTP 200 仅表示 `accepted`，没有状态证据时不得标记为 `sent`。
- Graph State 禁止保存正文、HTML、Base64、图片字节、完整附件、完整模型请求或完整飞书卡片。
- 应用业务表由 Alembic 管理；LangGraph checkpoint 迁移由独立 autocommit 引导命令管理。
- 生产环境强制 Exchange TLS 校验，禁止自动降级为 `verify=False`。
- 所有外部副作用必须通过 Outbox；模型永远不能直接发送邮件、访问任意文件或访问内部网络。
- 每个 Inbox 与 Notification/Mailbox/Send/Projection Outbox 均固化 `generation` 和 `fencing_token`，claim/complete 都验证；账户开关不授予执行权。
- 六个按账户期望开关为 `DURABLE_INBOX_ENABLED`、`SYNC_RECONCILIATION_ENABLED`、`NEW_APPROVAL_FLOW_ENABLED`、`SEND_OUTBOX_ENABLED`、`LIGHTWEIGHT_GRAPH_STATE_ENABLED`、`QDRANT_OUTBOX_ENABLED`。
- 每个任务先写失败测试，再写最小实现，再运行定向测试和阶段回归，最后提交。
- 每次暂存前运行 `git status --short` 和 `git diff --check`，只暂存任务列出的路径；不得覆盖、reset 或 checkout 用户已有修改。
- 当前工作区已有 49 个预存未提交文件。执行前必须先完成阶段 1 的“预存修改归档”任务，不得把它们混入后续功能提交。
- 全局覆盖率最终不少于 80%，可靠性关键模块不少于 90%；覆盖率采用只升不降 ratchet。

---

## Plan Suite

| 顺序 | 子计划 | 可独立交付结果 |
|---|---|---|
| 1 | `2026-07-10-ai-exchange-phase-1-p0-foundation.md` | P0 数据丢失、近 OOM、百万 Token、迁移、安全暴露得到止血 |
| 2 | `2026-07-10-ai-exchange-phase-2-durable-ingestion.md` | Webhook 202 持久化、5 分钟 Sync、固定 Worker、代次所有权可恢复 |
| 3 | `2026-07-10-ai-exchange-phase-3-approval-send.md` | 不可变审批快照、动作 CAS、Outbox、`send_unknown` 人工恢复 |
| 4 | `2026-07-10-ai-exchange-phase-4-graph-model-projection.md` | ContentStore 完整生命周期、轻量 Graph、T1→T2→T3、统一模型网关、Qdrant 可重建投影 |
| 5 | `2026-07-10-ai-exchange-phase-5-security-governance.md` | 飞书 RBAC、安全预览、HTML/PDF/附件、模型数据治理、容器纵深防御 |
| 6 | `2026-07-10-ai-exchange-phase-6-operations-cutover.md` | 真实指标、告警、CI/构建、保留清理、影子切换、旧路径收缩和最终验收 |

依赖关系：

```mermaid
flowchart LR
    P1["Phase 1 P0 Foundation"] --> P2["Phase 2 Durable Ingestion"]
    P2 --> P3["Phase 3 Approval and Send"]
    P3 --> P4["Phase 4 Graph and Model"]
    P4 --> P5["Phase 5 Security Governance"]
    P5 --> P6["Phase 6 Operations and Cutover"]
```

## Locked File Structure

新增文件按业务边界组织，现有大文件只保留兼容适配器：

```text
alembic/
  env.py                         # 仅应用业务表迁移环境
  versions/                     # 按阶段追加迁移
src/db/
  bootstrap.py                  # Alembic + LangGraph autocommit 引导命令
  schema.py                     # 启动时只读版本检查
  unit_of_work.py               # PostgreSQL 事务边界
src/storage/
  content_store.py              # ContentStore Protocol 与不可变 DTO
  encrypted_files.py            # AES-GCM 文件实现
  backend.py                    # 多后端字节存储 Port
  repository.py                 # 账户域内去重、引用与保全元数据
  migration.py                  # 历史内容幂等迁移
  rotation.py                   # 密钥版本轮换
  gc.py                         # 引用/保全感知的安全 GC
src/domain/
  errors.py                     # 统一错误类别
  email_state.py                # 状态和允许转换
src/ingestion/
  models.py                     # InboxEvent、SyncCursor、ChangeType
  repository.py                 # Inbox/Cursor/Email 聚合持久化
  worker.py                     # 固定 Worker 与租约
  sync.py                       # 5 分钟增量协调器
  ownership.py                  # current_ingress/draining/retired + fencing
src/approval/
  models.py                     # DraftVersion、ApprovalAction、SendIntent
  repository.py                 # 审批 CAS 与不可变快照
  service.py                    # 飞书动作业务入口
src/outbox/
  repository.py                 # 通用 Outbox 领取接口
  notification.py               # 飞书 at-least-once 投递
  mailbox.py                    # mark_read 等幂等邮箱动作
  send.py                       # 发送专用 write-ahead 与 send_unknown
src/llm/
  budget.py                     # Token/字节硬闸门
  gateway.py                    # 单层重试、限流、断路器与 Schema
  policy.py                     # 账户级供应商与数据策略
src/security/
  auth.py                       # 内部接口与飞书身份授权
  service_auth.py               # Webhook/Metrics/管理接口认证
  preview.py                    # SSO/一次性 Token 会话
  html.py                       # HTML 白名单与 CSP
  pdf.py                        # WeasyPrint 拒绝外部资源
  attachments.py               # 魔数、大小、像素与压缩检查
  redaction.py                  # 日志字段脱敏
src/maintenance/
  checkpoint_cleanup.py         # dry-run/备份后 checkpoint 清理
  retention.py                  # 内容、Outbox、Qdrant、日志保留
  cutover.py                    # 高水位对账与 generation 切换
src/runtime/
  resources.py                  # 客户端/Worker/连接池生命周期
  blocking.py                   # 有界线程/进程执行器
src/operations/
  alerts.py                     # 状态化告警规则
  runbooks.py                   # 告警到 Runbook 映射
src/projections/
  qdrant.py                     # 终态邮件投影 DTO 与幂等 Upsert/Delete
```

## Cross-Plan Interfaces

后续计划必须复用以下名称，不得自行改名：

```python
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Mapping, Protocol, Sequence


class ErrorKind(StrEnum):
    VALIDATION = "validation_error"
    AUTHENTICATION = "authentication_error"
    RATE_LIMITED = "rate_limited"
    TRANSIENT_DEPENDENCY = "transient_dependency_error"
    PERMANENT_DEPENDENCY = "permanent_dependency_error"
    POLICY_REJECTED = "policy_rejected"
    SEND_UNKNOWN = "send_unknown"
    INTERNAL_INVARIANT = "internal_invariant_error"


@dataclass(frozen=True)
class ContentRef:
    account_id: int
    object_id: str
    key_version: str
    sha256: str


class ContentStore(Protocol):
    async def put_email(self, account_id: int, email_id: str, email: Mapping[str, Any]) -> ContentRef:
        raise NotImplementedError

    async def load_email(self, ref: ContentRef, *, include_attachments: bool = False) -> dict[str, Any]:
        raise NotImplementedError

    async def delete(self, ref: ContentRef) -> None:
        raise NotImplementedError


class PipelineGenerationState(StrEnum):
    CURRENT_INGRESS = "current_ingress"
    QUIESCING = "quiescing"
    DRAINING = "draining"
    RETIRED = "retired"


@dataclass(frozen=True)
class ExchangeResult:
    outcome: str
    status_code: int | None
    remote_operation_id: str | None
    response_fingerprint: str | None


@dataclass(frozen=True)
class SendAttempt:
    outbox_id: str
    attempt_id: str
    request_started_at: datetime
    send_intent_id: str
```

## Stage Gates

每个子计划结束必须满足：

1. 该计划所有定向测试通过；
2. `.venv/bin/python -m pytest -q` 通过；
3. `.venv/bin/ruff check src/ tests/` 通过；
4. `git diff --check` 通过；
5. 数据库计划同时验证空数据库和现有结构升级；
6. 产生外部副作用的计划完成故障注入与重复事件测试；
7. 更新对应运行文档与 `.env.example`；
8. 由独立审查代理先做规格一致性审查，再做代码质量审查。

## Spec Coverage Audit

| Design section | Implementing tasks | Evidence at completion |
|---|---|---|
| §3 approved decisions | Master constraints; P2 T9-T10; P3 T6-T8 | Hybrid intake, accepted/send-unknown semantics and PostgreSQL source-of-truth tests |
| §4 request/sync/Worker architecture | P1 T4-T5; P2 T1-T10 | 202 durability, 5-minute reconciliation, fixed Workers, leases and cold-start tests |
| §5 persistence/state/outboxes | P2 T1-T5; P3 T1-T9; P4 T8 | Real PostgreSQL constraints, state/CAS races, immutable snapshots and four fenced Outboxes |
| §6 ContentStore/Graph/checkpoints | P1 T6-T7/T10; P4 T1/T7/T9; P5 T11; P6 T4/T9 | Restart, tenant dedupe, key rotation, holds/GC, state size and guarded cleanup tests |
| §7 T1→T2→T3/draft/review | P4 T1-T6 | Routing order, self-exclusion, immutable rewrite and single-interrupt tests |
| §8 errors/model gateway/data policy | P1 T2/T5/T8; P4 T2; P5 T8 | Typed errors, one retry layer, budget/schema/manual review and account policy tests |
| §9 full security | P1 T9; P5 T1-T12 | HMAC, service auth, Lark RBAC, preview, HTML/PDF/attachment, TLS/log/container and retention gates |
| §10 observability/operations | P6 T1-T4 | Runtime/DB truth metrics, low cardinality, all default alerts and linked Runbooks |
| §11 test/CI quality | Every task TDD; P5 T12; P6 T5-T6/T10 | Unit/concurrency/PostgreSQL/contract/fault/security/performance gates and coverage ratchet |
| §12 staged migration/history | P1 T1/T3; P2-P5 phase gates; P6 T8-T10 | Expand-migrate-switch-contract, backup/dry-run, compatibility views and final report |
| §13 flags/cutover/rollback | P2 T3/T10; P3 T4-T9; P4 T8; P6 T8 | Six per-account flags, ownership/fencing, quiesce/drain/switch/retire and rollback drain tests |
| §14 criteria 1-21 | P6 T5/T10 | Machine-checkable criterion-to-test/artifact manifest plus soak evidence |
| §15 Exchange server boundary | Master constraint; P6 T10 | Read-only verification and separate follow-up boundary report |
| §16 exclusions | Master constraints | No IMAP/EWS replacement, autonomous sending, distributed platform or server mutation added |
| §17 documentation | P5 T12; P6 T3/T7/T10 | Threat/data-flow, architecture, operations, OpenAPI, Runbooks and acceptance reports |

Self-review result: the earlier ContentStore lifecycle, all-Outbox fencing, reject/edit approval branching, guarded checkpoint cleanup, service-interface security, retention engine and six feature-flag gaps are now assigned to explicit tasks. No design requirement remains without an owner; Phase 6 T10 fails closed if any of the 21 final criteria lacks evidence.

## Final Acceptance

Phase 6 结束时逐项核对设计规格第 14 节的 21 条验收标准。任何一条没有自动化证据、运行日志或明确人工验收记录，都不能把项目标记为完成，也不能开始修改 Exchange 服务端项目。
