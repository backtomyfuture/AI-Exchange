# AI-Exchange Phase 6 Operations, Cutover, and Final Acceptance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 交付无异步阻塞、指标真实、告警可恢复、构建可复现、数据有保留期限且能够按账户安全切换和收缩旧路径的最终生产版本。

**Architecture:** 运行指标从真实数据库和执行点产生，告警通过持久化 Notification Outbox 投递并链接 Runbook。保留、备份、切换和历史清理都采用 dry-run、批量上限、代次 fencing 和高水位对账；CI 复现同一套迁移、故障注入、安全、性能与镜像门禁。当前轮只准备并验证旧路径收缩能力；真实删除必须等 extension v2、Phase-3 production activation 和完整观察窗之后。

**Tech Stack:** Python 3.12、asyncio、FastAPI、psycopg 3、Prometheus、OpenTelemetry、PostgreSQL 15、Qdrant、pytest、Ruff、Pyright、uv、GitHub Actions、Docker BuildKit、CycloneDX、Trivy、Gitleaks。

## Global Constraints

- `/Users/jarod/Documents/exchange-feishu-extension` 继续保持只读；服务端幂等键、发送状态查询和更强 Webhook 签名不在本计划实现。
- `pipeline_ownership` 是执行所有权事实；布尔功能开关不能越过 generation、fencing token 或逐邮件粘性归属。
- 回滚创建新的 `current_ingress` generation；不得把已返回 202 的 Inbox 退回不可见旧入口，也不得自动重放 `send_unknown`。
- 运行调度不得使用未追踪 `asyncio.create_task()`；每个后台任务有 owner、关闭信号、等待与超时策略。
- Metrics 标签仅允许低基数的账户类别、阶段、结果和错误类别；邮件、请求、Inbox、Outbox、Workflow ID 只进入脱敏日志/Trace/审计。
- 所有清理先 dry-run，再验证加密备份/快照，最后以有界批次执行；`waiting_approval`、`accepted`、`send_unknown` 和审计保全对象默认不可删。
- 每次 CI 都验证空数据库、现有结构快照升级和迁移重复运行；禁止永久 skip、空断言或全 Mock 集成测试充当发布证据。
- 全局覆盖率采用只升不降 ratchet，最终至少 80%；可靠性关键模块至少 90%。
- 两级验收不可混淆：21 条 AI 实现/稳定性证据通过且外部阻塞被精确记录后，可标记 `implementation_complete_external_blocked` 并开始独立 Exchange v2 设计；只有真实 v2 proof、Phase-3 switch 和生产观察窗通过后，才可标记 `production_activated` 并执行旧路径删除。

---

## File Map

| Area | Files | Responsibility |
|---|---|---|
| Runtime | `src/runtime/resources.py`, `src/runtime/blocking.py`, `src/init_app.py`, `src/main.py` | Own/close clients, Workers and bounded executors; prevent event-loop blocking |
| Observability | `src/observability/catalog.py`, `src/observability/db_collector.py`, `src/observability/runtime_collector.py`, `src/observability/tracing.py` | Produce metrics from real execution points and PostgreSQL facts with safe correlation |
| Operations | `src/operations/alerts.py`, `src/operations/runbooks.py`, `docs/runbooks/` | Stateful low-noise alerts linked to executable recovery procedures |
| Lifecycle | `src/maintenance/retention.py`, `src/maintenance/backup.py`, `src/maintenance/legacy_cleanup.py` | Schedule guarded retention, verify restore, and contract historical data safely |
| Cutover | `src/maintenance/cutover.py`, `src/maintenance/reconcile.py`, `src/maintenance/cli.py` | Post-activation plan/quiesce/rotate/retire/rollback wrappers with fencing, v2 and high-water proof; no initial switch authority |
| Quality | `tests/reliability/`, `tests/fault_injection/`, `tests/contracts/`, `tests/performance/`, `scripts/check_coverage_ratchet.py` | Non-skippable reliability, contract, performance and coverage release gates |
| Supply chain | `.github/workflows/ci.yml`, `Dockerfile`, `uv.lock`, `scripts/check_migrations.py`, `scripts/generate_sbom.sh` | Reproducible checks, migrations, locked image, SBOM and vulnerability/secret gates |
| Contracts/evidence | `docs/architecture/`, `docs/operations/`, `docs/api/openapi.json`, `docs/superpowers/reports/` | Operator/API contract, final evidence and service-repository boundary |

### Task 1: Remove Event-loop Blocking and Close Runtime Resources

**Files:**
- Create: `src/runtime/__init__.py`
- Create: `src/runtime/resources.py`
- Create: `src/runtime/blocking.py`
- Create: `tests/unit/runtime/test_resources.py`
- Create: `tests/unit/runtime/test_blocking_boundary.py`
- Create: `tests/unit/runtime/conftest.py`
- Create: `tests/performance/test_event_loop_latency.py`
- Modify: `src/init_app.py`
- Modify: `src/main.py`
- Modify: `src/utils/exchange_api.py`
- Modify: `src/utils/lark_file_ops.py`
- Modify: `src/utils/lark_messaging.py`
- Modify: `src/utils/lark_pdf_flow.py`
- Modify: `src/utils/retriever.py`
- Modify: `src/memory/preference_learner.py`

**Interfaces:**
- Consumes: Phase 2/3/4 Worker lifecycles and injected Exchange/Lark/Qdrant/Model adapters
- Produces: `RuntimeResources.start()`, `RuntimeResources.close(grace_seconds: float)`; named bounded executors; limited-concurrency Exchange detail fetch

- [ ] **Step 1: Write resource ownership and blocking-boundary tests**

```python
import asyncio
import inspect

import pytest

from src.runtime.resources import RuntimeResources


@pytest.mark.asyncio
async def test_resources_close_every_owned_client_and_worker():
    calls: list[str] = []
    resources = RuntimeResources.for_test(calls)
    await resources.start()
    await resources.close(grace_seconds=1.0)
    assert calls == [
        "workers.stop_accepting",
        "workers.drain",
        "scheduler.close",
        "model_client.aclose",
        "exchange_client.aclose",
        "db_pool.close",
        "executors.shutdown",
    ]


def test_async_modules_do_not_call_known_sync_clients_directly():
    modules = (lark_file_ops, lark_messaging, lark_pdf_flow, retriever, preference_learner)
    for module in modules:
        source = inspect.getsource(module)
        assert "asyncio.create_task(" not in source
        assert ".message.create(" not in source or "run_blocking(" in source
        assert ".upsert(" not in source or "run_blocking(" in source
```

- [ ] **Step 2: Implement named bounded blocking pools**

`run_blocking(kind, callable, *args)` routes PDF CPU work to a process pool of 2 and Lark/Qdrant/disk SDK calls to a thread pool of 8. A semaphore limits each kind independently; queue wait has a metric and timeout. Every call preserves contextvars and cancellation stops waiting, while shutdown waits at most the configured grace period.

```python
class BlockingKind(StrEnum):
    PDF = "pdf"
    LARK = "lark"
    QDRANT = "qdrant"
    DISK = "disk"
```

`tests/unit/runtime/conftest.py` defines fake closeable clients, Worker group, scheduler and executor registry; `RuntimeResources.for_test(calls)` uses those exact fakes and never starts a real external client.

- [ ] **Step 3: Bound Exchange detail requests and own all clients**

Reuse one `httpx.AsyncClient` per service. Fetch Exchange details with a semaphore default of 8 and `asyncio.TaskGroup`; preserve input order and return typed per-ID failures rather than cancelling successful siblings. `RuntimeResources.close()` stops new claims, drains Workers, closes scheduler/model/Exchange/PostgreSQL resources and executors in the tested order.

- [ ] **Step 4: Measure event-loop responsiveness**

Run 200 fake Lark/Qdrant calls and 40 PDF renders while a 10 ms heartbeat records lag. The performance test asserts p99 event-loop lag below 100 ms on the CI resource class and no more than 8 simultaneous Exchange detail calls. It uses deterministic local fakes and marks no network-dependent skip.

- [ ] **Step 5: Verify and commit**

```bash
.venv/bin/python -m pytest tests/unit/runtime tests/performance/test_event_loop_latency.py tests/unit/test_worker_concurrency.py -q
.venv/bin/ruff check src/runtime src/init_app.py src/main.py src/utils tests/unit/runtime tests/performance/test_event_loop_latency.py
git add src/runtime src/init_app.py src/main.py src/utils/exchange_api.py src/utils/lark_file_ops.py src/utils/lark_messaging.py src/utils/lark_pdf_flow.py src/utils/retriever.py src/memory/preference_learner.py tests/unit/runtime tests/performance/test_event_loop_latency.py tests/unit/test_worker_concurrency.py
git commit -m "perf: remove event loop blocking and own resources"
```

Expected: all owned resources close once, no untracked task remains, Exchange detail concurrency is bounded at 8, and heartbeat p99 stays below 100 ms.

---

### Task 2: Replace Synthetic Metrics with Runtime and Database Truth

**Files:**
- Create: `src/observability/catalog.py`
- Create: `src/observability/db_collector.py`
- Create: `src/observability/runtime_collector.py`
- Create: `src/observability/tracing.py`
- Create: `tests/unit/observability/test_metric_catalog.py`
- Create: `tests/integration/observability/test_db_metrics.py`
- Create: `tests/unit/observability/test_cardinality.py`
- Create: `tests/unit/observability/conftest.py`
- Create: `tests/integration/observability/conftest.py`
- Modify: `src/observability/metrics.py`
- Modify: `src/server.py`
- Modify: `src/main.py`

**Interfaces:**
- Produces: `MetricSnapshotRepository.snapshot(now) -> MetricSnapshot`; `RuntimeCollector.collect() -> RuntimeSnapshot`; correlation context manager

- [ ] **Step 1: Write a database-truth and cardinality test**

```python
from datetime import UTC, datetime, timedelta

import pytest


@pytest.mark.asyncio
async def test_db_metrics_match_seeded_business_rows(metric_repo, db):
    now = datetime.now(UTC)
    await db.seed_inbox(created_at=now - timedelta(minutes=7), state="pending")
    await db.seed_outbox(created_at=now - timedelta(minutes=6), state="pending")
    await db.seed_email(state="send_unknown")
    snapshot = await metric_repo.snapshot(now)
    assert snapshot.inbox_pending == 1
    assert snapshot.inbox_oldest_seconds == pytest.approx(420, abs=2)
    assert snapshot.outbox_oldest_seconds == pytest.approx(360, abs=2)
    assert snapshot.send_unknown == 1


def test_metric_labels_are_low_cardinality(metric_catalog):
    forbidden = {"request_id", "workflow_run_id", "email_id", "inbox_id", "outbox_id", "open_id"}
    for metric in metric_catalog:
        assert forbidden.isdisjoint(metric.label_names)
```

- [ ] **Step 2: Define and wire the complete metric catalog**

Implement gauges/counters/histograms for Inbox count/age/retry/dead-letter; Sync lag/cursor age/change count; stage throughput/latency/ErrorKind; processing/waiting/expired/over-age accepted/send_unknown; all Outbox count/age/delivery latency; model role calls/latency/Tokens/failures/retries/breaker; Exchange/Lark/PostgreSQL/Qdrant latency/error; checkpoint count/bytes/deletions; major table bytes/daily growth; RSS/container memory/event-loop lag/task count/pool usage; Qdrant projection lag/failure. Remove `webhook_queue_depth` and every metric that has no update at a real claim/complete/query point.

- [ ] **Step 3: Add correlation without label explosion**

`bind_correlation(request_id, workflow_run_id, email_id, inbox_id, outbox_id)` writes IDs to contextvars used by structlog, Trace spans and audit inserts. It never adds them to Prometheus labels. External spans record host category, outcome and error kind but no URL query, address, body or card data.

- [ ] **Step 4: Bound collector cost and protect `/metrics`**

One database query snapshot is cached for 15 seconds and times out after 2 seconds. A failed refresh serves the last successful snapshot with `metrics_snapshot_stale=1`. `/metrics` retains Phase 5 network/token authorization and returns 503 only if no successful snapshot has ever existed.

The two observability conftests define `metric_catalog`, a fixed clock, `db`, row seed methods and `metric_repo`; the integration fixture uses the shared migrated PostgreSQL schema and clears the Prometheus registry after each test.

- [ ] **Step 5: Verify and commit**

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/unit/observability tests/integration/observability tests/unit/test_metrics.py -q
git add src/observability src/server.py src/main.py tests/unit/observability tests/integration/observability tests/unit/test_metrics.py
git commit -m "feat: expose metrics from runtime and database truth"
```

Expected: seeded row counts/ages exactly match exported values, no forbidden high-cardinality label exists, and stale collection is visible rather than fabricated.

---

### Task 3: Deliver Actionable Alerts Through a Dedicated Operations Outbox

**Files:**
- Create: `alembic/versions/20260713_0013_operations_control.py`
- Create: `src/operations/__init__.py`
- Create: `src/operations/alerts.py`
- Create: `src/operations/outbox.py`
- Create: `src/operations/runbooks.py`
- Create: `tests/unit/operations/test_alert_rules.py`
- Create: `tests/integration/operations/test_alert_outbox.py`
- Create: `tests/unit/operations/conftest.py`
- Create: `tests/integration/operations/conftest.py`
- Create: `docs/runbooks/inbox-lag.md`
- Create: `docs/runbooks/sync-stale.md`
- Create: `docs/runbooks/send-unknown.md`
- Create: `docs/runbooks/accepted-stale.md`
- Create: `docs/runbooks/dead-letter.md`
- Create: `docs/runbooks/approval-expiry.md`
- Create: `docs/runbooks/outbox-lag.md`
- Create: `docs/runbooks/storage-growth.md`
- Create: `docs/runbooks/memory-pressure.md`
- Create: `docs/runbooks/model-failure.md`
- Create: `docs/runbooks/authentication-failure.md`
- Modify: `src/outbox/runtime.py`
- Modify: `src/config.py`
- Modify: `.env.example`
- Modify: `src/db/access_contract.py`
- Modify: `src/db/bootstrap.py`
- Modify: `src/db/schema.py`
- Modify: `src/db/schema_contract.py`
- Modify: `src/maintenance/checkpoint_repository.py`
- Modify: `tests/unit/test_database_revision.py`
- Modify: `tests/unit/test_database_schema_contract.py`
- Modify: `tests/unit/test_checkpoint_repository_safety.py`
- Modify: `tests/unit/test_alembic_offline.py`
- Modify: `tests/integration/ingestion/test_schema.py`
- Modify: `tests/integration/ingestion/test_access_roles.py`
- Create: `tests/integration/migrations/test_0012_to_0013.py`

**Interfaces:**
- Consumes: Phase 6 `MetricSnapshot` and runtime snapshot
- Produces: idempotent `AlertEvaluator.evaluate(snapshot, now) -> list[AlertTransition]`; runbook-linked `OperationsNotificationWorker`; backup/restore evidence repository

Migration revision is exactly `20260713_0013` with linear `down_revision = "20260713_0012"`.

- [ ] **Step 1: Write exact threshold and dedupe tests**

```python
from datetime import UTC, datetime, timedelta


def test_default_alert_thresholds_and_escalation(evaluator):
    now = datetime.now(UTC)
    snapshot = evaluator.snapshot(
        inbox_oldest_seconds=301,
        sync_last_success_at=now - timedelta(minutes=11),
        send_unknown=1,
        accepted_oldest_seconds=3601,
        new_dead_letters=1,
        expired_approvals=1,
        outbox_oldest_seconds=301,
        memory_ratio=0.91,
        model_consecutive_failures=5,
        exchange_auth_failed=True,
    )
    transitions = evaluator.evaluate(snapshot, now)
    assert {item.rule_id for item in transitions if item.state == "firing"} == {
        "inbox-oldest-5m",
        "sync-missed-two-periods",
        "send-unknown-any",
        "accepted-overdue",
        "dead-letter-new",
        "approval-expired",
        "outbox-oldest-5m",
        "memory-over-90",
        "model-failure-streak",
        "exchange-auth-failed",
    }


def test_same_firing_state_creates_one_notification(evaluator, outbox):
    first = evaluator.persist(evaluator.snapshot(send_unknown=1))
    second = evaluator.persist(evaluator.snapshot(send_unknown=1))
    assert first.created_notifications == 1
    assert second.created_notifications == 0
```

- [ ] **Step 2: Add operations-control schema and stateful alert transitions**

Migration `0013` creates `operations_alert_states`, `operations_notification_outbox`, `backup_snapshots` and `restore_verifications`. The operations Outbox has its own stable rule/transition business key, account category, state/severity, redacted payload hash/ref, lease/attempt/available/error/delivery evidence and operations build/config epoch; it deliberately has **no email, draft, card, business generation or `pipeline_ownership` foreign key**. Backup/restore rows store snapshot ID, scope/high-water/schema/build/config hashes, encryption/integrity status, verifier, immutable evidence hash and timestamps, never credentials or content.

Rules include every design threshold: Inbox >5m; Sync misses 2 five-minute cycles; any `send_unknown`; accepted older than `ACCEPTED_CONFIRMATION_SLA_SECONDS`; new dead letter; approval expiration; any Outbox >5m; abnormal checkpoint/table growth; memory >80% for 10m and >90% for 2m escalation; model failure streak/breaker open; Exchange/Lark authentication failure. Store `pending/firing/resolved`, first/last seen, severity and dedupe key in PostgreSQL; only state changes create `operations_notification_outbox` rows. `OperationsNotificationWorker` is lifecycle- and credential-fenced independently from business Card Outbox authority and may deliver in legacy-authoritative, Shadow, quiescing or Durable modes; it cannot create/consume card actions, mark mail, send mail or claim any business Outbox. Thus external-blocked implementation still has operational alert delivery without waking dormant Durable business workers.

`0013` advances the exact single head/schema digest, bootstrap checks, four ACL manifests, checkpoint allowlist and offline SQL. The operations runtime gets only alert evaluation/lease/complete columns; maintenance owns backup/restore evidence transitions but no DDL; auditor is SELECT-only; migration owns DDL. `tests/integration/migrations/test_0012_to_0013.py` proves the disabled-business-profile code-first real-PostgreSQL bridge, row preservation, exact role isolation between operations and business Outboxes, startup, second no-op upgrade, old-head rejection and a single empty-DB head.

- [ ] **Step 3: Make every Runbook operational**

Each Markdown Runbook contains: symptom and threshold; safe SQL/metric queries; diagnosis tree; actions that are safe automatically; actions requiring administrator confirmation; rollback/cutover constraints; verification; escalation owner; forbidden action. `send-unknown.md` explicitly forbids retry and describes only remote verification, mark-sent, mark-not-sent, or new authorization. Alert payloads contain rule ID, count/age bucket, account category, correlation ID and Runbook path, never mail/body/address/token.

The operations conftests define `evaluator`, fixed time, PostgreSQL alert state and fake Operations Notification Outbox used by both rule and dedupe tests.

- [ ] **Step 4: Verify and commit**

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/unit/operations/test_alert_rules.py tests/integration/operations/test_alert_outbox.py tests/integration/migrations/test_0012_to_0013.py tests/integration/ingestion/test_schema.py tests/integration/ingestion/test_access_roles.py -q
.venv/bin/python -m pytest tests/unit/test_database_revision.py tests/unit/test_database_schema_contract.py tests/unit/test_checkpoint_repository_safety.py tests/unit/test_alembic_offline.py -q
git add alembic/versions/20260713_0013_operations_control.py src/operations src/outbox/runtime.py src/config.py .env.example src/db/access_contract.py src/db/bootstrap.py src/db/schema.py src/db/schema_contract.py src/maintenance/checkpoint_repository.py tests/unit/operations tests/integration/operations tests/unit/test_database_revision.py tests/unit/test_database_schema_contract.py tests/unit/test_checkpoint_repository_safety.py tests/unit/test_alembic_offline.py tests/integration/ingestion/test_schema.py tests/integration/ingestion/test_access_roles.py tests/integration/migrations/test_0012_to_0013.py docs/runbooks
git commit -m "feat: add actionable durable operational alerts"
```

Expected: all default rules fire and resolve deterministically, repeated evaluation does not spam, and every rule ID resolves to an existing Runbook.

---

### Task 4: Schedule Retention, Garbage Collection, Backup, and Restore Verification

**Files:**
- Modify: `src/maintenance/retention.py`
- Create: `src/maintenance/backup.py`
- Create: `src/maintenance/cli.py`
- Create: `tests/unit/maintenance/test_retention.py`
- Create: `tests/integration/maintenance/test_retention_gc.py`
- Create: `tests/integration/maintenance/test_backup_restore.py`
- Modify: `tests/unit/maintenance/conftest.py`
- Modify: `tests/integration/maintenance/conftest.py`
- Create: `docs/runbooks/retention-and-gc.md`
- Create: `docs/runbooks/backup-restore.md`
- Modify: `src/outbox/runtime.py`
- Modify: `src/main.py`
- Modify: `src/config.py`
- Modify: `.env.example`

**Interfaces:**
- Consumes: Phase 5 `RetentionPolicy`, ContentStore references/holds, Projection Outbox and backup credentials
- Produces: scheduled `RetentionPlanner.plan(now, limit) -> RetentionPlan`; backup-gated `RetentionExecutor.execute(plan_id, authorization, snapshot_id)`; verified restore report

- [ ] **Step 1: Write hold, cascade, and idempotency tests**

```python
from datetime import UTC, datetime, timedelta

import pytest


@pytest.mark.asyncio
async def test_unresolved_send_states_hold_payload_and_content(planner, db):
    now = datetime.now(UTC)
    await db.seed_email(state="send_unknown", terminal_at=now - timedelta(days=400), content_ref="c1")
    await db.seed_send_intent(state="accepted", created_at=now - timedelta(days=90), content_ref="c2")
    plan = await planner.plan(now=now, limit=100)
    assert {"c1", "c2"}.isdisjoint(plan.content_refs_to_delete)


@pytest.mark.asyncio
async def test_content_delete_enqueues_qdrant_delete_once(executor, db, verified_snapshot):
    plan = await db.seed_retention_plan(content_ref="c3", qdrant_point_id="p3")
    await executor.execute(plan.id, snapshot_id=verified_snapshot.id)
    await executor.execute(plan.id, snapshot_id=verified_snapshot.id)
    assert await db.count_projection_delete("p3") == 1
```

- [ ] **Step 2: Encode every default retention period**

Use Phase 5 policy values: terminal content/artifacts 30d; waiting approval until completion/expiry; unresolved accepted/send_unknown until resolution then payload 30d; successful Inbox raw payload 7d and resolved dead letter 30d; drafts/send intents/Outbox payload 30d after terminal; preview/PDF/temp immediate and at most 24h; terminal checkpoint 24h; Qdrant coupled to content; redacted logs/Trace 30d; ordinary state/audit 180d; send_unknown/security/manual audit metadata 365d; encrypted backup 30d. Make each value configurable but never permit a production value longer than the governance maximum without an explicit policy version and audit record.

- [ ] **Step 3: Implement plan, guarded execute, and recovery**

Planner writes immutable candidate IDs and counts without deleting. Executor requires a verified encrypted snapshot newer than the plan, claims at most 500 rows with resumable checkpoints, clears payload columns before deleting audit metadata, decrements account-scoped content references, respects legal/send holds, and enqueues stable Qdrant delete operations. Partial failure records cursor/error and resumes without repeating completed operations. Snapshot creation and verification write the `backup_snapshots`/`restore_verifications` tables introduced by `0013`; an in-memory flag, filename or unsigned CLI output is never backup evidence.

- [ ] **Step 4: Prove encrypted backup restore**

Create a scrubbed test database with Inbox, approval, `accepted`, `send_unknown`, ContentStore ciphertext and projection rows; run `pg_dump` plus encrypted content snapshot, restore into an isolated database/storage directory, validate hashes and invariants, then destroy the restored test data. Save only counts, hashes and timestamps in the verification record.

Extend the maintenance conftests with `verified_snapshot`, backup repository, isolated restore database/storage and an executor that consumes the Phase 5 retention plan. The fixture destroys restore artifacts in `finally`, including failed-test paths.

- [ ] **Step 5: Verify and commit**

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test RESTORE_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_restore_test .venv/bin/python -m pytest tests/unit/maintenance tests/integration/maintenance -q
git add src/maintenance/retention.py src/maintenance/backup.py src/maintenance/cli.py src/outbox/runtime.py src/main.py src/config.py .env.example tests/unit/maintenance tests/integration/maintenance docs/runbooks/retention-and-gc.md docs/runbooks/backup-restore.md
git commit -m "feat: enforce retention with verified recovery"
```

Expected: dry-run and execution counts match, protected snapshots survive, Qdrant deletion is coupled, interrupted work resumes, and the isolated restore passes integrity checks.

---

### Task 5: Build the Complete Reliability Test Matrix and Coverage Ratchet

**Files:**
- Create: `tests/reliability/test_release_gates.py`
- Create: `tests/fault_injection/test_send_crash.py`
- Create: `tests/fault_injection/test_card_confirmation_loss.py`
- Create: `tests/fault_injection/test_cutover_fencing.py`
- Create: `tests/fault_injection/test_rollback_drain.py`
- Create: `tests/contracts/test_exchange_contract.py`
- Create: `tests/contracts/test_lark_contract.py`
- Create: `tests/contracts/snapshots/exchange-openapi.json`
- Create: `tests/contracts/snapshots/lark-card-events.json`
- Create: `tests/performance/test_memory_stability.py`
- Create: `scripts/check_coverage_ratchet.py`
- Create: `.coverage-baseline.json`
- Modify: `pyproject.toml`

**Interfaces:**
- Consumes: all Phase 1-5 state machines/adapters and real PostgreSQL fixtures
- Produces: eight non-skippable fault gates, contract drift checks, 80% global/90% critical coverage enforcement

- [ ] **Step 1: Encode the eight mandatory reliability scenarios**

Add explicit tests for: remote send completed then local crash → one `send_unknown`, no retry; post-approval mutation cannot change frozen snapshot; reauthorization creates new causal records; Lark card delivered but local acknowledgement lost → one transition; stale fencing token cannot commit any side effect; rollback drains all previously accepted Inbox/Outbox; shuffled create/update/read/delete does not reopen terminal mail; content references/retention/send holds prevent early GC. Tests use real PostgreSQL transactions and fake boundary adapters only; none may carry a skip marker.

- [ ] **Step 2: Add contract snapshots and drift classification**

Validate Exchange Webhook, Sync, details, reply, forward and send responses plus Lark signed card events against committed normalized snapshots. Snapshot comparison ignores descriptions/order but fails on removed fields, narrowed enums, changed requiredness, response status/meaning or signature fields. An intentional change requires updating the snapshot and a `contract-change` changelog entry.

- [ ] **Step 3: Add reproducible memory stability load**

Process 2,000 generated mails in 20 batches with a fixed seed, including 10 MiB bodies and allowed maximum attachments. Force GC between batches and record RSS, checkpoint bytes, content-store bytes and task count. Assert the linear slope of post-GC RSS across the final 10 batches is below 1 MiB per 100 messages, final steady RSS is below 80% of the 2 GiB container limit, attachment spike recovers within 15% of pre-batch RSS, and terminal checkpoint count follows the 24-hour policy rather than cumulative mail count.

- [ ] **Step 4: Implement the coverage ratchet**

`.coverage-baseline.json` records global 53 and per-critical-package values measured at implementation start. `check_coverage_ratchet.py` reads coverage JSON, rejects any package regression, requires each changed critical package at least 90 and global coverage to advance in review increments until it reaches 80; once 80 is recorded it cannot be lowered. Empty tests and module-level/permanent skips fail a collection audit.

- [ ] **Step 5: Verify and commit**

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/reliability tests/fault_injection tests/contracts -q
.venv/bin/python -m pytest --cov=src --cov-report=json:coverage.json -q
.venv/bin/python scripts/check_coverage_ratchet.py coverage.json .coverage-baseline.json
.venv/bin/python -m pytest tests/performance/test_memory_stability.py -q
git add tests/reliability tests/fault_injection tests/contracts tests/performance/test_memory_stability.py scripts/check_coverage_ratchet.py .coverage-baseline.json pyproject.toml
git commit -m "test: add reliability release gates and coverage ratchet"
```

Expected: all eight release scenarios and both contract snapshots pass without skip; measured coverage never regresses and memory/checkpoint growth stays bounded.

---

### Task 6: Make CI and the Supply Chain Reproducible

**Files:**
- Create: `.github/workflows/ci.yml`
- Create: `.github/dependabot.yml`
- Create: `scripts/check_migrations.py`
- Create: `scripts/generate_sbom.sh`
- Create: `tests/migration/test_legacy_snapshot_upgrade.py`
- Create: `tests/fixtures/db/legacy-schema.sql`
- Modify: `Dockerfile`
- Modify: `docker-compose.yml`
- Modify: `pyproject.toml`
- Modify: `uv.lock`
- Modify: `.dockerignore`
- Delete: `requirements.txt`

**Interfaces:**
- Produces: deterministic dependency sync, image build, SBOM, vulnerability/secret gates and database upgrade matrix

- [ ] **Step 1: Pin the declared supply chain**

Pin Python base to `python:3.12-slim@sha256:423ed6ab25b1921a477529254bfeeabf5855151dc2c3141699a1bfc852199fbf`, PostgreSQL to `postgres@sha256:f30e3de0ac9cc938dac627ef2231099867c694b5f949fadb924c8c977428c399`, and Qdrant to `qdrant/qdrant@sha256:f1c7272cdac52b38c1a0e89313922d940ba50afd90d593a1605dbbc214e66ffb`. Builder installs `uv==0.11.28`; add pytest/ruff/pyright/pip-audit/cyclonedx-bom/bandit to the `dev` dependency group. Runtime installs only the frozen production group with `uv sync --frozen --no-dev --no-editable`. Delete `requirements.txt` after Docker/CI stop reading it so there is one dependency source.

- [ ] **Step 2: Define the CI job graph**

Jobs are: `lint-type-lock` (`ruff format --check`, `ruff check`, `pyright`, `uv lock --check`); `unit`; `postgres-integration`; `security` (`pip-audit`, Bandit high severity, Gitleaks); `migration` (empty DB, legacy snapshot, second idempotent upgrade); `contracts`; `coverage`; `docker` (BuildKit); `sbom-scan` (CycloneDX + Trivy HIGH/CRITICAL); `release-gate`. Use PostgreSQL service with ephemeral credentials and Qdrant only for projection tests. No real mail/Lark/model secret is exposed to pull requests.

- [ ] **Step 3: Implement exact migration verification**

`scripts/check_migrations.py` creates three isolated schemas: empty; restored `legacy-schema.sql`; and already-current. It runs Alembic upgrade, LangGraph autocommit setup, schema check and a second upgrade, then asserts exactly one Alembic head, unchanged application row counts and no pending checkpoint migration. The legacy fixture contains structure and synthetic hashes only, no production data.

- [ ] **Step 4: Generate and scan the final artifact**

Generate SPDX/CycloneDX SBOMs for Python and the image, attach SHA256, and run Trivy against the exact built image ID. HIGH/CRITICAL vulnerabilities fail unless a time-bounded exception file names CVE, owner, reason and expiry. Gitleaks scans history and worktree; no allowlist may contain an actual credential.

- [ ] **Step 5: Verify and commit**

```bash
uv remove pytest pytest-asyncio pytest-cov pytest-mock
uv add --dev pytest pytest-asyncio pytest-cov pytest-mock ruff pyright pip-audit cyclonedx-bom bandit
uv lock
uv lock --check
uv run ruff format --check src tests
uv run ruff check src tests
uv run pyright src
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test uv run python scripts/check_migrations.py
docker build --target runtime -t ai-exchange:release-gate .
bash scripts/generate_sbom.sh ai-exchange:release-gate artifacts/sbom
git add .github Dockerfile docker-compose.yml pyproject.toml uv.lock .dockerignore scripts/check_migrations.py scripts/generate_sbom.sh tests/migration/test_legacy_snapshot_upgrade.py tests/fixtures/db/legacy-schema.sql
git rm requirements.txt
git commit -m "ci: enforce reproducible secure release gates"
```

Expected: lock, type, migration, image and SBOM commands exit 0; image references and dependencies are immutable in the produced evidence.

---

### Task 7: Publish the Operator and API Contract Documentation

**Files:**
- Create: `docs/architecture/target-architecture.md`
- Create: `docs/architecture/data-lifecycle.md`
- Create: `docs/operations/deployment.md`
- Create: `docs/operations/migration.md`
- Create: `docs/operations/rollback.md`
- Create: `docs/operations/key-rotation.md`
- Create: `docs/operations/cold-start-sync.md`
- Create: `docs/operations/send-unknown.md`
- Create: `docs/operations/monitoring.md`
- Create: `docs/api/openapi.json`
- Create: `tests/docs/test_documentation_contract.py`
- Create: `tests/docs/conftest.py`
- Modify: `README.md`
- Modify: `CLAUDE.md`
- Modify: `src/server.py`

**Interfaces:**
- Consumes: final settings, endpoints, states, feature flags, Runbooks and generated FastAPI schema
- Produces: checked-in OpenAPI snapshot and one navigable operator documentation set

- [ ] **Step 1: Write documentation contract tests**

```python
import json
from pathlib import Path


def normalize_openapi(value):
    if isinstance(value, dict):
        return {key: normalize_openapi(item) for key, item in sorted(value.items()) if key != "x-build-time"}
    if isinstance(value, list):
        return [normalize_openapi(item) for item in value]
    return value


def test_openapi_snapshot_matches_application(app):
    committed = json.loads(Path("docs/api/openapi.json").read_text())
    assert normalize_openapi(app.openapi()) == normalize_openapi(committed)


def test_every_alert_and_setting_has_documentation(alert_catalog, settings_schema):
    monitoring = Path("docs/operations/monitoring.md").read_text()
    deployment = Path("docs/operations/deployment.md").read_text()
    for rule in alert_catalog:
        assert rule.rule_id in monitoring
        assert Path(rule.runbook_path).exists()
    for name in settings_schema.required_production_names:
        assert name in deployment
```

`tests/docs/conftest.py` builds `app`, `alert_catalog` and `settings_schema` from production code without starting Workers; it verifies every referenced Markdown path under the repository root.

- [ ] **Step 2: Document architecture and data lifecycle**

Show PostgreSQL facts, ContentStore, lightweight Graph, Inbox/Sync, approval/send/mailbox/notification/projection Outboxes and external adapters. The lifecycle document maps every data class to encryption, references/holds, default retention, delete order and audit residue. State diagrams use the exact enum names implemented in Phases 1-4.

- [ ] **Step 3: Document every operator workflow**

Deployment includes secrets, TLS/CA, networks, migration ordering and readiness. Migration/rollback include generation/fencing and high-water checks. Key rotation includes new-write version, re-encryption, verification and old-key retirement. Cold-start includes preview/suppression approval. `send-unknown` forbids retry and lists manual evidence/actions. Monitoring maps every alert to metrics and Runbook.

- [ ] **Step 4: Normalize and commit OpenAPI**

Export FastAPI OpenAPI after hiding internal-only implementation detail, include schemas for 202 intake, sync management, manual send resolution and generic errors, and add descriptions of accepted versus sent semantics. Sort maps/lists deterministically and strip build timestamps before committing.

- [ ] **Step 5: Verify and commit**

```bash
.venv/bin/python -m pytest tests/docs/test_documentation_contract.py -q
.venv/bin/python -c 'import json; from src.server import app; print(json.dumps(app.openapi(), ensure_ascii=False, sort_keys=True))' >/tmp/ai-exchange-openapi-check.json
.venv/bin/python -m json.tool docs/api/openapi.json >/dev/null
git add docs/architecture docs/operations docs/api/openapi.json README.md CLAUDE.md src/server.py tests/docs/conftest.py tests/docs/test_documentation_contract.py
git commit -m "docs: publish production architecture and operations contract"
```

Expected: OpenAPI matches runtime, every production setting and alert is documented, and all internal links resolve.

---

### Task 8: Operationalize Post-activation Rotation, Reconciliation, and Rollback

**Files:**
- Create: `src/maintenance/cutover.py`
- Create: `src/maintenance/reconcile.py`
- Create: `tests/unit/maintenance/test_cutover_protocol.py`
- Create: `tests/integration/maintenance/test_cutover_fencing.py`
- Create: `tests/integration/maintenance/test_rollback_drain.py`
- Create: `docs/runbooks/pipeline-cutover.md`
- Modify: `tests/unit/maintenance/conftest.py`
- Modify: `tests/integration/maintenance/conftest.py`
- Modify: `src/maintenance/cli.py`
- Modify: `src/config.py`
- Modify: `.env.example`

**Interfaces:**
- Consumes: six per-account feature flags, `pipeline_ownership`, generation/fencing repositories and all Inbox/Outbox counts
- Produces: dry-run post-activation `RotationPlan`; reconciliation/retirement orchestration and CLI wrappers that delegate every authority/ownership mutation to Phase 3 `ActivationService`

- [ ] **Step 1: Write state-machine and stale-worker tests**

```python
import pytest


@pytest.mark.asyncio
async def test_rotation_requires_existing_durable_authority_and_high_water(
    service, account
):
    await account.seed_authority(mode="durable_active")
    plan = await service.plan_rotation(account.id, target_pipeline="durable-v2")
    await service.quiesce(plan.id)
    await account.seed_sending_attempt()
    with pytest.raises(CutoverBlocked, match="sending_attempts"):
        await service.rotate(plan.id)


@pytest.mark.asyncio
async def test_operations_cli_cannot_perform_initial_activation(service, account):
    await account.seed_authority(mode="shadow")
    with pytest.raises(CutoverBlocked, match="phase3_activation_required"):
        await service.plan_rotation(account.id, target_pipeline="durable-v2")


@pytest.mark.asyncio
async def test_stale_generation_cannot_claim_or_complete_any_outbox(service, stale_worker):
    await service.rotate(await service.ready_rotation_plan())
    for outbox_kind in ("notification", "mailbox", "send", "projection"):
        assert await stale_worker.claim(outbox_kind) == []
        with pytest.raises(StaleFence):
            await stale_worker.complete_existing(outbox_kind)
```

- [ ] **Step 2: Implement the post-activation rotation protocol**

This task is operational tooling for an account that Phase 3 has already activated; it cannot perform the first `shadow/quiescing -> durable_active` transition. A rotation plan snapshots the current authority receipt/barrier, old/new Webhook and Sync high water, Inbox/Outbox by state/generation, email state, in-flight sending and the currently deployed `exchange_sync_contract_v2` build/profile evidence. The wrapper requests quiesce, evidence sealing and generation rotation through Phase 3 `ActivationService`; it contains no direct SQL that mutates `pipeline_runtime_authority`, `pipeline_ownership` or consumes a cutover barrier. Contract-version drift blocks rotation/rollback activation rather than silently falling back to the legacy extension. All `sending` attempts must reach a clear result or `send_unknown`. After the delegated atomic rotation, new events bind only to the new generation and the old generation drains already-owned work. Observe Webhook/Sync reconciliation, then request retirement only after zero owned work and matching high water. An architecture scan keeps `src/ingestion/activation.py` as the sole production authority/ownership mutation site.

- [ ] **Step 3: Bind all six flags without granting ownership**

Add per-account desired settings for `DURABLE_INBOX_ENABLED`, `SYNC_RECONCILIATION_ENABLED`, `NEW_APPROVAL_FLOW_ENABLED`, `SEND_OUTBOX_ENABLED`, `LIGHTWEIGHT_GRAPH_STATE_ENABLED`, and `QDRANT_OUTBOX_ENABLED`. Flags describe a requested rotation profile only; they neither select nor create a generation and never grant execution. Phase 3 `ActivationService` validates the requested profile against current database authority and its new immutable barrier. Every actual claim/complete still checks authority plus `pipeline_ownership`; email ownership remains sticky at approval/send-intent creation.

- [ ] **Step 4: Implement forward-only rollback**

The operations CLI delegates rollback to Phase 3 `ActivationService.rollback()` with a new append-only command receipt; it never recreates that transaction. The service creates a fresh current `legacy_compat` Durable generation and moves the problem generation to draining without rewriting existing Inbox/Outbox ownership or replaying `send_unknown`. Because authority remains `durable_active`, the selector maps that new current generation only to `DurableLegacyCompatAdapter`: PostgreSQL Inbox plus four business Outboxes, zero legacy direct effects. It must never resolve to `LegacyProcessingAdapter`, which remains limited to pre-switch legacy/Shadow or already-stamped old draining work. Optional migration of a pending item requires a separate audited command that proves no remote request started, updates ownership/fencing atomically through the same service and records the old/new causal ID.

Extend the maintenance conftests with `service`, `account`, `stale_worker`, `cleaner` and verified snapshot/high-water builders. The stale Worker fake attempts Notification, Mailbox, Send and Projection claim/complete through production repositories.

- [ ] **Step 5: Verify and commit**

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/unit/maintenance/test_cutover_protocol.py tests/integration/maintenance/test_cutover_fencing.py tests/integration/maintenance/test_rollback_drain.py tests/fault_injection/test_cutover_fencing.py tests/fault_injection/test_rollback_drain.py -q
git add src/maintenance/cutover.py src/maintenance/reconcile.py src/maintenance/cli.py src/config.py .env.example tests/unit/maintenance/conftest.py tests/integration/maintenance/conftest.py tests/unit/maintenance/test_cutover_protocol.py tests/integration/maintenance/test_cutover_fencing.py tests/integration/maintenance/test_rollback_drain.py docs/runbooks/pipeline-cutover.md
git commit -m "feat: operationalize fenced generation rotation and rollback"
```

Expected: stale generations fail every side-effect path, 202 Inbox and all Outboxes remain owned/drainable during rollback, no rotation occurs with unresolved sending, and Phase 6 cannot perform an initial Durable activation or mutate authority outside Phase 3 `ActivationService`.

---

### Task 9: Govern Historical Cleanup and Prepare Legacy-path Contraction

**Files:**
- Create: `alembic/versions/20260713_0014_legacy_contraction_control.py`
- Create after sealed post-activation authorization: `alembic/versions/20260713_0015_legacy_contract_ddl.py`
- Create: `src/maintenance/legacy_cleanup.py`
- Create: `tests/integration/maintenance/test_legacy_cleanup.py`
- Create: `tests/architecture/test_no_legacy_side_effects.py`
- Create: `docs/runbooks/legacy-contraction.md`
- Modify: `src/db/migrate.py`
- Modify: `src/utils/db_async.py`
- Modify: `src/scheduler/polling.py`
- Modify: `src/nodes/sender.py`
- Modify: `src/utils/lark_app.py`
- Modify: `src/exchange_service.py`
- Modify: `src/db/access_contract.py`
- Modify: `src/db/bootstrap.py`
- Modify: `src/db/schema.py`
- Modify: `src/db/schema_contract.py`
- Modify: `src/maintenance/checkpoint_repository.py`
- Modify: `tests/unit/test_database_revision.py`
- Modify: `tests/unit/test_database_schema_contract.py`
- Modify: `tests/unit/test_checkpoint_repository_safety.py`
- Modify: `tests/unit/test_alembic_offline.py`
- Modify: `tests/integration/ingestion/test_schema.py`
- Modify: `tests/integration/ingestion/test_access_roles.py`
- Create: `tests/integration/migrations/test_0013_to_0014.py`
- Create after sealed post-activation authorization: `tests/integration/migrations/test_0014_to_0015.py`

**Interfaces:**
- Consumes: current DB authority, real v2 proof, consumed Phase-3 activation barrier/receipt, verified backup, retention planner, per-generation high-water reconciliation and target-generation production stability record
- Produces: non-destructive contraction plans and sealed DDL authorization; removal gate for a later migration-role-only contraction revision

Migration revision is exactly `20260713_0014` with linear `down_revision = "20260713_0013"`.
The pre-activation implementation head stops at `0014`. The separately reviewed post-activation contraction revision is exactly `20260713_0015` with linear `down_revision = "20260713_0014"`; it is created/applied only after the authorization and hard production window below.

- [ ] **Step 1: Write cleanup eligibility and architecture tests**

```python
import inspect

import pytest


@pytest.mark.asyncio
async def test_cleanup_refuses_unresolved_or_unmigrated_rows(cleaner, db, verified_snapshot):
    await db.seed_email(state="waiting_approval")
    await db.seed_email(state="accepted")
    await db.seed_email(state="send_unknown")
    plan = await cleaner.plan(snapshot_id=verified_snapshot.id)
    assert plan.eligible_count == 0
    assert plan.blocked_by_state == {"waiting_approval": 1, "accepted": 1, "send_unknown": 1}


def test_legacy_modules_have_no_unguarded_external_side_effects():
    for module in (sender, polling, exchange_service):
        findings = find_external_calls_without_legacy_effect_guard(
            inspect.getsource(module)
        )
        assert findings == []


@pytest.mark.integration
@pytest.mark.parametrize(
    "missing",
    ["durable_authority", "real_v2_proof", "consumed_barrier", "activation_receipt", "production_window"],
)
async def test_contract_refuses_every_missing_production_gate(
    cleaner, ready_contraction_plan, missing, db
):
    plan = ready_contraction_plan.without(missing)
    with pytest.raises(LegacyContractionBlocked, match=missing):
        await cleaner.authorize(
            plan.id,
            actor="operator",
            reason="contract",
            idempotency_key=f"contract-missing-{missing}",
        )
    assert await db.legacy_table_is_writable() is True


@pytest.mark.integration
async def test_zero_or_short_stability_window_can_never_authorize_contract(cleaner):
    plan = await cleaner.ready_plan(window=timedelta(0), event_count=100_000)
    with pytest.raises(LegacyContractionBlocked, match="minimum_production_window"):
        await cleaner.authorize(plan.id, actor="operator", reason="contract", idempotency_key="short")


@pytest.mark.integration
async def test_build_or_contract_drift_resets_stability_window(cleaner, ready_contraction_plan):
    await cleaner.observe(ready_contraction_plan.id, days=7, events=1_000)
    await cleaner.rotate_build_for_test()
    status = await cleaner.status(ready_contraction_plan.id)
    assert status.observed_seconds == status.observed_events == 0
    assert status.authorization_id is None
```

- [ ] **Step 2: Add non-destructive contraction metadata and seal migration authorization**

Migration `20260713_0014` is safe on every deployment: it creates `legacy_contraction_plans`, immutable evidence members, write-observation counters, sealed authorizations and completion-evidence tables under nonconflicting names; it never renames/drops a live table, creates a replacement view or changes legacy privileges. It also performs the complete revision-contract update: exact single head/schema digest, bootstrap checks, four ACL manifests, checkpoint allowlist and offline SQL. Maintenance may plan/observe/seal but has no DDL or GRANT/REVOKE privilege; auditor is SELECT-only; migration owns DDL. `tests/integration/migrations/test_0013_to_0014.py` proves a disabled-contraction code-first real-PostgreSQL bridge, preservation/roles/startup, second no-op, old-head rejection and single-head empty DB.

Implementation mode may create plans/dry-runs only. `legacy_cleanup authorize --plan-id --snapshot-id --actor --reason --idempotency-key` atomically re-reads and requires: current authority `durable_active`; a real build-matching v2 proof; consumed `production_ready` barrier and `pipeline.switch` receipt; exact target generation/fence; completed reconciliation; **at least seven continuous production days and at least 1,000 target-generation events** (configuration may raise but never lower either minimum); zero legacy writes; verified backup; and high-water/count equality. Any build/config/fence/v2/profile/high-water drift resets both observation counters to zero and invalidates prior authorization. The command writes only an immutable authorization/evidence hash, receipt and audit; it performs no `ALTER`, `RENAME`, `CREATE VIEW`, `GRANT` or `REVOKE`.

`20260713_0015_legacy_contract_ddl.py` has exactly two fail-closed branches. The **fresh-install branch** needs no authorization only when one transaction proves there are zero account/authority/ownership/business/legacy rows, zero legacy sequence/high-water/history markers, zero command/switch receipts, zero activation consumptions/audits and zero contraction plans/authorizations; it creates the final schema/view/ACL directly. Any single trace selects the **existing-production branch**, where the migration role takes the account plus relation locks and revalidates the sealed authorization and every bound fact in the same migration transaction before rename/view/revoke/completion evidence. There is no “empty-looking” fallback for a previously used database.

The production authorization binds authority, real v2 proof, consumed successor/live barrier/switch receipt, target generation/fence, reconcile result, seven-day/1,000-event window, zero legacy writes, backup, high-water and build/config/schema. Parameterized tests mutate each fact after sealing and assert the migration fails atomically: original relation remains writable with identical data, and no view, ACL change or completion row exists. A controlled lock race proves either a legacy write/fact drift commits first and the final recheck aborts DDL, or the migration lock wins and the writer/drift is rejected after contraction; no write is lost and no half-schema is visible.

The same `0015` commit advances exact head/schema digest, bootstrap pre/post checks, all four post-contraction ACL manifests, checkpoint allowlist and offline SQL. `tests/integration/migrations/test_0014_to_0015.py` covers baseline-to-`0015` empty install, pristine `0014 -> 0015`, every history trace without authorization, sealed production success, all bound-fact drifts, lock races, data/high-water preservation, read-only role denials, completion evidence, second no-op and old-binary/database-first rejection. `src/db/migrate.py`/`src/utils/db_async.py` never perform runtime DDL and retain only inspection compatibility until final code deletion.

- [ ] **Step 3: Clean historical checkpoints in guarded batches**

Require backup ID and dry-run plan; exclude waiting approval, accepted, send_unknown, unmatched high-water and nonterminal emails. Delete at most 500 checkpoint rows per batch, measure lock wait/query latency/table bytes, pause on thresholds, and vacuum only through an operator-approved Runbook step. Record counts and hashes without state payload.

- [ ] **Step 4: Remove legacy business responsibility**

During implementation acceptance, add authority/effect guards, compatibility adapters, contraction manifests and architecture tests, but retain the legacy-authoritative/Shadow code needed by the currently deployed system. Under `durable_active`, a normal current generation uses `DurableProcessingAdapter` and a rollback current generation uses `DurableLegacyCompatAdapter`; neither may invoke direct effects. `LegacyProcessingAdapter` is restricted to legacy/Shadow or already-stamped old draining work. Do **not** delete old direct sender/poller/Lark/Exchange/Qdrant/migration code while `blocked_external_exchange_sync_contract` is present. After the separate extension v2 release, real Phase-3 activation, hard minimum stability window/event count and the migration-role `0015` contraction, execute a distinct reviewed code-removal commit; thin import adapters may remain only when a deployed client needs the name and can no longer produce direct side effects.

- [ ] **Step 5: Verify and commit**

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/integration/maintenance/test_legacy_cleanup.py tests/architecture/test_no_legacy_side_effects.py tests/integration/migrations/test_0013_to_0014.py tests/integration/ingestion/test_schema.py tests/integration/ingestion/test_access_roles.py -q
.venv/bin/python -m pytest tests/unit/test_database_revision.py tests/unit/test_database_schema_contract.py tests/unit/test_checkpoint_repository_safety.py tests/unit/test_alembic_offline.py -q
.venv/bin/python -m pytest -q
git add alembic/versions/20260713_0014_legacy_contraction_control.py src/maintenance/legacy_cleanup.py src/db/migrate.py src/utils/db_async.py src/scheduler/polling.py src/nodes/sender.py src/utils/lark_app.py src/exchange_service.py src/db/access_contract.py src/db/bootstrap.py src/db/schema.py src/db/schema_contract.py src/maintenance/checkpoint_repository.py tests/integration/maintenance/test_legacy_cleanup.py tests/architecture/test_no_legacy_side_effects.py tests/unit/test_database_revision.py tests/unit/test_database_schema_contract.py tests/unit/test_checkpoint_repository_safety.py tests/unit/test_alembic_offline.py tests/integration/ingestion/test_schema.py tests/integration/ingestion/test_access_roles.py tests/integration/migrations/test_0013_to_0014.py docs/runbooks/legacy-contraction.md
git commit -m "refactor: prepare guarded legacy path contraction"
```

Expected: in implementation mode legacy/Shadow remains functional only through the exact effect guard, Durable mode cannot invoke a legacy side effect, protected/nonterminal history is untouched, and no deletion is claimed before production activation/stability evidence.

- [ ] **Step 6: After real activation and the hard stability gate, ship the separate `0015` contraction commit**

For an existing deployment this step is forbidden during `implementation_complete_external_blocked` and requires the sealed post-stability authorization; only the strictly pristine fresh-install branch may reach `0015` without it. Run the bridge/ACL/offline tests plus the full suite, apply Alembic as the migration role, verify completion evidence and zero writable legacy path, then remove obsolete direct business code in a second code commit whose architecture scan permits no remaining external call from the compatibility names.

```python
@pytest.mark.integration
@pytest.mark.parametrize("start", ["baseline_empty", "0014_pristine"])
async def test_0015_fresh_install_needs_no_contraction_authorization(migrator, start):
    db = await migrator.pristine(start)
    await migrator.upgrade(db, "20260713_0015")
    assert await db.head() == "20260713_0015"
    assert await db.legacy_compatibility_is_read_only() is True


@pytest.mark.integration
@pytest.mark.parametrize(
    "trace",
    ["account", "authority", "business", "legacy_row", "legacy_high_water", "switch_receipt", "activation_consumption", "audit_history"],
)
async def test_any_history_trace_without_sealed_authorization_blocks_atomically(
    migrator, trace
):
    db = await migrator.at_0014_with_trace(trace)
    before = await db.legacy_snapshot()
    with pytest.raises(MigrationAuthorizationRequired):
        await migrator.upgrade(db, "20260713_0015")
    assert await db.legacy_snapshot() == before
    assert await db.no_view_acl_or_completion_partial() is True


@pytest.mark.integration
@pytest.mark.parametrize(
    "drift",
    ["authority", "v2", "consumed_successor_receipt", "generation_fence", "reconcile", "window_events", "legacy_write", "backup", "high_water", "build_config_schema"],
)
async def test_every_sealed_fact_drift_aborts_0015_without_partial_ddl(
    migrator, sealed_production_db, drift
):
    await sealed_production_db.mutate_bound_fact(drift)
    before = await sealed_production_db.legacy_snapshot()
    with pytest.raises(ContractionEvidenceDrift):
        await migrator.upgrade(sealed_production_db, "20260713_0015")
    assert await sealed_production_db.legacy_snapshot() == before
    assert await sealed_production_db.no_view_acl_or_completion_partial() is True


@pytest.mark.integration
@pytest.mark.parametrize("racer", ["legacy_write", "bound_fact_drift"])
async def test_0015_lock_race_has_one_serialized_winner(migrator, sealed_production_db, racer):
    migrated, raced = await migrator.race_upgrade_with(sealed_production_db, racer)
    assert (migrated, raced) in {(False, True), (True, False)}
    assert await sealed_production_db.no_lost_write_or_partial_schema() is True
```

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/integration/migrations/test_0014_to_0015.py tests/integration/maintenance/test_legacy_cleanup.py tests/integration/ingestion/test_access_roles.py tests/architecture/test_no_legacy_side_effects.py -q
.venv/bin/python -m pytest tests/unit/test_database_revision.py tests/unit/test_database_schema_contract.py tests/unit/test_checkpoint_repository_safety.py tests/unit/test_alembic_offline.py -q
.venv/bin/python -m pytest -q
git add alembic/versions/20260713_0015_legacy_contract_ddl.py src/db/access_contract.py src/db/bootstrap.py src/db/schema.py src/db/schema_contract.py src/maintenance/checkpoint_repository.py tests/integration/migrations/test_0014_to_0015.py tests/integration/ingestion/test_schema.py tests/integration/ingestion/test_access_roles.py tests/unit/test_database_revision.py tests/unit/test_database_schema_contract.py tests/unit/test_checkpoint_repository_safety.py tests/unit/test_alembic_offline.py
git commit -m "refactor: contract legacy schema after production proof"
```

Expected: production final head is exactly `0015`; the old name is read-only, completion evidence matches the sealed authorization, no runtime/maintenance role has DDL, and only after this succeeds may the separate code-removal commit claim legacy contraction complete.

---

### Task 10: Run the Soak Test and Produce Implementation/Production Acceptance Reports

**Files:**
- Create: `scripts/run_soak.py`
- Create: `scripts/final_acceptance.py`
- Create: `tests/acceptance/test_design_acceptance.py`
- Create: `tests/acceptance/test_activation_successors.py`
- Create: `docs/superpowers/reports/2026-07-10-implementation-acceptance.md`
- Create after real activation: `docs/superpowers/reports/2026-07-10-production-activation-acceptance.md`
- Create after `0015` contraction: `docs/superpowers/reports/2026-07-10-production-contraction-acceptance.md`
- Create: `docs/superpowers/reports/2026-07-10-exchange-server-follow-up-boundary.md`
- Modify: `src/ingestion/cutover_barrier.py`
- Modify: `README.md`
- Modify: `CLAUDE.md`

**Interfaces:**
- Consumes: CI artifacts, migration/backup/cutover evidence, alerts, metrics, coverage and load results
- Produces: machine-checkable mapping for all 21 design criteria; `implementation_complete_external_blocked`, `production_activated`, or final `production_contracted`; and a decision on whether the separate server-side design may start

- [ ] **Step 1: Implement a reproducible 24-hour-equivalent soak**

`run_soak.py --seed 20260710 --virtual-hours 24 --messages 10000` drives deterministic bursts, duplicates, reordered changes, Sync gaps, database reconnects, model/Lark/Exchange failures, large bodies and maximum allowed attachments through local fake external adapters and real PostgreSQL/Qdrant. Every simulated hour samples RSS, event-loop p99, task/client counts, Inbox/Outbox age, checkpoint/table/content bytes and GC results.

- [ ] **Step 2: Define quantitative soak acceptance**

Require zero lost Inbox/Outbox, zero duplicate business side effect, zero automatic retry for send_unknown, no old fencing commit, zero security sentinel leak, maximum queue recovery below 5 minutes after fault removal, final steady RSS below 80% of 2 GiB, RSS trend below 1 MiB per 100 messages over the final half, post-attachment RSS recovery within 15%, zero leaked tasks/connections and checkpoint count bounded by active/waiting workflows plus the 24-hour retention window.

- [ ] **Step 3: Map every design criterion to evidence**

`final_acceptance.py` reads a checked-in manifest with criteria 1-21 and fails when an AI-repository implementation criterion lacks a passing test ID plus CI artifact or operator evidence hash. It has four explicit modes. `implementation` requires pre-activation head `0014`, accepts all dormant/fail-closed AI work, records the known incompatibility and appends immutable `phase6_implementation_complete_external_blocked` with null target/live-barrier fields; it does not quiesce or invalidate anything. `production-ready` runs only after the separate extension release, reserved target and ready live cutover barrier, revalidates the full predecessor chain/current `0014` head/four business Outboxes/Graph/security/operations plus all live-cutover evidence and appends a complete non-superseded `production_ready` successor. `activation` runs only after Phase-3 consumes that exact successor and requires controlled FolderScope apply, real cutover/reconciliation IDs and observed active authority; it writes the production-activation report and may mark `production_activated`, but not contraction complete. Final `production` additionally requires the hard seven-day/1,000-event window, sealed authorization, successful `0015` migration/completion evidence, read-only legacy view and code-removal architecture gate; only it writes the contraction report and marks `production_contracted`. Reports contain no body, address, token, card or internal credential. Tests reject base/P4/P5/P6-implementation/current-extension/mock leaves and accept only a real-v2 `production_ready` leaf bound to a ready live barrier.

- [ ] **Step 4: Run the final gate**

```bash
uv run ruff format --check src tests
uv run ruff check src tests
uv run pyright src
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test uv run pytest -q
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test uv run pytest --cov=src --cov-report=json:artifacts/coverage.json --cov-fail-under=80 -q
uv run python scripts/check_coverage_ratchet.py artifacts/coverage.json .coverage-baseline.json
uv run python scripts/run_soak.py --seed 20260710 --virtual-hours 24 --messages 10000 --output artifacts/soak.json
uv run python scripts/final_acceptance.py --mode implementation --evidence artifacts --output docs/superpowers/reports/2026-07-10-implementation-acceptance.md
docker compose config --quiet
docker build --target runtime -t ai-exchange:final .
git diff --check
```

Expected: every AI implementation command exits 0, coverage is at least 80% globally/90% on critical modules, soak thresholds pass, and all 21 implementation criteria have evidence. With the current extension the report exits successfully only in implementation mode, records `implementation_complete_external_blocked`, appends the non-consumable Phase-6 successor and leaves production-ready/activation/production modes fail-closed.

- [ ] **Step 5: Record the service-repository boundary and commit**

The follow-up boundary report repeats only verified server gaps: synchronous reply/forward without idempotency keys or operation query, ordinary send accepted-only semantics, Sync generator auto-pagination that defeats the HTTP `limit`, missing continuation/`includes_last` v2 contract, raw `read_flag_change` with `item=None`, and the lack of an authenticated header/nonce freshness contract. It records the exact timestamp distinction: the JSON body timestamp is already inside the HMAC-signed payload and remains trusted source-event data, while `X-Webhook-Timestamp` is separately generated and cannot authorize freshness. A future version may sign an explicit timestamp/body/nonce tuple. The report states the server repository remained unmodified and that a separate brainstorming/design cycle is required. Reaching `implementation_complete_external_blocked` authorizes that later design task. After its release the exact order is: real v2 probe; `production-ready`; Phase-3 switch; controlled per-folder apply/reconciliation; `activation` acceptance at head `0014`; hard stability window and sealed contraction authorization; reviewed `0015` migration/code removal; final `production` acceptance. No pre-switch report may claim `production_activated`, and no head-`0014` report may claim `production_contracted`.

```bash
git add scripts/run_soak.py scripts/final_acceptance.py src/ingestion/cutover_barrier.py tests/acceptance/test_design_acceptance.py tests/acceptance/test_activation_successors.py docs/superpowers/reports/2026-07-10-implementation-acceptance.md docs/superpowers/reports/2026-07-10-exchange-server-follow-up-boundary.md README.md CLAUDE.md
git commit -m "docs: certify AI-Exchange implementation acceptance"
```

Expected: the report says `implementation_complete_external_blocked` only when all AI evidence is green and the external blocker is exact; that outcome authorizes the later Exchange server design task but not production activation. A subsequent real v2 proof plus Phase-3 switch may produce `production_activated` at head `0014`; only the hard stability gate plus successful `0015` contraction may produce `production_contracted`.
