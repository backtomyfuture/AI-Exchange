# AI-Exchange Phase 2 Durable Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 Webhook 和 5 分钟增量同步统一写入 PostgreSQL Inbox，通过固定 Worker、租约、死信和 generation/fencing 实现可恢复的收件链路。

**Architecture:** 新建独立 `src/ingestion/` 边界。Webhook 仅验证并持久化后返回 202；Sync 通过账户/文件夹 advisory lock 拉取批次，并在同一事务写 Inbox 与游标。切换期间 Durable Worker 可调用旧处理器兼容适配器，但绝不回退到进程内队列。

**Tech Stack:** Python 3.12、FastAPI、psycopg 3、Alembic、PostgreSQL `SKIP LOCKED`/advisory lock、httpx、pytest。

## Global Constraints

- Phase 1 Alembic baseline、typed DB error、`ProcessingOutcome` 和最小 ContentStore 必须已经合入。
- `SYNC_INTERVAL_SECONDS=300`，默认 `SYNC_BATCH_SIZE=500`。
- 当前服务端只明确返回 create/update/delete；`update.item.is_read` 作为读状态投影，不能假设已存在独立 read 事件。
- 远程 HTTP 调用不得位于数据库事务中。
- 游标只有在本批 Inbox 事件与新游标同事务提交成功后才能推进。
- 游标失效必须进入 `reset_required`；禁止自动传 `None` 冷启动。
- Shadow 模式绝不执行模型、飞书、Exchange 或 Qdrant 副作用。
- 回滚仍由 Durable Inbox 排空已返回 202 的数据，不能退回当前内存队列。
- 所有 Inbox claim/complete 都比较租约 owner、generation 和 fencing token；任何 Outbox 继承相同所有权规则。

---

## File Map

| Area | Files | Responsibility |
|---|---|---|
| Schema/types | `alembic/versions/20260710_0003_durable_ingestion.py`, `src/ingestion/models.py` | Pipeline ownership, Inbox, cursor, email aggregate, audit and cold-start facts |
| Intake | `src/ingestion/normalization.py`, `src/ingestion/webhook.py` | Canonical Webhook/Sync normalization, dedupe and durable 202 receipt |
| Ownership | `src/ingestion/ownership.py`, `src/ingestion/repository.py` | Generation/fencing, leases, retry/dead-letter and claim/complete CAS |
| State | `src/ingestion/email_events.py` | Sole allowed create/update/read/delete transition table and cancellation decisions |
| Reconciliation | `src/ingestion/sync.py`, `src/ingestion/cold_start.py`, `src/scheduler/sync_reconciliation.py` | Dedicated advisory-lock connection, atomic cursor/Inbox commit and approved reset |
| Execution | `src/ingestion/worker.py`, `src/ingestion/legacy_adapter.py`, `src/ingestion/runtime.py` | Fixed Workers, compatibility processing and owned lifecycle |
| Operations | `src/ingestion/backfill.py`, `src/ingestion/cutover.py`, `scripts/backfill_durable_ingestion.py`, `scripts/manage_pipeline.py` | Dry-run migration and phase-local shadow/activation control |
| Test harness | `tests/integration/ingestion/conftest.py`, `tests/unit/ingestion/conftest.py` | Migrated per-test PostgreSQL schema, deterministic clocks/fakes and fault injector |

### Task 1: Create Durable Ingestion Tables and DTOs

**Files:**
- Create: `alembic/versions/20260710_0003_durable_ingestion.py`
- Create: `src/ingestion/__init__.py`
- Create: `src/ingestion/models.py`
- Create: `tests/integration/ingestion/test_schema.py`
- Create: `tests/integration/ingestion/conftest.py`
- Create: `tests/unit/ingestion/conftest.py`

**Interfaces:**
- Consumes: Phase 1 Alembic head
- Produces: `PipelineGeneration`, `NormalizedIngressEvent`, `SyncChange`, `SyncBatch`, `InboxLease`, `InboxStats`, `IngressReceipt`; named fixtures used by Tasks 1-10

- [ ] **Step 1: Write real PostgreSQL constraint tests**

```python
@pytest.mark.integration
async def test_one_current_ingress_per_account(db):
    await insert_generation(db, account_id=8, generation=1, state="current_ingress")
    with pytest.raises(psycopg.errors.UniqueViolation):
        await insert_generation(db, account_id=8, generation=2, state="current_ingress")


@pytest.mark.integration
async def test_inbox_dedupe_key_is_unique(db):
    await insert_inbox(db, dedupe_key="event-key")
    with pytest.raises(psycopg.errors.UniqueViolation):
        await insert_inbox(db, dedupe_key="event-key")
```

- [ ] **Step 2: Run and confirm missing migration**

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/integration/ingestion/test_schema.py -q
```

- [ ] **Step 3: Add the migration**

Create `pipeline_ownership`, `event_inbox`, `sync_cursors`, `emails`, `audit_events`, `sync_cold_start_plans`, and `pipeline_shadow_comparisons`. The migration must include:

```python
op.execute(
    "CREATE UNIQUE INDEX uq_pipeline_current_ingress "
    "ON pipeline_ownership(account_id) WHERE state='current_ingress'"
)
op.execute(
    "CREATE INDEX ix_event_inbox_claim "
    "ON event_inbox(status, available_at, received_at) "
    "WHERE status IN ('pending','retry_wait')"
)
op.create_unique_constraint("uq_event_inbox_dedupe", "event_inbox", ["dedupe_key"])
op.create_unique_constraint("uq_email_external", "emails", ["account_id", "external_email_id"])
```

`event_inbox` stores `generation` and `fencing_token`; `emails.owner_generation` is separate and sticky.

Use these exact columns in addition to IDs/timestamps: `pipeline_ownership(account_id, generation, pipeline_name, state, fencing_token, created_by, reason)`; `event_inbox(account_id, external_email_id, folder, source, raw_event_type, change_kind, dedupe_key, payload, execution_mode, pipeline_name, generation, fencing_token, status, lease_owner, lease_until, attempts, available_at, safe_error_code, safe_error_summary, received_at, updated_at)`; `sync_cursors(account_id, folder, cursor, status, last_success_at, last_attempt_at, version)`; `emails(account_id, external_email_id, status, version, owner_generation, create_seen_at, source_deleted_at, external_effects_started_at, content_ref, is_read, updated_at)`; audit/cold-start/shadow tables with actor/reason, stable hashes and no raw body beyond the governed Inbox payload.

Lock the migration types/constraints as follows: IDs are UUID; account/generation/fencing/version/attempts are BIGINT; payload/content reference are JSONB; event/status/mode/pipeline/folder/error fields are TEXT; lease/cursor/event dates are TIMESTAMPTZ. `pipeline_ownership` primary key is `(account_id,generation)`, state checks the four `PipelineGenerationState` values, and a partial unique index allows one `current_ingress`. `event_inbox.dedupe_key CHAR(64) UNIQUE`, attempts defaults 0 with nonnegative check, status checks `pending/retry_wait/leased/completed/dead_letter/manual_review`, and `(account_id,generation)` references ownership. `sync_cursors` primary key is `(account_id,folder)` with status `active/reset_required/cold_start_pending`. `emails` has `UNIQUE(account_id,external_email_id)`, nonnegative version, and owner generation foreign key. Audit rows use UUID, account/email/object IDs, action/result/safe metadata JSONB and TIMESTAMPTZ; cold-start plans have unique plan hash/status/cutoff/actor; shadow comparisons have unique `(inbox_id,pipeline_name)` plus input/decision hashes only.

- [ ] **Step 4: Define immutable DTOs**

```python
class ChangeKind(StrEnum):
    CREATE = "create"
    UPDATE = "update"
    READ = "read"
    DELETE = "delete"


class ExecutionMode(StrEnum):
    ACTIVE = "active"
    SHADOW = "shadow"


@dataclass(frozen=True)
class NormalizedIngressEvent:
    account_id: int
    source: str
    raw_event_type: str
    kind: ChangeKind
    external_email_id: str
    folder: str
    source_version: str | None
    dedupe_key: str
    payload: Mapping[str, Any]


@dataclass(frozen=True)
class PipelineGeneration:
    account_id: int
    generation: int
    pipeline_name: str
    state: PipelineGenerationState
    fencing_token: int


@dataclass(frozen=True)
class SyncChange:
    kind: ChangeKind
    external_email_id: str
    item: Mapping[str, Any] | None
    source_version: str | None = None


@dataclass(frozen=True)
class SyncBatch:
    cursor: str
    changes: Sequence[SyncChange]
    is_full: bool


@dataclass(frozen=True)
class InboxLease:
    id: str
    account_id: int
    generation: int
    fencing_token: int
    lease_owner: str
    attempts: int
    event: NormalizedIngressEvent


@dataclass(frozen=True)
class InboxStats:
    pending: int
    retry_wait: int
    leased: int
    dead_letter: int
    oldest_pending_seconds: float


@dataclass(frozen=True)
class InboxDisposition:
    status: Literal["retry_wait", "dead_letter"]
    attempts: int
    available_at: datetime | None
    safe_error_code: str


@dataclass(frozen=True)
class IngressReceipt:
    inbox_id: str
    duplicate: bool
```

The two `conftest.py` files define `db`, `ownership`, `repo`, `repo_a`, `repo_b`, `inbox_lease`, `leased_event`, `seeded_events`, `coordinator`, `exchange`, `worker`, `inbox`, `fault`, `crash`, `ingress`, `client`, `runtime`, `cutover`, deterministic `EVENT`, raw-body/payload fixtures, HMAC helper and seed methods referenced below. `InjectedFailure(RuntimeError)` and the fault/crash injectors expose only the named booleans used by tests. Integration setup reads `TEST_DATABASE_URL`, creates a unique schema, upgrades Alembic to head, yields the pool and drops the schema in `finally`; a session fixture creates the test database through the maintenance database when it does not exist.

- [ ] **Step 5: Verify and commit**

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/integration/ingestion/test_schema.py -q
git add alembic/versions/20260710_0003_durable_ingestion.py src/ingestion tests/integration/ingestion/conftest.py tests/unit/ingestion/conftest.py tests/integration/ingestion/test_schema.py
git commit -m "feat: add durable ingestion schema and types"
```

---

### Task 2: Normalize Webhook and Sync Events with Stable Dedupe Keys

**Files:**
- Create: `src/ingestion/normalization.py`
- Create: `tests/unit/ingestion/test_normalization.py`
- Modify: `src/domain/errors.py`

**Interfaces:**
- Consumes: `NormalizedIngressEvent`, raw Webhook bytes and `SyncChange`
- Produces: `normalize_webhook_event()`, `normalize_sync_change()`

- [ ] **Step 1: Write deterministic-key tests**

```python
def test_same_webhook_retry_has_same_key(raw_body, payload):
    first = normalize_webhook_event(raw_body=raw_body, payload=payload, header_event="NewMailEvent")
    second = normalize_webhook_event(raw_body=raw_body, payload=payload, header_event="NewMailEvent")
    assert first.dedupe_key == second.dedupe_key


def test_sync_change_key_includes_cursor_and_change_type():
    created = normalize_sync_change(8, "INBOX", "cursor-2", SyncChange(ChangeKind.CREATE, "m1", {}))
    deleted = normalize_sync_change(8, "INBOX", "cursor-2", SyncChange(ChangeKind.DELETE, "m1", None))
    assert created.dedupe_key != deleted.dedupe_key
```

- [ ] **Step 2: Implement canonical hashing**

Use `sha256` over UTF-8 JSON with `sort_keys=True`, compact separators, account/folder/change kind/external ID/source version. Webhook falls back to the raw-body hash only when stable fields are absent. Define `IngressValidationError` with `ErrorKind.VALIDATION` and raise it for missing account/external ID or unsupported change type; never overload `InputLimitExceeded` for schema validation.

- [ ] **Step 3: Verify and commit**

```bash
.venv/bin/python -m pytest tests/unit/ingestion/test_normalization.py -q
git add src/ingestion/normalization.py src/domain/errors.py tests/unit/ingestion/test_normalization.py
git commit -m "feat: normalize and deduplicate ingestion events"
```

---

### Task 3: Implement Pipeline Ownership and Fencing

**Files:**
- Create: `src/ingestion/ownership.py`
- Create: `tests/unit/ingestion/test_ownership.py`
- Create: `tests/integration/ingestion/test_pipeline_fencing.py`
- Modify: `src/domain/errors.py`

**Interfaces:**
- Produces: `StaleFence(ErrorKind.INTERNAL_INVARIANT)`; `bootstrap(account_id, pipeline_name)`, `get(account_id, generation)`, `current_ingress(account_id)`, `assert_fence(account_id, generation, fencing_token)`, `can_execute(lease)`, `quiesce()`, `switch()`, `retire()`

- [ ] **Step 1: Write current/draining/retired tests**

```python
@pytest.mark.integration
async def test_switch_creates_new_current_and_drains_old(ownership):
    old = await ownership.bootstrap(8, "legacy_compat")
    new = await ownership.switch(
        account_id=8,
        expected_generation=old.generation,
        expected_fencing_token=old.fencing_token,
        target_pipeline="durable_v1",
        actor="test",
        reason="cutover",
    )
    assert new.generation == old.generation + 1
    assert (await ownership.get(8, old.generation)).state is PipelineGenerationState.DRAINING
    assert (await ownership.current_ingress(8)).generation == new.generation


@pytest.mark.integration
async def test_retired_fence_cannot_complete(ownership, inbox_lease):
    await ownership.retire(8, inbox_lease.generation, inbox_lease.fencing_token, "test", "drained")
    assert await ownership.can_execute(inbox_lease) is False
```

- [ ] **Step 2: Implement transactional generation changes**

Lock the current row with `FOR UPDATE`; compare expected generation/token. `quiesce()` moves current to `quiescing` and forbids new claims while existing leases finish. `switch()` requires no unresolved `sending`, moves the old row to `draining`, inserts the new `current_ingress` with generation/token incremented and appends audit in the same transaction. Draining rows may execute only tasks already stamped with their generation; `retire()` requires zero owned Inbox/Outbox and matching high water. `assert_fence()` is used by both claim and completion.

- [ ] **Step 3: Verify and commit**

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/unit/ingestion/test_ownership.py tests/integration/ingestion/test_pipeline_fencing.py -q
git add src/ingestion/ownership.py src/domain/errors.py tests/unit/ingestion/test_ownership.py tests/integration/ingestion/test_pipeline_fencing.py
git commit -m "feat: add pipeline generations and fencing"
```

---

### Task 4: Implement Inbox Repository, Leases, Retries, and Dead Letters

**Files:**
- Create: `src/ingestion/repository.py`
- Create: `tests/integration/ingestion/test_inbox_repository.py`

**Interfaces:**
- Consumes: ownership rows and `NormalizedIngressEvent`
- Produces: `insert(event, generation, fencing_token, mode) -> IngressReceipt`; `claim_batch(worker_id, pipeline_names, limit, lease_seconds) -> list[InboxLease]`; `complete(lease, outcome) -> bool`; `fail(lease, error) -> InboxDisposition`; `stats() -> InboxStats`

- [ ] **Step 1: Write concurrent claim tests**

```python
@pytest.mark.integration
async def test_skip_locked_claims_are_disjoint(repo_a, repo_b, seeded_events):
    first, second = await asyncio.gather(
        repo_a.claim_batch(worker_id="a", pipeline_names=frozenset({"durable_v1"}), limit=10, lease_seconds=60),
        repo_b.claim_batch(worker_id="b", pipeline_names=frozenset({"durable_v1"}), limit=10, lease_seconds=60),
    )
    assert {item.id for item in first}.isdisjoint({item.id for item in second})


@pytest.mark.integration
async def test_complete_requires_matching_fence(repo, leased_event):
    stale = replace(leased_event, fencing_token=leased_event.fencing_token - 1)
    assert await repo.complete(stale) is False


@pytest.mark.integration
async def test_retry_and_dead_letter_are_bounded(repo, leased_event):
    disposition = await repo.fail(leased_event, TransientDependencyError("temporary"))
    assert disposition.status == "retry_wait"
    assert disposition.available_at > leased_event.event_received_at
    fifth = await repo.seed_lease(attempts=5)
    disposition = await repo.fail(fifth, TransientDependencyError("still temporary"))
    assert disposition.status == "dead_letter"
```

- [ ] **Step 2: Implement claim SQL**

Use this single-statement claim inside one transaction; pass pipeline names, limit, worker ID and lease seconds as parameters:

```sql
WITH claimable AS (
    SELECT e.id
    FROM event_inbox AS e
    JOIN pipeline_ownership AS p
      ON p.account_id = e.account_id
     AND p.generation = e.generation
     AND p.fencing_token = e.fencing_token
    WHERE e.status IN ('pending', 'retry_wait')
      AND e.available_at <= now()
      AND e.execution_mode = 'active'
      AND e.pipeline_name = ANY(%s)
      AND p.state IN ('current_ingress', 'draining')
    ORDER BY e.received_at, e.id
    FOR UPDATE OF e SKIP LOCKED
    LIMIT %s
)
UPDATE event_inbox AS e
SET status = 'leased',
    lease_owner = %s,
    lease_until = now() + make_interval(secs => %s),
    updated_at = now()
FROM claimable AS c
WHERE e.id = c.id
RETURNING e.*
```

`complete` and `fail` update only where ID, `lease_owner`, generation and fencing token match and the ownership row still permits that exact generation. Shadow rows are never claimable.

- [ ] **Step 3: Implement error disposition**

Retryable errors increment attempts and set `retry_wait` with exponential delay capped at 15 minutes. Authentication and invariant errors dead-letter immediately. Exceeding five attempts sets `dead_letter`, stores only safe code/summary, and appends audit.

- [ ] **Step 4: Verify and commit**

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/integration/ingestion/test_inbox_repository.py -q
git add src/ingestion/repository.py tests/integration/ingestion/test_inbox_repository.py
git commit -m "feat: add leased durable inbox repository"
```

---

### Task 5: Apply create/update/read/delete without Reopening Terminal Mail

**Files:**
- Create: `src/ingestion/email_events.py`
- Create: `tests/unit/ingestion/test_email_events.py`
- Create: `tests/architecture/test_email_state_repository_boundary.py`
- Modify: `src/ingestion/repository.py`

**Interfaces:**
- Produces: immutable `EmailEventDecision(should_process, should_cancel, new_status, cancel_pending_side_effects, reason)` and the sole repository CAS for email status

- [ ] **Step 1: Write ordered and out-of-order tests**

```python
@pytest.mark.parametrize(
    ("current", "event", "expected", "should_process"),
    [
        (None, "create", "ingested", True),
        ("ingested", "create", "ingested", False),
        ("sent", "update", "sent", False),
        ("cancelled", "create", "cancelled", False),
        ("waiting_approval", "delete", "cancelled", False),
        ("processing", "read", "processing", False),
        ("accepted", "delete", "accepted", False),
        ("delivery_failed", "create", "delivery_failed", False),
        ("rejected", "update", "rejected", False),
        ("expired", "create", "expired", False),
    ],
)
def test_event_transition(current, event, expected, should_process):
    decision = decide_email_event(current_status=current, kind=ChangeKind(event), source_is_read=True)
    assert decision.new_status == expected
    assert decision.should_process is should_process
```

- [ ] **Step 2: Implement pure transition function and repository CAS**

Define the complete allowed-transition table for `ingested`, `processing`, `manual_review`, `waiting_approval`, `send_queued`, `sending`, `accepted`, `sent`, `delivery_failed`, `send_unknown`, `no_action`, `rejected`, `expired`, `cancelled` and `dead_letter`. Only first create sets `create_seen_at` and triggers processing. Update may refresh metadata only before external effects start. Read/update-is-read only changes projection. Delete before `request_started_at` atomically cancels pending approval and unstarted Send/Notification/Mailbox Outboxes and invalidates cards; once a send attempt started, delete records `source_deleted_at` but cannot roll back or erase its result. `manual_review`/`dead_letter` recovery requires an authenticated administrator reason and creates audit. Architecture test scans nodes/handlers and fails on direct `UPDATE emails SET status`; all mutations call the repository CAS.

- [ ] **Step 3: Verify and commit**

```bash
.venv/bin/python -m pytest tests/unit/ingestion/test_email_events.py -q
git add src/ingestion/email_events.py src/ingestion/repository.py tests/unit/ingestion/test_email_events.py tests/architecture/test_email_state_repository_boundary.py
git commit -m "feat: enforce monotonic email event transitions"
```

---

### Task 6: Add the Typed Exchange Sync Client

**Files:**
- Create: `tests/unit/ingestion/test_exchange_sync_client.py`
- Create: `tests/contracts/test_exchange_sync_contract.py`
- Modify: `src/utils/exchange_api.py`
- Modify: `src/domain/errors.py`
- Modify: `tests/unit/test_exchange_api.py`

**Interfaces:**
- Produces: `ExchangeClient.sync_emails(account_id, folder, sync_state, limit) -> SyncBatch`; `validate_sync_permission(account_id, folder) -> None`; `SyncAuthorizationError`, `SyncCursorInvalidError`, `SyncTransientError`, `SyncContractError`

- [ ] **Step 1: Replace the obsolete “sync removed” assertion**

```python
@pytest.mark.asyncio
async def test_sync_client_maps_server_contract(client):
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/sync"
        assert json.loads(request.content) == {
            "account_id": 8,
            "folder": "INBOX",
            "sync_state": "cursor-1",
            "limit": 500,
            "only_fields": ["id", "subject", "sender", "received_at", "is_read", "has_attachments"],
        }
        return httpx.Response(
            200,
            json={"data": {"sync_state": "cursor-2", "items": [{"change_type": "create", "id": "m1", "item": {"subject": "s"}}]}},
        )
    client.replace_transport_for_test(httpx.MockTransport(handler))
    batch = await client.sync_emails(account_id=8, folder="INBOX", sync_state="cursor-1", limit=500)
    assert batch.cursor == "cursor-2"
    assert batch.changes[0].kind is ChangeKind.CREATE
    assert batch.is_full is False
```

- [ ] **Step 2: Implement response and error classification**

POST to `{api_url}/sync` with exactly `account_id`, `folder`, `sync_state`, `limit`, and the six `only_fields` asserted above. Map 403 to `SyncAuthorizationError`, 400 containing `Invalid sync_state` to `SyncCursorInvalidError`, 429/502/503/504 to `SyncTransientError`, and malformed 2xx to `SyncContractError`. A permission probe uses `limit=1` and never writes the local cursor.

- [ ] **Step 3: Verify and commit**

```bash
.venv/bin/python -m pytest tests/unit/ingestion/test_exchange_sync_client.py tests/contracts/test_exchange_sync_contract.py tests/unit/test_exchange_api.py -q
git add src/utils/exchange_api.py src/domain/errors.py tests/unit/ingestion/test_exchange_sync_client.py tests/contracts/test_exchange_sync_contract.py tests/unit/test_exchange_api.py
git commit -m "feat: add typed Exchange sync client"
```

---

### Task 7: Implement Atomic Sync Coordinator and Cold-start Approval

**Files:**
- Create: `src/ingestion/sync.py`
- Create: `src/ingestion/cold_start.py`
- Create: `tests/unit/ingestion/test_sync_coordinator.py`
- Create: `tests/integration/ingestion/test_sync_atomicity.py`
- Create: `tests/unit/ingestion/test_cold_start.py`

**Interfaces:**
- Produces: `SyncCoordinator.run_folder()`, `ColdStartService.preview()` and `approve()`

- [ ] **Step 1: Write rollback and cursor-stall tests**

```python
@pytest.mark.integration
async def test_cursor_and_events_commit_together(coordinator, db, fault):
    fault.raise_after_inbox_insert = True
    with pytest.raises(InjectedFailure):
        await coordinator.run_folder(8, "INBOX")
    assert await db.cursor_value(8, "INBOX") == "cursor-1"
    assert await db.inbox_count() == 0


@pytest.mark.asyncio
async def test_invalid_cursor_requires_manual_cold_start(coordinator, exchange):
    exchange.sync_emails.side_effect = SyncCursorInvalidError("invalid")
    result = await coordinator.run_folder(8, "INBOX")
    assert result.status == "reset_required"
    assert exchange.sync_emails.await_count == 1


@pytest.mark.asyncio
async def test_nonempty_batch_with_unchanged_cursor_is_invariant_error(coordinator, exchange):
    exchange.sync_emails.return_value = SyncBatch(
        cursor="cursor-1",
        changes=(SyncChange(ChangeKind.CREATE, "m1", {"id": "m1"}),),
        is_full=False,
    )
    with pytest.raises(InternalInvariantError, match="cursor did not advance"):
        await coordinator.run_folder(8, "INBOX")


@pytest.mark.asyncio
async def test_advisory_lock_releases_on_cancellation(coordinator, lock_probe):
    task = asyncio.create_task(coordinator.run_folder(8, "INBOX"))
    await coordinator.exchange_call_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await lock_probe.can_acquire(8, "INBOX") is True
```

- [ ] **Step 2: Implement lock/fetch/commit loop**

Derive two stable signed int32 lock keys from SHA-256 of `account_id + NUL + normalized folder`; never use Python `hash()`. Check out one dedicated pool connection for the entire run and acquire session `pg_try_advisory_lock(key1, key2)`. End database transactions before each Exchange HTTP call but keep that connection checked out. Each short transaction locks/compares the old cursor, inserts normalized Inbox rows and advances the cursor atomically. In `finally`, execute `pg_advisory_unlock(key1, key2)` on the same connection before returning it to the pool; connection/cancellation tests prove release. Because the server has no `has_more`, every non-empty batch continues and only an empty batch ends; non-empty unchanged cursor raises `InternalInvariantError`.

- [ ] **Step 3: Implement cold-start preview**

Preview walks from `sync_state=None` into a separate plan record, collects counts and redacted samples, and never changes production cursor or Inbox. Because the server has no time-range parameter, the client records all returned changes but marks events older than `COLD_START_PROCESS_AFTER` as `historical_suppressed`, so they cannot create approval/model/side effects. `approve(plan_id, actor)` writes authorization audit and starts a new official run from `None` using this local suppression boundary; counts and cutoff are shown before approval.

- [ ] **Step 4: Verify and commit**

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/unit/ingestion/test_sync_coordinator.py tests/integration/ingestion/test_sync_atomicity.py tests/unit/ingestion/test_cold_start.py -q
git add src/ingestion/sync.py src/ingestion/cold_start.py tests/unit/ingestion/test_sync_coordinator.py tests/integration/ingestion/test_sync_atomicity.py tests/unit/ingestion/test_cold_start.py
git commit -m "feat: add atomic sync reconciliation and cold start"
```

---

### Task 8: Add Fixed Durable Worker and Legacy Processor Adapter

**Files:**
- Create: `src/ingestion/legacy_adapter.py`
- Create: `src/ingestion/worker.py`
- Create: `tests/unit/ingestion/test_worker.py`
- Create: `tests/integration/ingestion/test_webhook_crash_recovery.py`

**Interfaces:**
- Consumes: `ProcessingOutcome`, Inbox leases, Phase 1 ContentStore
- Produces: `DurableInboxWorker.start()`, `run_once()`, `stop(grace_seconds)`

- [ ] **Step 1: Write fixed-count and recovery tests**

```python
@pytest.mark.asyncio
async def test_worker_has_fixed_consumer_count(worker):
    await worker.start()
    assert len(worker.tasks) == worker.concurrency
    await worker.stop(grace_seconds=1.0)


@pytest.mark.integration
async def test_crash_before_effect_is_retryable(worker, inbox, crash):
    event = await inbox.seed_pending()
    crash.before_external_effect = True
    await worker.run_once("worker-a")
    assert (await inbox.get(event.id)).status == "retry_wait"


@pytest.mark.integration
async def test_crash_after_effect_marker_requires_manual_review(worker, inbox, crash):
    event = await inbox.seed_pending()
    crash.after_external_effect_marker = True
    await worker.run_once("worker-a")
    row = await inbox.get(event.id)
    assert row.status == "manual_review"
    assert row.external_effects_started_at is not None
```

- [ ] **Step 2: Implement compatibility adapter**

The adapter fetches email detail, validates/stores content, applies the event state decision, and calls `process_and_archive_email()` only when `should_process=True`. Before invoking any old external side effect it records `external_effects_started_at`; a crash after that marker becomes `manual_review`, not blind retry.

- [ ] **Step 3: Verify and commit**

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/unit/ingestion/test_worker.py tests/integration/ingestion/test_webhook_crash_recovery.py -q
git add src/ingestion/legacy_adapter.py src/ingestion/worker.py tests/unit/ingestion/test_worker.py tests/integration/ingestion/test_webhook_crash_recovery.py
git commit -m "feat: add recoverable durable inbox workers"
```

---

### Task 9: Switch Webhook to 202 Durable Intake and Add Shadow Mode

**Files:**
- Create: `src/ingestion/webhook.py`
- Modify: `src/server.py:33-180`
- Modify: `tests/unit/test_exchange_webhook.py`
- Modify: `tests/unit/test_event_routing.py`

**Interfaces:**
- Consumes: `IngressService.accept(*, raw_body: bytes, payload: Mapping[str, Any], header_event: str | None) -> IngressReceipt`
- Produces: injected `IngressService`; 202 after durable commit, 503 on storage failure, idempotent duplicate 202

- [ ] **Step 1: Write request-boundary tests**

```python
def test_webhook_returns_202_only_after_commit(client, ingress):
    ingress.accept.return_value = IngressReceipt(inbox_id="in-1", duplicate=False)
    response = post_signed_webhook(client, EVENT)
    assert response.status_code == 202
    ingress.accept.assert_awaited_once()


def test_webhook_storage_failure_returns_503(client, ingress):
    ingress.accept.side_effect = DatabaseOperationError(
        operation="insert_inbox", retryable=True, message="database unavailable"
    )
    assert post_signed_webhook(client, EVENT).status_code == 503
```

- [ ] **Step 2: Implement Active and Shadow modes**

Active writes executable Inbox. Shadow writes `execution_mode='shadow'`, records comparison hashes, then allows the old path to remain authoritative. Neither mode fetches email detail or runs a model in the HTTP request.

- [ ] **Step 3: Verify and commit**

```bash
.venv/bin/python -m pytest tests/unit/test_exchange_webhook.py tests/unit/test_event_routing.py -q
git add src/ingestion/webhook.py src/server.py tests/unit/test_exchange_webhook.py tests/unit/test_event_routing.py
git commit -m "feat: accept Exchange webhooks into durable inbox"
```

---

### Task 10: Wire Runtime, 5-minute Scheduler, Backfill, and Cutover Commands

**Files:**
- Create: `src/ingestion/runtime.py`
- Create: `src/ingestion/backfill.py`
- Create: `src/ingestion/cutover.py`
- Create: `src/scheduler/sync_reconciliation.py`
- Create: `scripts/backfill_durable_ingestion.py`
- Create: `scripts/manage_pipeline.py`
- Create: `tests/unit/ingestion/test_runtime.py`
- Create: `tests/unit/ingestion/test_cutover.py`
- Create: `tests/unit/ingestion/test_backfill.py`
- Modify: `src/config.py`
- Modify: `src/init_app.py`
- Modify: `src/main.py`
- Modify: `src/commands/handlers.py`
- Modify: `src/observability/metrics.py`
- Modify: `.env.example`
- Modify: `tests/unit/test_metrics.py`
- Modify: `tests/unit/test_command_router.py`
- Replace: `src/scheduler/polling.py`
- Replace: `tests/unit/test_polling_scheduler.py`

**Interfaces:**
- Produces: one `IngestionRuntime`, DB-backed `/queue` and metrics; `BackfillService.plan/execute`; `CutoverService.quiesce/switch/rollback/retire`; CLI exit codes 0 success, 2 blocked, 3 invariant failure

- [ ] **Step 1: Write runtime and rollback tests**

```python
@pytest.mark.asyncio
async def test_runtime_stops_claiming_before_drain(runtime):
    await runtime.start()
    await runtime.stop(grace_seconds=1.0)
    assert runtime.worker.accepting is False
    assert runtime.worker.inflight_count == 0


@pytest.mark.asyncio
async def test_rollback_keeps_existing_generation_owned(cutover, inbox):
    row = await inbox.seed(generation=2)
    stable = await cutover.rollback(8, "legacy_compat", "operator", "regression")
    assert stable.generation == 3
    assert (await inbox.get(row.id)).generation == 2
```

- [ ] **Step 2: Add exact settings**

Add `DURABLE_INBOX_ENABLED`, `INGESTION_SHADOW_ENABLED`, `SYNC_RECONCILIATION_ENABLED`, `SYNC_INTERVAL_SECONDS=300`, `SYNC_BATCH_SIZE=500`, `INBOX_WORKER_CONCURRENCY=4`, `INBOX_LEASE_SECONDS=120`, `INBOX_MAX_ATTEMPTS=5`, and `INBOX_SHUTDOWN_GRACE_SECONDS=30`.

- [ ] **Step 3: Wire lifecycle and replace old polling**

`AppContext` owns one `IngestionRuntime`. Startup validates Sync permission, starts fixed Worker and the 300-second coordinator. Shutdown stops scheduling, stops claims, waits in-flight, then closes clients. Delete the `/list` direct-processing loop; keep a compatibility module that imports and runs the Sync scheduler for old callers.

- [ ] **Step 4: Implement idempotent backfill and management CLI**

Backfill CLI requires `--account-id`, defaults to `--dry-run`, accepts `--execute --plan-id --batch-size 500`, emits JSON counts/hashes and migrates `emails_log`/`app_kv_store` cursor keys idempotently. Pipeline CLI has explicit `plan`, `quiesce`, `switch`, `rollback`, `drain-status`, and `retire` subcommands with actor/reason/idempotency key. Cutover performs `quiescing -> drain sending -> compare high water -> switch -> observe`; rollback creates a new current generation while previous generations retain ownership and drain.

- [ ] **Step 5: Run Phase 2 gate and commit**

```bash
.venv/bin/python -m pytest tests/unit/ingestion tests/contracts/test_exchange_sync_contract.py -q
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/integration/ingestion -q
.venv/bin/python -m pytest --cov=src.ingestion --cov-report=term-missing --cov-fail-under=90
.venv/bin/ruff check src/ tests/
.venv/bin/python -m pytest -q
git diff --check
git add src/ingestion src/scheduler src/config.py src/init_app.py src/main.py src/commands/handlers.py src/observability/metrics.py .env.example scripts/backfill_durable_ingestion.py scripts/manage_pipeline.py tests/unit/ingestion tests/unit/test_polling_scheduler.py tests/unit/test_metrics.py tests/unit/test_command_router.py
git commit -m "feat: activate durable ingestion and sync reconciliation"
```
