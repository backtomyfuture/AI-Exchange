# AI-Exchange Phase 1 P0 Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在不等待完整架构重构的前提下，立即消除邮件误标已读、数据库错误吞没、近 OOM、百万 Token、无限后台任务、重复审批发送和公开安全暴露。

**Architecture:** 先归档当前 49 个已验证修改，再建立统一错误语义和 Alembic baseline。随后交付最小 AES-GCM ContentStore、轻量 checkpoint、输入硬闸门和固定消费者，并通过临时 CAS 与最小授权为阶段 2–3 提供安全过渡。

**Tech Stack:** Python 3.12、Pydantic 2、psycopg 3、Alembic、LangGraph、cryptography/AES-GCM、FastAPI、httpx、pytest。

## Global Constraints

- 完整约束继承自 `docs/superpowers/plans/2026-07-10-ai-exchange-remediation-master.md`。
- 本阶段默认限制：Webhook 1 MiB、Exchange JSON 响应 64 MiB、正文 10 MiB、附件最多 20 个、单附件 25 MiB、总附件 50 MiB、模型输入最多 131072 Token。
- 超限、数据库不可用、模型异常或 Schema 解析失败统一进入 `manual_review`，不得降级为“无需回复”或标记已读。
- 最小 ContentStore 必须先成功持久化，Graph 才能接收 `content_ref`。
- 阶段 1 发送 CAS 只做止血；正式不可变发送意图和 Outbox 在阶段 3 实施。
- Exchange 服务端仓库保持只读。

---

## File Map

| Area | Files | Responsibility |
|---|---|---|
| Domain | `src/domain/errors.py`, `src/domain/email_state.py` | Shared error kinds, processing outcomes and generation/status vocabulary |
| Database | `alembic/`, `src/db/bootstrap.py`, `src/db/schema.py`, `src/utils/db_async.py` | Forward-only business migrations, checkpoint bootstrap and typed writes/CAS |
| Safety | `src/safety/input_limits.py`, `src/safety/model_budget.py`, `src/safety/http_response.py`, `src/safety/approval_claim.py` | Hard input/Token limits and temporary approval/send claims |
| Content | `src/storage/content_store.py`, `src/storage/encrypted_files.py` | Minimal restart-safe AES-GCM content storage and references |
| Graph | `src/graph/dependencies.py`, `src/graph/state_factory.py`, `src/graph/state.py` | Minimal explicit ContentStore injection and state smaller than 16 KiB |
| Security | `src/security/auth.py`, `src/security/redaction.py`, production settings/Compose | P0 allowlist, endpoint/log minimization, TLS and data-network isolation |
| Maintenance | `src/maintenance/checkpoint_cleanup.py`, `scripts/checkpoint_cleanup.py` | Dry-run plan and backup-gated bounded checkpoint deletion |
| Test infrastructure | `tests/integration/conftest.py` | Create/drop isolated PostgreSQL schemas and run Alembic for all later plans |

### Task 1: Preserve and Commit the Pre-existing Worktree Baseline

**Files:**
- Create: `docs/superpowers/reports/2026-07-10-stage-0-reliability-baseline.md`
- Preserve: the 49 currently modified tracked files reported by `git status --short`

**Interfaces:**
- Consumes: current dirty worktree plus design commit `c62315e`
- Produces: one reviewed baseline commit, a clean worktree suitable for `using-git-worktrees`, and a measurement report

- [ ] **Step 1: Confirm the dirty set has not drifted**

Run:

```bash
git status --short
git diff --name-only
git diff --check
```

Expected: 49 tracked modified files, no untracked business-code files, and no whitespace errors. If the set differs, review the new diff before staging; never discard it.

- [ ] **Step 2: Re-run the recorded baseline**

Run:

```bash
.venv/bin/python -m pytest -q
.venv/bin/python -m pytest --cov=src --cov-report=term-missing
.venv/bin/ruff check src/ tests/
.venv/bin/python -m pip check
uv lock --check
docker compose ps
```

Expected: pytest remains at least 378 passed/12 skipped, Ruff and pip check pass, `uv lock --check` fails only because the lock is stale, and the three runtime containers are healthy.

- [ ] **Step 3: Write the baseline report**

Create the report with these exact sections and current measured values:

```markdown
# Stage 0 Reliability Baseline

- Branch: feat/lark-push-filtering
- Pre-existing tracked modifications: 49 files
- Baseline tests: 378 passed, 12 skipped
- Coverage: 53 percent
- Application RSS: approximately 1.88 GiB of 2 GiB
- PostgreSQL size: approximately 1.99 GiB
- checkpoint_blobs: approximately 1.29 GiB
- checkpoint_writes: approximately 679 MiB
- Known lock state: uv.lock is stale

## Preserved behavioral changes

- Exchange Sent/Drafts Chinese and English folder aliases
- Explicit Lark SDK imports
- Existing formatter and lint cleanup

## Safety rule

No later task may reset or overwrite this baseline commit. Each task stages only its declared paths.
```

- [ ] **Step 4: Commit only the reviewed pre-existing baseline and report**

Run:

```bash
git add -u
git add docs/superpowers/reports/2026-07-10-stage-0-reliability-baseline.md
git diff --cached --check
git commit -m "chore: preserve verified pre-remediation baseline"
```

Expected: one commit containing the 49 existing tracked modifications plus the report; `git status --short` is clean.

---

### Task 2: Introduce Typed Failure and Processing Outcomes

**Files:**
- Create: `src/domain/__init__.py`
- Create: `src/domain/errors.py`
- Create: `src/domain/email_state.py`
- Create: `tests/unit/test_db_write_semantics.py`
- Create: `tests/integration/conftest.py`
- Modify: `src/utils/db_async.py:120-204`
- Modify: `src/exchange_service.py:218-271`
- Modify: `tests/integration/test_service_flow.py`

**Interfaces:**
- Consumes: `AsyncDatabaseManager.get_connection()`
- Produces: master `ErrorKind`, `PipelineGenerationState`; `InitialEmailWriteResult`, `ProcessingOutcome`, `DatabaseOperationError`; `get_email_status(email_id) -> str | None`; `compare_and_set_status(email_id, expected, target) -> bool`; shared migrated PostgreSQL fixture

- [ ] **Step 1: Write failing tests for duplicate versus database failure**

```python
import psycopg
import pytest

from src.domain.email_state import InitialEmailWriteResult, ProcessingOutcome
from src.domain.errors import DatabaseOperationError


@pytest.mark.asyncio
async def test_duplicate_is_typed(db_manager, duplicate_cursor):
    db_manager.get_connection = duplicate_cursor
    result = await db_manager.log_initial_email({"id": "mail-1"})
    assert result is InitialEmailWriteResult.DUPLICATE


@pytest.mark.asyncio
async def test_database_failure_is_not_duplicate(db_manager, failing_connection):
    failing_connection.side_effect = psycopg.OperationalError("database unavailable")
    db_manager.get_connection = failing_connection
    with pytest.raises(DatabaseOperationError) as caught:
        await db_manager.log_initial_email({"id": "mail-2"})
    assert caught.value.operation == "log_initial_email"
    assert caught.value.retryable is True


@pytest.mark.asyncio
async def test_database_failure_never_marks_read(mock_context):
    mock_context.db_manager.log_initial_email.side_effect = DatabaseOperationError(
        operation="log_initial_email", retryable=True, message="database unavailable"
    )
    with pytest.raises(DatabaseOperationError):
        await process_and_archive_email({"id": "mail-3"}, mock_context)
    mock_context.exchange_client.mark_as_read.assert_not_awaited()
```

`tests/unit/test_db_write_semantics.py` defines `FakeCursor`, `FakeConnection` and an `@asynccontextmanager fake_connection()` directly in the file; `duplicate_cursor` returns rowcount 0 and `failing_connection` raises `psycopg.OperationalError` from `__aenter__`. `tests/integration/conftest.py` reads `TEST_POSTGRES_ADMIN_URL`, creates a random test database/schema, runs Alembic, yields a pool and removes it in `finally`.

- [ ] **Step 2: Run tests and confirm the current bool API fails**

Run:

```bash
.venv/bin/python -m pytest tests/unit/test_db_write_semantics.py tests/integration/test_service_flow.py -q
```

Expected: FAIL because the enums and exception do not exist and DB errors currently return `False`.

- [ ] **Step 3: Create the domain types**

```python
# src/domain/errors.py
from enum import StrEnum


class ErrorKind(StrEnum):
    VALIDATION = "validation_error"
    AUTHENTICATION = "authentication_error"
    RATE_LIMITED = "rate_limited"
    TRANSIENT_DEPENDENCY = "transient_dependency_error"
    PERMANENT_DEPENDENCY = "permanent_dependency_error"
    POLICY_REJECTED = "policy_rejected"
    SEND_UNKNOWN = "send_unknown"
    INTERNAL_INVARIANT = "internal_invariant_error"


class DatabaseOperationError(RuntimeError):
    def __init__(self, *, operation: str, retryable: bool, message: str):
        super().__init__(message)
        self.operation = operation
        self.retryable = retryable


class ManualReviewRequired(RuntimeError):
    def __init__(self, *, reason: str, safe_summary: str):
        super().__init__(safe_summary)
        self.reason = reason
        self.safe_summary = safe_summary
```

```python
# src/domain/email_state.py
from enum import StrEnum


class InitialEmailWriteResult(StrEnum):
    CREATED = "created"
    DUPLICATE = "duplicate"


class ProcessingOutcome(StrEnum):
    PROCESSED = "processed"
    DUPLICATE = "duplicate"
    ARCHIVED = "archived"
    MANUAL_REVIEW = "manual_review"


class PipelineGenerationState(StrEnum):
    CURRENT_INGRESS = "current_ingress"
    QUIESCING = "quiescing"
    DRAINING = "draining"
    RETIRED = "retired"


SAFE_DUPLICATE_READ_STATUSES = frozenset({"waiting_approval", "notified_readonly", "skipped", "sent"})
```

- [ ] **Step 4: Replace bool and swallowed-error behavior**

Implement `log_initial_email()` to return `CREATED` or `DUPLICATE` and raise `DatabaseOperationError` on `psycopg.Error`. Add `get_email_status()` and make `update_status()` raise when `rowcount != 1`. Add:

```python
async def compare_and_set_status(
    self,
    email_id: str,
    *,
    expected: frozenset[str],
    target: str,
) -> bool:
    async with self.get_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE emails_log SET status=%s, updated_at=CURRENT_TIMESTAMP "
                "WHERE id=%s AND status=ANY(%s)",
                (target, email_id, list(expected)),
            )
            return cur.rowcount == 1
```

Update `process_and_archive_email()` to return `ProcessingOutcome`. A duplicate may call mark-read only when `get_email_status()` returns a value in `SAFE_DUPLICATE_READ_STATUSES`; a DB exception propagates to the Worker.

- [ ] **Step 5: Run focused and regression tests**

Run:

```bash
.venv/bin/python -m pytest tests/unit/test_db_write_semantics.py tests/unit/test_two_phase_mark_read.py tests/unit/test_queue_persistence.py tests/integration/test_service_flow.py -q
```

Expected: PASS, including a new assertion that DB failure does not mark read.

- [ ] **Step 6: Commit**

```bash
git add src/domain src/utils/db_async.py src/exchange_service.py tests/unit/test_db_write_semantics.py tests/integration/conftest.py tests/integration/test_service_flow.py
git commit -m "fix: separate duplicate mail from database failure"
```

---

### Task 3: Establish Alembic Baseline and Explicit Database Bootstrap

**Files:**
- Create: `alembic.ini`
- Create: `alembic/env.py`
- Create: `alembic/versions/20260710_0001_existing_schema.py`
- Create: `alembic/versions/20260710_0002_p0_alignment.py`
- Create: `src/db/schema.py`
- Create: `src/db/bootstrap.py`
- Create: `tests/unit/test_database_revision.py`
- Create: `tests/integration/test_alembic_migrations.py`
- Modify: `pyproject.toml`
- Modify: `uv.lock`
- Modify: `src/main.py:33-46`
- Modify: `src/init_app.py:86-90`
- Modify: `src/utils/db_async.py:41-119`
- Modify: `src/db/migrate.py`
- Modify: `tests/unit/test_db_migrate.py`
- Delete: `migrations/002_observability_and_feedback.sql`

**Interfaces:**
- Consumes: `Settings.database_url`, upstream `AsyncPostgresSaver.MIGRATIONS`
- Produces: `require_current_database(dsn)`, `python -m src.db.bootstrap`

- [ ] **Step 1: Write migration tests against empty and legacy schemas**

```python
@pytest.mark.integration
def test_empty_database_upgrades_to_head(alembic_runner, empty_schema):
    alembic_runner.upgrade(empty_schema, "head")
    assert empty_schema.table_exists("emails_log")
    assert empty_schema.column_exists("emails_log", "error_message")
    assert empty_schema.column_exists("emails_log", "content_ref")


@pytest.mark.integration
def test_legacy_schema_upgrades_idempotently(alembic_runner, legacy_schema):
    alembic_runner.upgrade(legacy_schema, "head")
    alembic_runner.upgrade(legacy_schema, "head")
    assert legacy_schema.scalar("SELECT count(*) FROM alembic_version") == 1
```

The shared integration conftest defines `MigrationHarness.upgrade(schema, revision)`, `empty_schema` and `legacy_schema`. `legacy_schema` loads the exact 16-column table declared in Step 3 with two synthetic rows; `SchemaProbe` implements `table_exists`, `column_exists` and `scalar` against the isolated schema.

- [ ] **Step 2: Run and confirm failure**

```bash
TEST_POSTGRES_ADMIN_URL=postgresql://user:password@localhost:5432/postgres .venv/bin/python -m pytest tests/integration/test_alembic_migrations.py -q
```

Expected: FAIL because Alembic is not configured.

- [ ] **Step 3: Add Alembic and the idempotent baseline**

Add `alembic>=1.15` and refresh `uv.lock`. The baseline creates `emails_log` with exactly: `id TEXT PRIMARY KEY`; nullable `subject TEXT`, `sender TEXT`, `received_at TIMESTAMP`; `status TEXT DEFAULT 'pending'`; nullable `classification JSONB`, `draft_content TEXT`; `processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`; `updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`; nullable `routing_log JSONB`, `active_skills JSONB`, `original_draft TEXT`, `final_draft TEXT`, `draft_diff TEXT`, `approver_user_id TEXT`, `rejection_reason TEXT`. It also creates `app_kv_store(key TEXT PRIMARY KEY, value TEXT, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)` and the compatibility `processed_emails(id, processed_at)` view. The alignment revision executes:

```python
def upgrade() -> None:
    op.execute("ALTER TABLE emails_log ADD COLUMN IF NOT EXISTS error_message TEXT")
    op.execute("ALTER TABLE emails_log ADD COLUMN IF NOT EXISTS content_ref JSONB")
    op.execute("ALTER TABLE emails_log ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 0")
    op.execute("CREATE INDEX IF NOT EXISTS idx_emails_log_status_processed ON emails_log(status, processed_at DESC)")


def downgrade() -> None:
    raise RuntimeError("Forward-only production migration")
```

- [ ] **Step 4: Implement explicit bootstrap and read-only startup check**

`src/db/bootstrap.py` runs `alembic upgrade head` and the upstream checkpoint migrations with `autocommit=True`. `src/db/schema.py` reads `alembic_version`; `require_current_database()` raises before readiness when current and expected revisions differ. Remove runtime calls to `_init_db()` DDL and `checkpointer.setup()`. Convert `src/db/migrate.py` into a compatibility wrapper that delegates only to `src.db.bootstrap` and never reads filesystem SQL; delete the now-encoded SQL migration and update its tests.

- [ ] **Step 5: Verify empty, legacy, and repeated upgrades**

```bash
uv add "alembic>=1.15,<2"
uv lock --check
TEST_POSTGRES_ADMIN_URL=postgresql://user:password@localhost:5432/postgres .venv/bin/python -m pytest tests/unit/test_database_revision.py tests/integration/test_alembic_migrations.py -q
```

Expected: PASS for empty, legacy and repeated runs.

- [ ] **Step 6: Commit**

```bash
git add alembic.ini alembic pyproject.toml uv.lock src/db src/main.py src/init_app.py src/utils/db_async.py tests/unit/test_database_revision.py tests/unit/test_db_migrate.py tests/integration/test_alembic_migrations.py
git rm migrations/002_observability_and_feedback.sql
git commit -m "feat: establish explicit Alembic database bootstrap"
```

---

### Task 4: Bound the Existing Webhook Worker and Shut It Down Cleanly

**Files:**
- Modify: `src/exchange_service.py:434-521`
- Replace: `tests/unit/test_worker_concurrency.py`

**Interfaces:**
- Consumes: current in-memory queue until Phase 2
- Produces: `WebhookWorker(ctx, *, queue_maxsize: int = 500, concurrency: int = 3)`; `consumer_tasks: Sequence[asyncio.Task]`; fixed `start()` and `stop(drain_timeout: float = 30.0)`

- [ ] **Step 1: Write lifecycle tests using the real Worker**

```python
@pytest.mark.asyncio
async def test_worker_creates_only_fixed_consumers(worker):
    await worker.start()
    for index in range(100):
        await worker.enqueue_event(({"id": f"mail-{index}"}, False))
    assert len(worker.consumer_tasks) == worker.concurrency
    await worker.stop(drain_timeout=1.0)


@pytest.mark.asyncio
async def test_task_done_happens_after_processing(worker, processor):
    processor.block()
    await worker.start()
    await worker.enqueue_event(({"id": "mail-1"}, False))
    join_task = asyncio.create_task(worker.queue.join())
    await asyncio.sleep(0)
    assert join_task.done() is False
    processor.release()
    await asyncio.wait_for(join_task, timeout=1.0)
    await worker.stop(drain_timeout=1.0)
```

The test file constructs the real Worker with an `AsyncMock` processor and two `asyncio.Event` objects used by `block()`/`release()`; it does not inspect private Queue counters.

- [ ] **Step 2: Run and confirm failure**

```bash
.venv/bin/python -m pytest tests/unit/test_worker_concurrency.py -q
```

Expected: FAIL because the current dispatcher creates a task per email.

- [ ] **Step 3: Implement fixed consumers**

`start()` creates exactly `concurrency` `_consume()` tasks. Each consumer awaits one queue item, processes it inline, and calls `task_done()` in `finally`. `stop()` closes intake, awaits `queue.join()` with `asyncio.timeout(drain_timeout)`, then cancels and gathers only the fixed consumer tasks.

- [ ] **Step 4: Run tests and commit**

```bash
.venv/bin/python -m pytest tests/unit/test_worker_concurrency.py tests/unit/test_exchange_webhook.py -q
git add src/exchange_service.py tests/unit/test_worker_concurrency.py
git commit -m "fix: bound webhook worker tasks and drain on shutdown"
```

---

### Task 5: Enforce HTTP, Email, Attachment, and Model Input Limits

**Files:**
- Create: `src/safety/__init__.py`
- Create: `src/safety/input_limits.py`
- Create: `src/safety/model_budget.py`
- Create: `src/safety/http_response.py`
- Create: `tests/unit/test_input_limits.py`
- Create: `tests/unit/test_model_input_budget.py`
- Create: `tests/unit/test_exchange_response_limits.py`
- Modify: `src/config.py`
- Modify: `src/server.py:117-153`
- Modify: `src/utils/exchange_api.py`
- Modify: `src/nodes/categorizer.py`
- Modify: `src/nodes/drafter.py`
- Modify: `src/nodes/reviewer.py`
- Modify: `src/nodes/retriever_node.py`
- Modify: `src/router/engine.py`
- Modify: `.env.example`

**Interfaces:**
- Produces: `InputLimits`; `validate_email_input(email: Mapping[str, Any], limits: InputLimits) -> None`; `enforce_model_input_budget(role: str, value: str, *, budget: TokenBudget) -> None`; `read_json_limited(response: httpx.Response, *, max_bytes: int) -> dict[str, Any]`

- [ ] **Step 1: Write boundary tests**

```python
def test_attachment_total_limit_is_checked_before_decode():
    default_limits = InputLimits()
    encoded = "A" * 70_000_000
    with pytest.raises(InputLimitExceeded) as caught:
        validate_email_input({"body": "ok", "attachments": [{"content": encoded}]}, default_limits)
    assert caught.value.category == "attachment_total_bytes"


def test_utf8_byte_upper_bound_blocks_oversized_prompt():
    budget = TokenBudget(max_input_tokens=8, max_output_tokens=2, max_total_tokens=10)
    with pytest.raises(ModelInputTooLarge):
        enforce_model_input_budget("categorizer", "这是一段超过预算的正文", budget=budget)
```

- [ ] **Step 2: Run and confirm missing modules**

```bash
.venv/bin/python -m pytest tests/unit/test_input_limits.py tests/unit/test_model_input_budget.py tests/unit/test_exchange_response_limits.py -q
```

Expected: collection fails because `src.safety` and the four locked interfaces do not exist.

- [ ] **Step 3: Implement exact defaults and safe upper-bound estimation**

```python
@dataclass(frozen=True)
class InputLimits:
    webhook_bytes: int = 1_048_576
    exchange_response_bytes: int = 67_108_864
    body_bytes: int = 10_485_760
    attachment_count: int = 20
    attachment_single_bytes: int = 26_214_400
    attachment_total_bytes: int = 52_428_800


@dataclass(frozen=True)
class TokenBudget:
    max_input_tokens: int
    max_output_tokens: int
    max_total_tokens: int


class InputLimitExceeded(ValueError):
    def __init__(self, category: str) -> None:
        self.category = category
        super().__init__(category)


class ModelInputTooLarge(InputLimitExceeded):
    pass


def conservative_token_upper_bound(value: str) -> int:
    return len(value.encode("utf-8"))
```

Use streamed response reads and abort when accumulated bytes exceed the limit. Validate Base64 decoded size with `len(encoded) * 3 // 4` before decoding. Add the same values to `Settings` and `.env.example`.

- [ ] **Step 4: Wire the limits before JSON/model work**

Webhook rejects oversized bodies with 413 before logging or parsing. Exchange detail/list reads use `read_json_limited()`. `validate_email_input()` runs before ContentStore/Graph. Model callers use `enforce_model_input_budget()` until Phase 4 replaces it with the unified gateway.

- [ ] **Step 5: Run tests and commit**

```bash
.venv/bin/python -m pytest tests/unit/test_input_limits.py tests/unit/test_model_input_budget.py tests/unit/test_exchange_response_limits.py tests/unit/test_exchange_webhook.py -q
git add src/safety src/config.py src/server.py src/utils/exchange_api.py src/nodes/categorizer.py src/nodes/drafter.py src/nodes/reviewer.py src/nodes/retriever_node.py src/router/engine.py .env.example tests/unit/test_input_limits.py tests/unit/test_model_input_budget.py tests/unit/test_exchange_response_limits.py tests/unit/test_exchange_webhook.py
git commit -m "fix: enforce bounded email and model inputs"
```

---

### Task 6: Add the Minimal AES-GCM ContentStore

**Files:**
- Create: `src/storage/__init__.py`
- Create: `src/storage/content_store.py`
- Create: `src/storage/encrypted_files.py`
- Create: `tests/unit/storage/conftest.py`
- Create: `tests/unit/storage/test_encrypted_content_store.py`
- Create: `tests/integration/storage/test_content_ref_restart.py`
- Modify: `pyproject.toml`
- Modify: `src/config.py`
- Modify: `src/init_app.py`
- Modify: `docker-compose.yml`
- Modify: `.env.example`
- Modify: `uv.lock`

**Interfaces:**
- Produces: the `ContentRef` and `ContentStore` interfaces locked in the master plan; `AppContext.content_store`

- [ ] **Step 1: Write encryption and restart tests**

```python
@pytest.mark.asyncio
async def test_content_store_never_writes_plaintext(store, root):
    ref = await store.put_email(8, "mail-1", {"body": "secret-body", "attachments": []})
    disk_bytes = b"".join(path.read_bytes() for path in root.rglob("*.enc"))
    assert b"secret-body" not in disk_bytes
    assert (await store.load_email(ref))["body"] == "secret-body"


@pytest.mark.asyncio
async def test_content_ref_survives_new_store_instance(store_factory):
    first = store_factory()
    ref = await first.put_email(8, "mail-2", {"body": "persisted", "attachments": []})
    second = store_factory()
    assert (await second.load_email(ref))["body"] == "persisted"
```

`tests/unit/storage/conftest.py` defines a temporary `root`, valid AES key, `store`, and `store_factory()` that creates new instances over the same root/key without retaining process memory.

- [ ] **Step 2: Run and confirm failure**

```bash
.venv/bin/python -m pytest tests/unit/storage/test_encrypted_content_store.py tests/integration/storage/test_content_ref_restart.py -q
```

- [ ] **Step 3: Implement encrypted atomic storage**

Add `cryptography>=44` and implement AES-GCM with a 32-byte Base64 key. Compute `object_id` as UUID4, write nonce + ciphertext to a random sibling temp file, flush/fsync, then `os.replace()`. AAD is reconstructable from the reference alone: `f"{ref.account_id}:{ref.object_id}:{ref.key_version}".encode()`. Persist only ciphertext under the configured content volume. Attachment Base64 is decoded before encryption and never written plaintext.

- [ ] **Step 4: Wire settings and AppContext**

Add `CONTENT_STORE_ROOT=/app/data/content`, `CONTENT_STORE_KEY`, and `CONTENT_STORE_KEY_VERSION=v1`. Production validation rejects missing/invalid keys. Mount `content_data:/app/data/content` in Compose.

- [ ] **Step 5: Run tests and commit**

```bash
uv add "cryptography>=44,<46"
uv lock --check
.venv/bin/python -m pytest tests/unit/storage/test_encrypted_content_store.py tests/integration/storage/test_content_ref_restart.py -q
git add src/storage src/config.py src/init_app.py pyproject.toml uv.lock docker-compose.yml .env.example tests/unit/storage/conftest.py tests/unit/storage/test_encrypted_content_store.py tests/integration/storage/test_content_ref_restart.py
git commit -m "feat: add encrypted durable email content store"
```

---

### Task 7: Replace Large Graph Payloads with content_ref

**Files:**
- Create: `src/graph/dependencies.py`
- Create: `src/graph/state_factory.py`
- Create: `tests/unit/test_graph_state_slim.py`
- Create: `tests/unit/test_email_processor_content_boundary.py`
- Modify: `src/graph/state.py`
- Modify: `src/graph/builder.py`
- Modify: `src/exchange_service.py:47-78`
- Modify: `src/nodes/categorizer.py`
- Modify: `src/nodes/retriever_node.py`
- Modify: `src/nodes/drafter.py`
- Modify: `src/nodes/reviewer.py`
- Modify: `src/utils/email_processor.py`
- Modify: `src/init_app.py`
- Modify: `src/utils/db_async.py`

**Interfaces:**
- Consumes: `ContentStore`
- Produces: minimal `GraphDependencies(content_store: ContentStore)`; `build_initial_graph_state(metadata, ref)` and node-local hydration that is never returned to State

- [ ] **Step 1: Write State serialization tests**

```python
def test_initial_state_contains_no_large_email_content():
    content_ref = ContentRef(
        account_id=8,
        object_id="obj-1",
        key_version="v1",
        sha256="0" * 64,
    )
    state = build_initial_graph_state(
        {"id": "mail-1", "subject": "subject", "sender": "sender@example.com"},
        content_ref,
    )
    encoded = json.dumps(state, ensure_ascii=False)
    assert "body" not in state["email"]
    assert "attachments" not in state["email"]
    assert "base64" not in encoded.lower()
    assert len(encoded.encode("utf-8")) < 16_384
```

- [ ] **Step 2: Run and confirm failure**

```bash
.venv/bin/python -m pytest tests/unit/test_graph_state_slim.py tests/unit/test_email_processor_content_boundary.py -q
```

- [ ] **Step 3: Narrow AgentState and inject ContentStore into graph nodes**

`AgentState` retains `email_id`, small `email` metadata, JSON `content_ref`, small classification/routing fields, `draft_id`, review status and small error summary. Phase 1 creates `@dataclass(frozen=True) class GraphDependencies: content_store: ContentStore`; Phase 4 extends that same file. `AppContext` builds it once and `build_graph(checkpointer, dependencies)` binds it. Every node loads body locally and returns only its delta. Persist `content_ref` JSON through the typed database method so a restart can rebuild State without body data.

- [ ] **Step 4: Remove disconnected image copying**

Delete the `_image_attachments` copy path from `EmailProcessor.process_batch()`. Image bytes remain in ContentStore; no production node may add them to Graph State.

- [ ] **Step 5: Run graph and checkpoint tests**

```bash
.venv/bin/python -m pytest tests/unit/test_graph_state_slim.py tests/unit/test_email_processor_content_boundary.py tests/unit/test_nodes.py tests/unit/test_reviewer_node.py -q
```

Expected: PASS and no checkpoint State over 16 KiB for the large-attachment fixture.

- [ ] **Step 6: Commit**

```bash
git add src/graph src/init_app.py src/utils/db_async.py src/exchange_service.py src/nodes src/utils/email_processor.py tests/unit/test_graph_state_slim.py tests/unit/test_email_processor_content_boundary.py tests/unit/test_nodes.py tests/unit/test_reviewer_node.py
git commit -m "fix: keep email bodies and attachments out of graph state"
```

---

### Task 8: Fail Closed on Model Errors and Add Temporary Approval/Send CAS

**Files:**
- Create: `tests/unit/test_model_failure_semantics.py`
- Create: `tests/unit/test_approval_claim.py`
- Create: `tests/unit/test_sender_idempotency.py`
- Create: `src/safety/approval_claim.py`
- Modify: `src/nodes/categorizer.py:94-126`
- Modify: `src/nodes/drafter.py`
- Modify: `src/nodes/reviewer.py`
- Modify: `src/utils/lark_app.py:997-1047`
- Modify: `src/nodes/sender.py`

**Interfaces:**
- Consumes: `compare_and_set_status()` from Task 2
- Produces: `claim_approval(email_id, user_id, db_manager) -> bool`; `claim_send(email_id, db_manager) -> bool`; temporary `waiting_approval -> approved -> sending` CAS until Phase 3 replaces it

- [ ] **Step 1: Write failure-closed and duplicate-click tests**

```python
@pytest.mark.asyncio
async def test_categorizer_failure_requires_manual_review(llm):
    llm.side_effect = TimeoutError("model timeout")
    state = {
        "email_id": "mail-1",
        "email": {"subject": "subject", "sender": "sender@example.com"},
        "content_ref": {"account_id": 8, "object_id": "obj", "key_version": "v1", "sha256": "0" * 64},
    }
    result = await categorize_email(state, model=llm)
    assert result["next_step"] == "manual_review"
    assert result["classification"]["need_reply"] is not False


@pytest.mark.asyncio
async def test_duplicate_approval_claim_succeeds_once(db_manager):
    first, second = await asyncio.gather(
        claim_approval("mail-1", "ou_1", db_manager),
        claim_approval("mail-1", "ou_1", db_manager),
    )
    assert sorted([first, second]) == [False, True]


@pytest.mark.asyncio
async def test_sender_claim_prevents_second_remote_call(db_manager, exchange):
    results = await asyncio.gather(
        claim_send("mail-1", db_manager),
        claim_send("mail-1", db_manager),
    )
    for claimed in results:
        if claimed:
            await exchange.send_reply("mail-1")
    assert sorted(results) == [False, True]
    exchange.send_reply.assert_awaited_once_with("mail-1")
```

`tests/unit/test_approval_claim.py` defines an async lock-backed fake `db_manager.compare_and_set_status`; `tests/unit/test_sender_idempotency.py` reuses it with an `AsyncMock` Exchange adapter. `tests/unit/test_model_failure_semantics.py` injects an `AsyncMock` model into the new optional `model` parameter of `categorize_email`.

- [ ] **Step 2: Run and confirm failure**

```bash
.venv/bin/python -m pytest tests/unit/test_model_failure_semantics.py tests/unit/test_approval_claim.py tests/unit/test_sender_idempotency.py -q
```

- [ ] **Step 3: Remove unsafe fallbacks**

Categorizer catches classified model errors and returns `next_step="manual_review"` plus a safe error code. Drafter never uses exception text as a draft. Reviewer errors and rewrite-limit exhaustion enter manual review; they never silently approve.

- [ ] **Step 4: Gate approval and send with CAS**

`claim_approval()` calls `compare_and_set_status(expected=frozenset({"waiting_approval"}), target="approved")`; `process_approval()` resumes Graph only when it returns true. `claim_send()` uses `expected=frozenset({"approved"})`, target `sending`; Sender calls Exchange only on true. A restart that sees `sending` never sends again and moves to manual review; Phase 3 replaces this with `send_unknown` and Outbox.

- [ ] **Step 5: Run tests and commit**

```bash
.venv/bin/python -m pytest tests/unit/test_model_failure_semantics.py tests/unit/test_approval_claim.py tests/unit/test_sender_idempotency.py tests/unit/test_lark_app.py -q
git add src/safety/approval_claim.py src/nodes src/utils/lark_app.py tests/unit/test_model_failure_semantics.py tests/unit/test_approval_claim.py tests/unit/test_sender_idempotency.py tests/unit/test_lark_app.py
git commit -m "fix: fail closed and gate duplicate approval sends"
```

---

### Task 9: Apply the Minimum Production Security Boundary

**Files:**
- Create: `src/security/__init__.py`
- Create: `src/security/auth.py`
- Create: `src/security/redaction.py`
- Create: `tests/unit/test_runtime_security.py`
- Create: `tests/unit/test_health_ready.py`
- Create: `tests/unit/test_lark_authorization.py`
- Create: `tests/unit/test_logging_redaction.py`
- Create: `docker-compose.dev.yml`
- Modify: `tests/test_server_local.py`
- Modify: `tests/test_view_original_local.py`
- Modify: `tests/unit/test_debug_endpoint_guard.py`
- Modify: `src/config.py`
- Modify: `src/server.py`
- Modify: `src/utils/lark_app.py`
- Modify: `src/providers/codex_provider.py`
- Modify: `src/utils/exchange_api.py`
- Modify: `docker-compose.yml`
- Modify: `.env.example`

**Interfaces:**
- Produces: `validate_runtime_security()`, `is_lark_operator_allowed()`, `require_metrics_auth()`

- [ ] **Step 1: Write minimum-security tests**

```python
def test_production_rejects_default_password(settings):
    settings.APP_ENV = "production"
    settings.POSTGRES_PASSWORD = SecretStr("password")
    with pytest.raises(RuntimeError, match="POSTGRES_PASSWORD"):
        validate_runtime_security(settings)


def test_health_never_returns_last_error(client, open_circuit):
    response = client.get("/health")
    assert response.status_code == 200
    assert "last_error" not in response.text


def test_unlisted_lark_operator_is_rejected(settings):
    settings.LARK_ALLOWED_OPEN_IDS = "ou_allowed"
    assert is_lark_operator_allowed("ou_unknown", settings) is False
```

The four security test files construct `Settings` explicitly, use FastAPI `TestClient`, override readiness dependencies with `open_circuit`, and capture structlog output. They do not depend on a live Exchange/Lark/model service.

- [ ] **Step 2: Run and confirm failure**

```bash
.venv/bin/python -m pytest tests/unit/test_runtime_security.py tests/unit/test_health_ready.py tests/unit/test_lark_authorization.py tests/unit/test_logging_redaction.py -q
```

- [ ] **Step 3: Implement fail-fast production settings**

Default `EXCHANGE_SSL_VERIFY=True`. Add `APP_ENV`, `METRICS_TOKEN`, `LARK_ALLOWED_OPEN_IDS`, `EXCHANGE_CA_FILE`. Production rejects empty allowlists, default DB credentials, empty webhook secret, empty ContentStore key, disabled TLS and placeholder secrets.

- [ ] **Step 4: Minimize endpoints and logs**

`/health` returns only status/version/time. `/ready` checks DB/schema without returning exception text. `/metrics` requires `Authorization: Bearer <METRICS_TOKEN>`. `/email/{id}` returns 404 until Phase 5 secure preview is delivered. Webhook logs metadata only; raw headers/body/signatures are prohibited. Lark commands and card actions reject operators outside the allowlist.

- [ ] **Step 5: Remove TLS downgrade and isolate production ports**

Delete Codex certificate-error retry with `verify=False`. Exchange supports a configured CA bundle. Production Compose removes host ports for PostgreSQL/Qdrant, removes port 15000, requires credentials from env, and uses an internal network. `docker-compose.dev.yml` binds local-only `127.0.0.1:5432` and `127.0.0.1:6333` for development.

- [ ] **Step 6: Verify and commit**

```bash
.venv/bin/python -m pytest tests/unit/test_runtime_security.py tests/unit/test_health_ready.py tests/unit/test_lark_authorization.py tests/unit/test_logging_redaction.py tests/unit/test_codex_provider.py tests/unit/test_exchange_webhook.py tests/unit/test_debug_endpoint_guard.py tests/test_server_local.py tests/test_view_original_local.py -q
docker compose config >/dev/null
docker compose -f docker-compose.yml -f docker-compose.dev.yml config >/dev/null
git add src/security src/config.py src/server.py src/utils/lark_app.py src/providers/codex_provider.py src/utils/exchange_api.py docker-compose.yml docker-compose.dev.yml .env.example tests/unit/test_runtime_security.py tests/unit/test_health_ready.py tests/unit/test_lark_authorization.py tests/unit/test_logging_redaction.py tests/unit/test_codex_provider.py tests/unit/test_exchange_webhook.py tests/unit/test_debug_endpoint_guard.py tests/test_server_local.py tests/test_view_original_local.py
git commit -m "fix: establish minimum production security boundary"
```

---

### Task 10: Add Safe Checkpoint Cleanup and Complete the Phase Gate

**Files:**
- Create: `src/maintenance/__init__.py`
- Create: `src/maintenance/checkpoint_cleanup.py`
- Create: `scripts/checkpoint_cleanup.py`
- Create: `tests/unit/test_checkpoint_cleanup_plan.py`
- Create: `tests/integration/test_checkpoint_cleanup.py`
- Modify: `docs/superpowers/reports/2026-07-10-stage-0-reliability-baseline.md`

**Interfaces:**
- Produces: immutable `CleanupCandidate`, `CheckpointCleanupPlan`, `CheckpointCleanupReport`; `CheckpointCleaner.plan(*, older_than: datetime, limit: int) -> CheckpointCleanupPlan`; `CheckpointCleaner.run(plan_id: str, *, dry_run: bool, backup_id: str | None, limit: int) -> CheckpointCleanupReport`

- [ ] **Step 1: Write eligibility tests**

```python
def test_cleanup_excludes_nonterminal_and_unknown():
    now = datetime(2026, 7, 10, tzinfo=UTC)
    candidates = [
        CleanupCandidate("t1", "sent", now - timedelta(hours=25), 100),
        CleanupCandidate("t2", "waiting_approval", now - timedelta(days=8), 100),
        CleanupCandidate("t3", "accepted", now - timedelta(days=8), 100),
        CleanupCandidate("t4", "send_unknown", now - timedelta(days=8), 100),
    ]
    selected = select_cleanup_candidates(candidates, older_than=now - timedelta(hours=24))
    assert {item.status for item in selected}.isdisjoint(
        {"waiting_approval", "accepted", "send_unknown"}
    )


@pytest.mark.asyncio
async def test_execute_requires_backup_id(cleaner, cleanup_plan):
    with pytest.raises(ValueError, match="backup_id"):
        await cleaner.run(cleanup_plan.id, dry_run=False, backup_id=None, limit=100)
```

`tests/unit/test_checkpoint_cleanup_plan.py` imports the production `CleanupCandidate` and defines the fixed UTC time used above. `tests/integration/test_checkpoint_cleanup.py` builds `cleaner` against the shared migrated database and inserts `cleanup_plan`; it replaces the checkpointer delete Port with an AsyncMock so eligibility/authorization can be verified without deleting live rows.

- [ ] **Step 2: Run and confirm failure**

```bash
.venv/bin/python -m pytest tests/unit/test_checkpoint_cleanup_plan.py tests/integration/test_checkpoint_cleanup.py -q
```

- [ ] **Step 3: Implement dry-run-first deletion**

Query checkpoint thread sizes joined to `emails_log`; select only mapped terminal states older than 24 hours. Default `dry_run=True`. Execution requires a non-empty backup ID, processes at most the configured batch size, and calls `AsyncPostgresSaver.adelete_thread(thread_id)`. Record scanned/eligible/deleted/reclaimed estimates without email content.

- [ ] **Step 4: Run dry-run on the live database**

```bash
.venv/bin/python scripts/checkpoint_cleanup.py --dry-run --older-than-hours 24 --limit 100
```

Expected: report candidates but delete zero; waiting approval and unknown states appear only in excluded counts.

- [ ] **Step 5: Run the Phase 1 gate**

```bash
.venv/bin/ruff check src/ tests/
.venv/bin/python -m pytest -q
.venv/bin/python -m pytest --cov=src --cov-report=term-missing
uv lock --check
.venv/bin/python -m pip check
git diff --check
```

Expected: all tests and checks pass, lock is current, and checkpoint cleanup has not deleted live data.

- [ ] **Step 6: Commit**

```bash
git add src/maintenance scripts/checkpoint_cleanup.py tests/unit/test_checkpoint_cleanup_plan.py tests/integration/test_checkpoint_cleanup.py docs/superpowers/reports/2026-07-10-stage-0-reliability-baseline.md
git commit -m "feat: add guarded checkpoint cleanup workflow"
```
