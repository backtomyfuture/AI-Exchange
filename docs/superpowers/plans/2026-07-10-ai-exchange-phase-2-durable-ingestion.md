# AI-Exchange Phase 2 Durable Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 建成 Webhook 与 5 分钟增量同步共用的 PostgreSQL Inbox、固定 Worker、租约、死信和 generation/fencing 收件链路，并在 Phase 2 交付 dormant Shadow/readiness 能力；当前 extension 不满足 `exchange_sync_contract_v2` 时明确停在 external-blocked，而不是伪装成可生产激活。

**Architecture:** 新建独立 `src/ingestion/` 边界。Durable Webhook 路径只验证并持久化后返回 202；Sync 通过账户/文件夹 advisory lock 拉取批次，并在同一事务写 Inbox 与游标。Phase 2 生产权威最多推进到 side-effect-free Shadow 和 fenced readiness；真正 `durable_active` 必须等 Phase 3 的审批、Notification/Mailbox/Send Outbox、Lark action authority 与旧卡失效证明全部通过后执行。

**Tech Stack:** Python 3.12、FastAPI、psycopg 3、Alembic、PostgreSQL `SKIP LOCKED`/advisory lock、httpx、pytest。

## Global Constraints

- Phase 1 Alembic baseline、typed DB error、`ProcessingOutcome` 和最小 ContentStore 必须已经合入。
- `SYNC_INTERVAL_SECONDS=300`，默认 `SYNC_BATCH_SIZE=500`。
- 当前服务端只明确返回 create/update/delete；`update.item.is_read` 作为读状态投影，不能假设已存在独立 read 事件。
- 远程 HTTP 调用不得位于数据库事务中。
- 游标只有在本批 Inbox 事件与新游标同事务提交成功后才能推进。
- 游标失效必须进入 `reset_required`；禁止自动传 `None` 冷启动。
- Shadow **candidate** 绝不执行模型、飞书、Exchange 或 Qdrant 副作用；legacy-authoritative 业务链仍可通过 stamped `LegacyProcessingAdapter` 与 exact `LegacyEffectGuard` 连续运行。
- 采用发布方案 B：Phase 2 禁止把数据库 authority 切到 `durable_active`，禁止在生产启动 Durable claim、202 intake 或 Sync 写入；本阶段只允许 legacy-authoritative、Shadow、quiescing/standby 和不可变 readiness evidence。
- 当前 Exchange extension 已确认分页上界与 `read_flag_change` 均不满足 `exchange_sync_contract_v2`。AI 仓库仍须完成并测试全部 dormant/fail-closed 实现，但 readiness 状态必须是 `blocked_external_exchange_sync_contract`；只有后续独立修复 extension 并生成真实 v2 证据后才可解除。
- Phase 3 激活后的回滚仍由 Durable Inbox 排空已返回 202 的数据，不能退回当前内存队列。
- 所有 Inbox claim/complete 都比较租约 owner、generation 和 fencing token；任何 Outbox 继承相同所有权规则。

---

## File Map

| Area | Files | Responsibility |
|---|---|---|
| Schema/types | `alembic/versions/20260710_0003_durable_ingestion.py`, `alembic/versions/20260713_0004_ingestion_policy_ignored.py`, future `20260713_0005_sync_reconciliation_control.py`, `20260713_0006_shadow_inputs.py` and `20260713_0007_runtime_activation.py`, `src/ingestion/models.py` | Pipeline ownership, Inbox, cursor, aggregate and audit; ignored policy; resumable cold-start control; replayable Shadow input; DB-authoritative runtime activation |
| Intake | `src/ingestion/normalization.py`, `src/ingestion/webhook.py` | Canonical Webhook/Sync normalization, dedupe and durable 202 receipt |
| Ownership | `src/ingestion/ownership.py`, `src/ingestion/repository.py` | Generation/fencing, leases, retry/dead-letter and claim/complete CAS |
| State | `src/ingestion/email_events.py` | Sole allowed create/update/read/delete transition table and cancellation decisions |
| Reconciliation | `src/ingestion/sync.py`, `src/ingestion/cold_start.py`, `src/scheduler/sync_reconciliation.py` | Dedicated advisory-lock connection, atomic cursor/Inbox commit and approved reset |
| Execution | `src/ingestion/processing.py`, `src/ingestion/worker.py`, `src/ingestion/legacy_adapter.py`, `src/ingestion/runtime.py` | Stamped adapter selection, fixed Workers, guarded compatibility processing and owned lifecycle |
| Operations | `src/ingestion/backfill.py`, `src/ingestion/cutover.py`, `scripts/backfill_durable_ingestion.py`, `scripts/manage_pipeline.py` | Dry-run/quarantine migration, Shadow evidence and Phase-3 activation readiness; no Phase-2 durable activation |
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
- Produces: `PipelineGeneration`, `NormalizedIngressEvent`, `SyncChange`, `SyncBatch`, `InboxLease`, `InboxStats`, `IngressReceipt`; named fixtures used by Tasks 1-11

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

Create `pipeline_ownership`, `event_inbox`, `sync_cursors`, `emails`, `audit_events`, and `pipeline_shadow_comparisons`. Resumable `sync_cold_start_plans` is deliberately added by Task 7's linear `0005` revision; it is not present in `0003`. The migration must include:

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

The migration and exact schema manifest are the column source of truth. Core ownership columns are `pipeline_ownership(account_id, generation, pipeline_name, state, fencing_token, created_by, reason)`. Core Inbox columns are `event_inbox(account_id, external_email_id, folder_key, source, raw_event_type, change_kind, dedupe_key, source_version, source_event_at, payload, processing_policy, pipeline_name, generation, fencing_token, status, lease_owner, lease_until, attempts, available_at, processing_started_at, effect_started_at, safe_error_code, safe_error_summary, received_at, updated_at)`. `sync_cursors` owns the folder cursor, state, attempt/success timestamps, version and blocked-contract evidence. `emails` owns its sticky generation/fence, processing Inbox reference, source/effect timestamps, bounded content reference and read projection. Audit and Shadow comparison tables retain bounded actor/reason or stable hashes and no raw body beyond the governed Inbox payload; Task 7 owns the later cold-start plan schema.

Lock the migration types/constraints as follows: IDs are UUID; account/generation/fencing/version/attempts are BIGINT; payload/content reference are JSONB; event/status/policy/pipeline/folder/error fields are TEXT; lease/cursor/event dates are TIMESTAMPTZ. `pipeline_ownership` primary key is `(account_id,generation)`, state checks the four `PipelineGenerationState` values, and a partial unique index allows one `current_ingress`. `event_inbox.dedupe_key CHAR(64) UNIQUE`, attempts defaults 0 with nonnegative check, status checks `pending/retry_wait/leased/completed/dead_letter/manual_review`, and its four-column foreign key pins ownership generation, fence and pipeline. `sync_cursors` primary key is `(account_id,folder_key)` with status `active/reset_required/cold_start_pending/blocked_contract`. `emails` has `UNIQUE(account_id,external_email_id)`, nonnegative version, and sticky generation/fence ownership. Audit rows use UUID, account/email/object IDs, action/result/safe metadata JSONB and TIMESTAMPTZ. `pipeline_shadow_comparisons` uses the exact unique key `(account_id, generation, pipeline_name, candidate_pipeline_name, candidate_build_id, candidate_config_hash, event_key)` plus input/decision hashes only.

- [ ] **Step 4: Define immutable DTOs**

```python
class ChangeKind(StrEnum):
    CREATE = "create"
    UPDATE = "update"
    READ = "read"
    DELETE = "delete"


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
    processing_policy: ProcessingPolicy
    source_event_at: datetime | None = None


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
    contract_version: Literal["exchange_sync_contract_v2"]
    cursor: str
    changes: Sequence[SyncChange]
    includes_last: bool


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
    manual_review: int
    oldest_pending_seconds: float


@dataclass(frozen=True)
class InboxDisposition:
    status: Literal["retry_wait", "dead_letter", "manual_review"]
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
- Create: `alembic/versions/20260713_0004_ingestion_policy_ignored.py`
- Modify: `src/domain/errors.py`
- Modify: `src/ingestion/__init__.py`
- Modify: `src/ingestion/models.py`
- Modify: `src/db/access_contract.py`
- Modify: `src/db/bootstrap.py`
- Modify: `src/db/schema.py`
- Modify: `src/db/schema_contract.py`
- Modify: `tests/unit/ingestion/test_models.py`
- Modify: `tests/integration/ingestion/test_schema.py`
- Modify: `tests/integration/ingestion/test_access_roles.py`
- Modify: `tests/integration/ingestion/test_execution_hook_exactness.py`

**Interfaces:**
- Consumes: `NormalizedIngressEvent`, raw Webhook bytes and `SyncChange`
- Produces:

```python
def normalize_webhook_event(
    *,
    raw_body: bytes,
    payload: Mapping[str, Any],
    processing_policy: ProcessingPolicy,
    header_event: str | None = None,
) -> NormalizedIngressEvent: ...


def normalize_sync_change(
    account_id: int,
    folder: str,
    cursor: str,
    change: SyncChange,
    *,
    processing_policy: ProcessingPolicy,
) -> NormalizedIngressEvent: ...


def validate_sync_change_contract(change: Mapping[str, Any] | SyncChange) -> SyncChange: ...
```

`normalize_webhook_event` is entirely keyword-only. `normalize_sync_change` keeps its first four arguments positional-compatible while making `processing_policy` required and keyword-only. `validate_sync_change_contract()` is policy-free and is the sole Sync item shape/length/ID/payload validator reused later by the HTTP client and normalizer; neither layer may maintain a second validator. Neither normalizer nor `NormalizedIngressEvent` may default to `ProcessingPolicy.FULL`; every caller must choose a policy explicitly.

- [ ] **Step 1: Write strict-boundary and deterministic-key tests**

```python
def test_same_webhook_retry_has_same_key(raw_body, payload):
    first = normalize_webhook_event(
        raw_body=raw_body,
        payload=payload,
        processing_policy=ProcessingPolicy.FULL,
        header_event="NewMailEvent",
    )
    second = normalize_webhook_event(
        raw_body=raw_body,
        payload=payload,
        processing_policy=ProcessingPolicy.FULL,
        header_event="NewMailEvent",
    )
    assert first.dedupe_key == second.dedupe_key


def test_sync_change_key_includes_cursor_and_change_type():
    created = normalize_sync_change(
        8,
        "INBOX",
        "cursor-2",
        SyncChange(ChangeKind.CREATE, "m1", {"id": "m1"}),
        processing_policy=ProcessingPolicy.FULL,
    )
    deleted = normalize_sync_change(
        8,
        "INBOX",
        "cursor-2",
        SyncChange(ChangeKind.DELETE, "m1", None),
        processing_policy=ProcessingPolicy.FULL,
    )
    assert created.dedupe_key != deleted.dedupe_key
```

- [ ] **Step 2: Implement the authoritative Webhook boundary**

Parse `raw_body` as strict UTF-8 JSON and require a top-level object. Reject duplicate object keys, `NaN`/`Infinity`, non-JSON values, cycles, malformed UTF-8 and any canonical mismatch between the parsed signed body and the supplied `payload`. Reject NUL in every JSON key/value because PostgreSQL `jsonb` cannot represent it. Size the immutable payload against PostgreSQL `jsonb::text`, including conservative fixed-point expansion of exponent-form numbers, rather than compact Python JSON bytes. The parsed signed body is authoritative; `payload` is only an equality assertion and `header_event` is only an optional consistency assertion.

Require the body to contain `event` or `event_type`. If both exist, they must be non-null and exactly equal; a header can neither fill a missing body event nor override it. Map only `NewMailEvent`/`CreatedEvent` to `CREATE`, `ModifiedEvent` to `UPDATE`, and `DeletedEvent` to `DELETE`.

Read `account_id` only from the signed body and require a positive PostgreSQL BIGINT, excluding booleans. Resolve the external ID from `item_id.id`, then top-level `id`, then `item.id`, but reject any disagreement among present values. Require `parent_folder_id.id`; optional folder assertions must normalize to the same value. Resolve version from consistent changekeys (`item_id.changekey`, top-level `changekey`, `item.changekey`) and use `watermark` only as fallback. Accept only timezone-aware ISO timestamps or bounded epoch timestamps from the signed body. Preserve the complete validated body in the normalized payload.

- [ ] **Step 3: Implement canonical hashing and Sync normalization**

Hash a schema-versioned identity with SHA-256 over UTF-8 canonical JSON using `ensure_ascii=False`, `sort_keys=True`, compact separators and `allow_nan=False`. The common identity contains account, source, raw event type, change kind, external ID, normalized folder, source version, cursor, trusted source time and raw-body digest. Webhook identity must select exactly one retry discriminator in this order: source version, otherwise trusted body time, otherwise exact raw-body SHA-256; the two lower-priority identity fields are `None`. Header, payload metadata and processing policy are excluded from the identity.

`validate_sync_change_contract()` has two explicit input layers and never confuses them. For an untrusted raw HTTP mapping, the outer keys are **exactly** `change_type`, `id`, and `item`; unknown or missing keys fail closed. Raw create/update `item` has exactly `id`, `subject`, `sender`, `received_time`, `is_read`, and `has_attachments`; raw delete requires `item is None`. For an already-constructed typed `SyncChange`, create/update may carry a validated subset of those six item fields, inner `id` may be absent but must equal the outer ID when present, and an optional typed-only `source_version` is allowed; delete still has no item. In both layers only create/update/delete are accepted, the outer ID is a bounded non-empty string, `subject`/`sender` are bounded strings or null, `received_time` is a bounded display-time string or null (including the service's current naive ISO serialization), and `is_read`/`has_attachments` are booleans rather than integers. Nested objects/arrays, transport-level `source_version`, unknown raw keys, coercion, invalid scalar types, surrogate/NUL and JSONB overflow fail closed. The validator remains policy-free and returns one canonical `SyncChange`; `normalize_sync_change()` calls it even for a typed value before binding the caller-supplied policy. Standard folder aliases canonicalize to uppercase while custom folder case is preserved after trimming. Every Sync identity includes the cursor and change type. The Exchange service obtains EWS `datetime_received` but serializes response `received_time`; this is display metadata only. Until a separately versioned trusted timestamp contract exists, Sync always emits `source_event_at=None` and never promotes `received_time`, typed `source_version` or another item field to trusted source time.

Define `IngressValidationCode` as a closed safe-code allowlist and `IngressValidationError(ErrorKind.VALIDATION)` with a fixed safe summary/repr and no raw IDs, body, folder, version, cursor or chained cause. Never overload `InputLimitExceeded` for schema validation.

- [ ] **Step 4: Make ignored delivery durable with a forward-only migration**

Add required `ProcessingPolicy.IGNORED` and keep policy outside dedupe identity so the same verified source event retains one durable identity regardless of routing policy. Apply a new `20260713_0004` migration that changes only `ck_event_inbox_processing_policy` to admit `ignored` and raises from `downgrade()` because production migrations are forward-only. Before changing the CHECK, the revision takes `ACCESS EXCLUSIVE` on `event_inbox` and rejects any non-empty table in the same migration transaction. Keep both the exact `0003` and `0004` physical constraint digests in the schema contract so the new code can verify the old head before advancing it.

This release supports **code-first only**, not migration-first: deploy the new code with every Phase 2 flag off on exact `0003`; require `event_inbox` to be inactive and empty; run `bootstrap_database()` to `0004`; pass exact runtime/schema and migration/runtime/maintenance/auditor role gates; then keep Durable intake dormant while Shadow/readiness work continues. Bootstrap performs an early empty-table preflight, and the revision repeats the check after taking `ACCESS EXCLUSIVE`, in the same transaction as `DROP CONSTRAINT`/`ADD CHECK`; a concurrent or pre-existing row therefore aborts the whole revision and leaves `0003` unchanged. Any pre-existing `pending` ignored/historical row requires an explicit audited remediation plan before retry; it is never silently carried into activation.

- [ ] **Step 5: Verify and commit**

```bash
.venv/bin/python -m pytest tests/unit/ingestion/test_normalization.py tests/unit/ingestion/test_models.py -q
TEST_POSTGRES_ADMIN_URL=postgresql://user:password@localhost:5432/postgres TEST_POSTGRES_ROLE_DDL=1 .venv/bin/python -m pytest tests/integration/ingestion -q
git add alembic/versions/20260713_0004_ingestion_policy_ignored.py src/ingestion src/domain/errors.py src/db tests/unit/ingestion tests/integration/ingestion
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
- Produces: `StaleFence(ErrorKind.INTERNAL_INVARIANT)`; `bootstrap(account_id, pipeline_name)`, `get(account_id, generation)`, `current_ingress(account_id)`, `assert_fence(account_id, generation, fencing_token)`, `can_execute(lease)`, `quiesce()`, `retire()` and transaction-local ownership primitives consumed only by Phase 3 activation

- [ ] **Step 1: Write current/draining/retired tests**

```python
@pytest.mark.integration
async def test_phase2_quiesce_never_creates_a_new_current_generation(ownership):
    old = await ownership.bootstrap(8, "legacy_compat")
    await ownership.quiesce(
        account_id=8,
        expected_generation=old.generation,
        expected_fencing_token=old.fencing_token,
        actor="test",
        reason="prepare cutover",
    )
    assert (await ownership.get(8, old.generation)).state is PipelineGenerationState.QUIESCING
    assert await ownership.current_ingress(8) is None
    assert await ownership.next_generation(8) == old.generation + 1


@pytest.mark.integration
async def test_quiesced_fence_cannot_claim_new_work(ownership, repo, inbox_lease):
    stale_lease = inbox_lease
    assert await repo.complete(stale_lease) is True
    await ownership.quiesce(
        8, stale_lease.generation, stale_lease.fencing_token, "test", "drain"
    )
    assert await repo.try_claim_new(8) is None
    assert await ownership.can_execute(stale_lease) is True
```

- [ ] **Step 2: Implement transactional generation changes**

Lock the current row with `FOR UPDATE`; compare expected generation/token. `quiesce()` is the only Phase-2 production transition from `current_ingress -> quiescing` and forbids new claims while existing stamped work finishes. Phase 2 exposes no public generation switch. The repository provides transaction-local insert/state primitives, but they require the caller's already-open unit of work and are reachable in production only from Phase 3 `ActivationService`, which atomically moves the old generation to draining and creates the new current generation together with authority/barrier/receipt/audit facts. `retire()` requires zero unresolved Inbox (`pending`, `retry_wait`, `leased`, `manual_review`), zero nonterminal or `send_unknown` Outbox, and matching high-water reconciliation. Historical terminal Inbox/Outbox rows remain append-only and may retain the ownership foreign key after retirement; `completed`/accounted dead-letter facts are not required to be deleted. `assert_fence()` is used by claim, effect-start and completion. Architecture tests fail if any Phase-2 runtime/CLI calls the transaction-local handoff primitives.

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
- Produces: `insert(event, generation, fencing_token) -> IngressReceipt`; `claim_batch(worker_id, pipeline_names, limit, lease_seconds) -> list[InboxLease]`; `renew(lease, lease_seconds) -> InboxLease | None`; repeatable `begin_effect(lease) -> bool`; `recover_expired_leases(limit) -> int`; `complete(lease) -> bool`; `fail(lease, error) -> InboxDisposition`; `stats() -> InboxStats`

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
    assert disposition.available_at > leased_event.received_at
    fifth = await repo.seed_lease(attempts=5)
    disposition = await repo.fail(fifth, TransientDependencyError("still temporary"))
    assert disposition.status == "dead_letter"


@pytest.mark.parametrize(
    "policy",
    [ProcessingPolicy.IGNORED, ProcessingPolicy.HISTORICAL_SUPPRESSED],
)
async def test_suppressed_insert_is_terminal_and_audited_exactly_once(repo, event, policy):
    suppressed = replace(event, processing_policy=policy)
    first = await repo.insert(suppressed, generation=1, fencing_token=1)
    duplicate = await repo.insert(suppressed, generation=1, fencing_token=1)
    assert await repo.status(first.inbox_id) == "completed"
    assert duplicate == IngressReceipt(inbox_id=first.inbox_id, duplicate=True)
    assert await repo.audit_count(first.inbox_id, "ingress.policy_suppressed") == 1


async def test_claim_defensively_excludes_anomalous_pending_suppressed_rows(repo):
    await repo.seed_pending_policy(ProcessingPolicy.IGNORED)
    await repo.seed_pending_policy(ProcessingPolicy.HISTORICAL_SUPPRESSED)
    assert await repo.claim_batch("worker", {"durable_v1"}, 10, 60) == []


@pytest.mark.integration
async def test_expired_lease_before_effect_is_recovered_with_bounded_retry(repo, clock):
    lease = await repo.seed_expired_lease(effect_started_at=None, attempts=1)
    assert await repo.recover_expired_leases(limit=10) == 1
    row = await repo.get(lease.id)
    assert row.status == "retry_wait"
    assert row.attempts == 2
    assert row.lease_owner is None


@pytest.mark.integration
async def test_expired_lease_after_effect_requires_manual_review(repo):
    lease = await repo.seed_expired_lease(effect_started_at="now")
    assert await repo.recover_expired_leases(limit=10) == 1
    assert (await repo.get(lease.id)).status == "manual_review"


@pytest.mark.integration
async def test_effect_start_and_expiry_recovery_cannot_both_win(repo, leased_event):
    started, recovered = await asyncio.gather(
        repo.begin_effect(leased_event),
        repo.recover_expired_leases(limit=10),
    )
    assert (started, recovered) in {(True, 0), (False, 1)}


@pytest.mark.integration
async def test_effect_guard_is_repeatable_without_replacing_first_marker(repo, leased_event):
    assert await repo.begin_effect(leased_event) is True
    first_marker = (await repo.get(leased_event.id)).effect_started_at
    assert await repo.begin_effect(leased_event) is True
    assert (await repo.get(leased_event.id)).effect_started_at == first_marker


@pytest.mark.integration
async def test_failure_after_effect_marker_is_never_automatically_retried(
    repo, leased_event
):
    assert await repo.begin_effect(leased_event) is True
    disposition = await repo.fail(
        leased_event, SyncTransientError("outcome unknown")
    )
    assert disposition.status == "manual_review"
    assert disposition.available_at is None
    assert await repo.audit_count(leased_event.id, "ingress.effect_unknown") == 1


@pytest.mark.integration
async def test_definite_failure_before_effect_marker_can_retry(repo, leased_event):
    disposition = await repo.fail(
        leased_event, SyncTransientError("definitely before effect")
    )
    assert disposition.status == "retry_wait"
```

- [ ] **Step 2: Implement claim SQL**

`insert()` recursively materializes the immutable payload only through `event.payload_for_storage()` before wrapping it in psycopg `Jsonb`; a real-PostgreSQL nested mapping/list round-trip test is mandatory. For `IGNORED` and `HISTORICAL_SUPPRESSED`, the first insert writes `status='completed'` plus one append-only `ingress.policy_suppressed` audit row in the same transaction. A duplicate dedupe key returns the existing receipt and never appends another audit row.

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
      AND e.processing_policy NOT IN ('ignored', 'historical_suppressed')
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

`complete` and `fail` update only where ID, `lease_owner`, generation and fencing token match and the ownership row still permits that exact generation. `fail` locks and reads the durable effect marker in the same CAS transaction: only `effect_started_at IS NULL` may enter bounded retry/dead-letter classification. Any failure after the marker defaults to `manual_review`, clears the lease, stores a fixed outcome-unknown code and appends one idempotent safe audit; it is never scheduled automatically. A future adapter may bypass that rule only with a typed, tested proof that the remote operation definitely did not execute or uses an approved stable idempotency key—generic timeout/transport exceptions are never such proof. The explicit policy predicate is a defense in depth: even a malformed legacy `pending` ignored/historical row is never claimable. Shadow comparison rows live outside this claim path; no `event_inbox.execution_mode` column exists.

Expired leases are durable work, not permanent `leased` rows. A bounded `FOR UPDATE SKIP LOCKED` reaper runs before claim cycles and on startup. If `effect_started_at IS NULL`, it clears owner/deadline, increments attempts once and applies the same capped retry/dead-letter policy as `fail()`. If the effect marker exists, it clears the lease into `manual_review` with a privacy-safe unknown-outcome code and one idempotent audit; it never retries. `renew()` is a capped CAS available for the worker's entire lease lifetime, including after the effect marker. `begin_effect()` is a repeatable, idempotent authority guard: every invocation atomically requires matching ID/owner, unexpired lease, generation/fence and `status='leased'`, records only the first marker with `COALESCE(effect_started_at, now())`, and returns false after reaping or fencing. Completion/failure use the same fence, so an old worker cannot act after a reaper or replacement worker wins.

- [ ] **Step 3: Implement error disposition**

Before the effect marker, retryable errors increment attempts and set `retry_wait` with exponential delay capped at 15 minutes. Authentication and invariant errors dead-letter immediately. Exceeding five attempts sets `dead_letter`, stores only safe code/summary, and appends audit. After the marker, all unproven outcomes instead return `InboxDispositionStatus.MANUAL_REVIEW`; `InboxStats`, repository SQL, `/queue`, metrics and drain/retire reporting expose `manual_review` as a first-class operator backlog.

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
- Create: `tests/integration/ingestion/test_email_event_concurrency.py`
- Create: `tests/architecture/test_email_state_repository_boundary.py`
- Create: `tests/architecture/test_phase2_delete_has_no_outbox_mutation.py`
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
    decision = decide_email_event(
        current_status=current,
        create_seen=current is not None,
        kind=ChangeKind(event),
        source_is_read=True,
    )
    assert decision.new_status == expected
    assert decision.should_process is should_process


@pytest.mark.parametrize(
    "current",
    [
        "ingested", "processing", "retry_wait", "manual_review",
        "waiting_approval", "notified_readonly", "send_queued", "sending",
        "accepted", "sent", "send_failed", "delivery_failed", "send_unknown",
        "no_action", "archived", "rejected", "draft_saved", "expired",
        "cancelled", "dead_letter",
    ],
)
def test_every_database_email_status_has_an_explicit_event_row(current):
    assert transition_table.has_explicit_row(current)


def test_delete_only_emits_phase3_cancellation_intent():
    decision = decide_email_event(
        current_status="waiting_approval",
        create_seen=True,
        kind=ChangeKind.DELETE,
        source_is_read=False,
    )
    assert decision.new_status == "cancelled"
    assert decision.cancel_pending_side_effects is True


def test_delete_after_external_effect_start_only_records_source_deletion():
    decision = decide_email_event(
        current_status="sending",
        create_seen=True,
        kind=ChangeKind.DELETE,
        source_is_read=False,
    )
    assert decision.new_status == "sending"
    assert decision.cancel_pending_side_effects is False


def test_unknown_delete_creates_terminal_tombstone_and_late_create_cannot_reopen():
    deleted = decide_email_event(
        current_status=None,
        create_seen=False,
        kind=ChangeKind.DELETE,
        source_is_read=False,
    )
    assert (deleted.new_status, deleted.should_process) == ("cancelled", False)
    late_create = decide_email_event(
        current_status="cancelled",
        create_seen=False,
        kind=ChangeKind.CREATE,
        source_is_read=False,
    )
    assert (late_create.new_status, late_create.should_process) == (
        "cancelled",
        False,
    )


@pytest.mark.parametrize("kind", [ChangeKind.UPDATE, ChangeKind.READ])
def test_unknown_update_or_read_creates_metadata_shell_without_effect(kind):
    decision = decide_email_event(
        current_status=None,
        create_seen=False,
        kind=kind,
        source_is_read=True,
    )
    assert decision.new_status == "ingested"
    assert decision.should_process is False
    assert decision.create_seen is False


@pytest.mark.integration
async def test_webhook_and_sync_create_elect_exactly_one_processor(repo):
    webhook = event(source="webhook", external_email_id="m1", dedupe_key="a" * 64)
    sync = event(source="sync", external_email_id="m1", dedupe_key="b" * 64)
    first, second = await asyncio.gather(
        repo.apply_email_event(webhook),
        repo.apply_email_event(sync),
    )
    assert sum(result.should_process for result in (first, second)) == 1
    assert await repo.email_count(account_id=8, external_email_id="m1") == 1
    assert await repo.create_seen_count(account_id=8, external_email_id="m1") == 1
    assert await repo.processing_election_count("m1") == 1
    assert {first.disposition, second.disposition} == {
        "creator_elected",
        "aggregate_noop",
    }


@pytest.mark.integration
async def test_delete_before_create_persists_tombstone(repo):
    await repo.apply_email_event(event(source="sync", kind="delete", external_email_id="m2"))
    late = await repo.apply_email_event(
        event(source="webhook", kind="create", external_email_id="m2")
    )
    row = await repo.email(account_id=8, external_email_id="m2")
    assert row.status == "cancelled"
    assert row.source_deleted_at is not None
    assert row.create_seen_at is None
    assert late.should_process is False


@pytest.mark.integration
@pytest.mark.parametrize("winner", ["delete", "effect_begin"])
async def test_delete_and_worker_effect_begin_have_one_serialized_winner(
    repo, worker, legacy_effects, legacy_external, winner
):
    lease = await repo.seed_create_elected()
    legacy_effects.install_begin_barrier(winner=winner)
    processing = asyncio.create_task(worker.process_lease(lease))
    await legacy_effects.begin_barrier_reached.wait()
    deleting = asyncio.create_task(
        repo.apply_email_event(event(
            source="sync", kind="delete", external_email_id=lease.external_email_id
        ))
    )
    if winner == "delete":
        deleted = await deleting
        legacy_effects.release_begin_barrier.set()
    else:
        legacy_effects.release_begin_barrier.set()
        await legacy_effects.execute_token_committed.wait()
        deleted = await deleting
    await processing
    if winner == "delete":
        assert deleted.cancel_pending_side_effects is True
        legacy_external.assert_no_calls()
    else:
        assert deleted.cancel_pending_side_effects is False
        assert legacy_external.completed_effect_count == 1
        assert await legacy_effects.try_begin_for_lease(
            lease, "lark_notification", 1, "next-target"
        ) is None
```

- [ ] **Step 2: Implement pure transition function and repository CAS**

Define the complete allowed-transition table for the exact database vocabulary: `ingested`, `processing`, `retry_wait`, `manual_review`, `waiting_approval`, `notified_readonly`, `send_queued`, `sending`, `accepted`, `sent`, `send_failed`, `delivery_failed`, `send_unknown`, `no_action`, `archived`, `rejected`, `draft_saved`, `expired`, `cancelled` and `dead_letter`. A manifest test fails if either the schema CHECK or transition table gains/loses a status independently. The decision takes explicit `create_seen`; only the first create sets `create_seen_at` and wins processing. An unknown update/read creates or updates an `ingested` metadata shell with `create_seen_at=NULL` and no effect, so a later first create may still process. An unknown delete atomically creates `status='cancelled'` plus `source_deleted_at` tombstone with no effect; every later create/update/read preserves that tombstone.

Webhook and Sync use transport-specific Inbox dedupe keys, so aggregate election must not rely on Inbox dedupe. In one short transaction, `apply_email_event()` executes `INSERT INTO emails ... ON CONFLICT (account_id,external_email_id) DO NOTHING RETURNING ...` (or an equivalent non-throwing election), then locks the elected/existing email row and CASes its version/create marker. The conflict loser waits for/reloads the committed row and returns a normal `aggregate_noop`; it never exposes `UniqueViolation`, retries the business event or runs a second effect. Create/update/read/delete all use this same aggregate identity and row-lock order. A real PostgreSQL `asyncio.gather` test with distinct Webhook/Sync dedupe keys proves one email, one `create_seen_at`, one `should_process`/processing election and a side-effect-free loser.

Phase 2 owns only the monotonic email decision and CAS: a delete sets `source_deleted_at` exactly once and returns `cancel_pending_side_effects=True` only when the current aggregate is still cancellable. It must not update a Notification/Mailbox/Send Outbox, mutate a card resource or call Lark—those relations and their race-safe cancellation transaction do not exist until Phase 3. Once external effects have started, delete preserves the current status, sets only `source_deleted_at` and returns `cancel_pending_side_effects=False`. `tests/architecture/test_phase2_delete_has_no_outbox_mutation.py` scans Phase 2 production code and forbids Outbox/card cancellation SQL or Lark invalidation calls. Phase 3 consumes the decision under the email/outbox row locks and owns the delete-versus-send-start race.

`manual_review`/`dead_letter` recovery requires an authenticated administrator reason and creates audit. Architecture test scans nodes/handlers and fails on direct `UPDATE emails SET status`; all mutations call the repository CAS.

- [ ] **Step 3: Verify and commit**

```bash
.venv/bin/python -m pytest tests/unit/ingestion/test_email_events.py tests/integration/ingestion/test_email_event_concurrency.py -q
git add src/ingestion/email_events.py src/ingestion/repository.py tests/unit/ingestion/test_email_events.py tests/integration/ingestion/test_email_event_concurrency.py tests/architecture/test_email_state_repository_boundary.py tests/architecture/test_phase2_delete_has_no_outbox_mutation.py
git commit -m "feat: enforce monotonic email event transitions"
```

---

### Task 6: Add the Typed Exchange Sync Client

**Files:**
- Create: `tests/unit/ingestion/test_exchange_sync_client.py`
- Create: `tests/contracts/test_exchange_sync_contract.py`
- Modify: `src/utils/exchange_api.py`
- Modify: `src/ingestion/normalization.py`
- Modify: `src/domain/errors.py`
- Modify: `tests/unit/test_exchange_api.py`
- Modify: `tests/unit/test_exchange_response_limits.py`
- Modify: `tests/unit/ingestion/test_normalization.py`

**Interfaces:**
- Consumes: Task 2 policy-free `validate_sync_change_contract(change) -> SyncChange`
- Produces: `ExchangeClient.sync_emails(account_id, folder, sync_state, limit) -> SyncBatch`; `validate_sync_permission(account_id, folder) -> None`; `SyncAuthorizationError`, `SyncCursorInvalidError`, `SyncTransientError`, `SyncContractError`

- [ ] **Step 1: Replace the obsolete “sync removed” assertion**

```python
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "configured_url",
    [
        "https://host/api/v1/exchange",
        "https://host/api/v1/exchange/emails",
    ],
)
async def test_sync_client_maps_server_contract(client, configured_url):
    client.api_url = configured_url
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/exchange/emails/sync"
        assert json.loads(request.content) == {
            "account_id": 8,
            "folder": "INBOX",
            "sync_state": "cursor-1",
            "limit": 500,
            "only_fields": ["id", "subject", "sender", "datetime_received", "is_read", "has_attachments"],
        }
        return httpx.Response(
            200,
            headers={"X-Exchange-Sync-Contract": "exchange_sync_contract_v2"},
            json={
                "data": {
                    "sync_state": "cursor-2",
                    "includes_last": True,
                    "items": [
                        {
                            "change_type": "create",
                            "id": "m1",
                            "item": {
                                "id": "m1",
                                "subject": "s",
                                "sender": "sender@example.com",
                                "received_time": "2026-07-13T10:00:00",
                                "is_read": False,
                                "has_attachments": False,
                            },
                        }
                    ],
                }
            },
        )
    client.replace_transport_for_test(httpx.MockTransport(handler))
    batch = await client.sync_emails(account_id=8, folder="INBOX", sync_state="cursor-1", limit=500)
    assert batch.cursor == "cursor-2"
    assert batch.changes[0].kind is ChangeKind.CREATE
    assert batch.contract_version == "exchange_sync_contract_v2"
    assert batch.includes_last is True


@pytest.mark.asyncio
async def test_sync_stream_rejects_body_before_response_buffer_exceeds_limit(client):
    client.replace_transport_for_test(streaming_json_larger_than_configured_limit())
    with pytest.raises(SyncContractError):
        await client.sync_emails(8, "INBOX", "cursor-1", 500)


@pytest.mark.asyncio
async def test_sync_rejects_limit_plus_one_items(client):
    client.replace_transport_for_test(sync_response_with_items(501))
    with pytest.raises(SyncContractError):
        await client.sync_emails(8, "INBOX", "cursor-1", 500)


@pytest.mark.asyncio
async def test_legacy_read_flag_change_is_fail_closed_until_v2_mapping(client):
    client.replace_transport_for_test(
        sync_response(
            contract_version=None,
            cursor="cursor-2",
            includes_last=True,
            items=[{"change_type": "read_flag_change", "id": "m1", "item": None}],
        )
    )
    with pytest.raises(SyncContractError):
        await client.sync_emails(8, "INBOX", "cursor-1", 500)


@pytest.mark.asyncio
async def test_empty_incremental_batch_accepts_new_opaque_cursor(client):
    client.replace_transport_for_test(
        sync_response(cursor="cursor-2", includes_last=True, items=[])
    )
    batch = await client.sync_emails(8, "INBOX", "cursor-1", 500)
    assert batch.cursor == "cursor-2"
    client.replace_transport_for_test(
        sync_response(cursor=None, includes_last=True, items=[])
    )
    with pytest.raises(SyncContractError):
        await client.sync_emails(8, "INBOX", "cursor-1", 500)


@pytest.mark.asyncio
async def test_sync_client_and_normalizer_share_policy_free_item_validator(
    client, monkeypatch
):
    validator = MagicMock(wraps=validate_sync_change_contract)
    monkeypatch.setattr(exchange_api, "validate_sync_change_contract", validator)
    client.replace_transport_for_test(sync_response_with_one_create())
    await client.sync_emails(8, "INBOX", "cursor-1", 500)
    validator.assert_called_once()


@pytest.mark.parametrize(
    "failure",
    [
        httpx.ReadTimeout("timeout"),
        httpx.ConnectError("disconnect"),
        httpx.Response(408),
        httpx.Response(429, headers={"Retry-After": "999999999"}),
        httpx.Response(500),
    ],
)
async def test_sync_network_and_server_failures_are_bounded_transient_errors(
    client, failure
):
    client.replace_transport_for_test(failing_transport(failure))
    with pytest.raises(SyncTransientError) as caught:
        await client.sync_emails(8, "INBOX", "secret-cursor", 500)
    assert "secret-cursor" not in repr(caught.value)
```

- [ ] **Step 2: Implement response and error classification**

Treat `EXCHANGE_API_URL` as either the Exchange API root (`https://host/api/v1/exchange`) or the legacy email-resource base (`https://host/api/v1/exchange/emails`). Normalize once to an `emails_base_url`: append `/emails` only when the configured path does not already end with that exact segment. POST to `{emails_base_url}/sync`, yielding exactly `/api/v1/exchange/emails/sync` in both supported shapes—never `/sync` and never `/emails/emails/sync`. Send exactly `account_id`, `folder`, `sync_state`, `limit`, and the six EWS `only_fields` asserted above. In particular, request `datetime_received`; never send the service response name `received_time` or the nonexistent `received_at` as an EWS field.

Receive Sync with `AsyncClient.stream()` and the existing bounded JSON reader before building one unbounded `bytes` or calling `response.json()`. Reject both an oversized declared `Content-Length` and an incremental body crossing `EXCHANGE_RESPONSE_MAX_BYTES`; never log or persist the raw response. Decode strict UTF-8 JSON with duplicate-key/non-finite rejection at every depth and require the documented top-level/data shapes. Production responses must authenticate `exchange_sync_contract_v2` through the versioned response header and contain a boolean `data.includes_last`; a missing/legacy/unknown version fails closed. Each response change mapping has exactly `change_type/id/item`; each create/update item has the exact six-field scalar manifest from Task 2, where the service output is `received_time` even though the EWS request field is `datetime_received`. The returned item count must be at most the requested `limit`; cursor remains a present, bounded, non-empty exact opaque token and every unmodified mapping must pass Task 2's policy-free `validate_sync_change_contract()` before the batch is returned. The client may not pre-coerce values, discard unknown keys, construct a looser private `SyncChange` or choose a policy. EWS SyncState is independent of the change count: an empty `items=[]` response may legally return either the same cursor or a new cursor, so equality with the requested cursor is never required. A missing or structurally invalid cursor is `SyncContractError`; trimming, Unicode normalization or any other client-side transformation is forbidden, and the exact token is returned unchanged for Task 7's transactional CAS. Oversize streams, `limit + 1`, duplicate keys, malformed 2xx or any shape/version violation map to privacy-safe `SyncContractError`, and the coordinator must not advance its cursor.

The read-only cross-repository audit proves the currently deployed extension/exchangelib v5.6.0 contract is **incompatible**, not merely unverified. First, the extension calls `list(folder.sync_items(max_changes_returned=limit, ...))`; exchangelib auto-pages the generator, so `limit` is an EWS page size rather than an HTTP total-result cap. A backlog above 500 can therefore build an unbounded server list and return more than the AI client's limit, after which the fail-closed client repeatedly blocks on the same cursor. Second, EWS emits `read_flag_change`; the extension currently returns that literal `change_type` with `item=None`, which violates the AI client's exact create/update/delete contract.

Record this as external dependency gate `exchange_sync_contract_v2` with current status/code `blocked_external_exchange_sync_contract` / `exchange_sync_contract_incompatible`. The future, separately authorized extension change must (a) return at most `limit` changes per HTTP call, a continuation SyncState that corresponds exactly to those returned changes, and authenticated `includes_last` semantics; and (b) map every read-flag change to a versioned complete representation—this plan's default requires `change_type='update'` with the exact six-field item and current `is_read`; an alternative ignore policy is admissible only if explicitly versioned, audited and approved before implementation. After that separate release, rerun a real probe with more than `limit` pending changes plus an actual read/unread transition, and seal evidence covering extension build, exchangelib version, contract version/profile, request limit, per-call count/bytes, continuation cursor, includes-last progression and mapped read result. Any legacy version, `limit + 1`, raw `read_flag_change`, inconclusive result or version drift keeps production Sync and `durable_active` blocked. The AI client remains bounded/fail-closed and this plan never modifies `/Users/jarod/Documents/exchange-feishu-extension`.

Map 401/403 to `SyncAuthorizationError`; map only the documented privacy-safe 400 invalid-state discriminator to `SyncCursorInvalidError`; map other 4xx responses to a permanent `SyncContractError`. HTTP 408, 429, every 5xx response, `httpx.TimeoutException` and `httpx.TransportError` map to `SyncTransientError` with a strictly bounded parsed `Retry-After` hint. Error instances/logs never retain or render response bodies, request URLs, mailbox IDs or cursor values. A permission probe uses `limit=1` and never writes the local cursor.

- [ ] **Step 3: Verify and commit**

```bash
.venv/bin/python -m pytest tests/unit/ingestion/test_exchange_sync_client.py tests/contracts/test_exchange_sync_contract.py tests/unit/ingestion/test_normalization.py tests/unit/test_exchange_api.py tests/unit/test_exchange_response_limits.py -q
git add src/utils/exchange_api.py src/ingestion/normalization.py src/domain/errors.py tests/unit/ingestion/test_exchange_sync_client.py tests/contracts/test_exchange_sync_contract.py tests/unit/ingestion/test_normalization.py tests/unit/test_exchange_api.py tests/unit/test_exchange_response_limits.py
git commit -m "feat: add typed Exchange sync client"
```

---

### Task 7: Implement Atomic Sync Coordinator and Cold-start Approval

**Files:**
- Create: `alembic/versions/20260713_0005_sync_reconciliation_control.py`
- Create: `src/ingestion/policy.py`
- Create: `src/ingestion/sync.py`
- Create: `src/ingestion/cold_start.py`
- Create: `src/ingestion/command_receipts.py`
- Create: `tests/unit/ingestion/test_policy.py`
- Create: `tests/unit/ingestion/test_sync_coordinator.py`
- Create: `tests/integration/ingestion/test_sync_atomicity.py`
- Create: `tests/unit/ingestion/test_cold_start.py`
- Create: `tests/integration/ingestion/test_command_receipts.py`
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
- Modify: `tests/integration/test_checkpoint_cleanup.py`

**Interfaces:**
- Produces: immutable `FolderScope`; fail-closed `ProcessingPolicyResolver`; append-only `CommandReceiptRepository`; `SyncCoordinator.run_folder()`; resumable `ColdStartService.preview(account_id, folder, *, actor, reason, idempotency_key)`, `approve(plan_id, *, actor, reason, idempotency_key)` and `apply(plan_id)`

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
async def test_advisory_lock_releases_on_cancellation(coordinator, lock_probe):
    task = asyncio.create_task(coordinator.run_folder(8, "INBOX"))
    await coordinator.exchange_call_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await lock_probe.can_acquire(8, "INBOX") is True


@pytest.mark.asyncio
async def test_missing_cursor_only_creates_cold_start_pending(coordinator, exchange, db):
    result = await coordinator.run_folder(8, "INBOX")
    assert result.status == "cold_start_pending"
    exchange.sync_emails.assert_not_awaited()
    assert await db.cursor_state(8, "INBOX") == "cold_start_pending"


@pytest.mark.parametrize(
    ("source", "raw_type", "kind", "folder", "expected"),
    [
        ("webhook", "NewMailEvent", "create", "INBOX_ID", "full"),
        ("sync", "create", "create", "INBOX", "full"),
        ("webhook", "CreatedEvent", "create", "SENT_ID", "archive"),
        ("sync", "create", "create", "SENT", "archive"),
        ("webhook", "CreatedEvent", "create", "DRAFTS_ID", "ignored"),
        ("sync", "create", "create", "DRAFTS", "ignored"),
        ("webhook", "ModifiedEvent", "update", "INBOX_ID", "metadata_only"),
        ("webhook", "DeletedEvent", "delete", "INBOX_ID", "metadata_only"),
        ("sync", "update", "update", "INBOX", "metadata_only"),
        ("sync", "delete", "delete", "INBOX", "metadata_only"),
    ],
)
def test_policy_matrix_is_equivalent_across_webhook_and_sync(
    resolver, source, raw_type, kind, folder, expected
):
    assert resolver.resolve(source, raw_type, kind, folder).value == expected


@pytest.mark.asyncio
async def test_page_budget_persists_each_batch_and_releases_lock(
    coordinator, exchange, db, lock_probe
):
    exchange.sync_emails.side_effect = [
        nonempty_batch("cursor-2", includes_last=False),
        nonempty_batch("cursor-3", includes_last=False),
    ]
    result = await coordinator.run_folder(8, "INBOX", max_pages=2)
    assert result.status == "budget_exhausted"
    assert await db.cursor_value(8, "INBOX") == "cursor-3"
    assert await lock_probe.can_acquire(8, "INBOX") is True


@pytest.mark.parametrize(
    ("error", "expected_state"),
    [
        (SyncCursorInvalidError("invalid"), "reset_required"),
        (SyncContractError("shape"), "blocked_contract"),
        (SyncAuthorizationError("forbidden"), "blocked_contract"),
        (SyncTransientError("unavailable"), "active"),
    ],
)
async def test_sync_errors_never_advance_cursor_and_persist_disposition(
    coordinator, exchange, db, error, expected_state
):
    exchange.sync_emails.side_effect = error
    await coordinator.run_folder(8, "INBOX")
    row = await db.cursor_row(8, "INBOX")
    assert row.cursor == "cursor-1"
    assert row.status == expected_state
    assert await db.safe_audit_count(error) == 1


async def test_nonempty_unchanged_cursor_blocks_contract_without_retry_loop(
    coordinator, exchange, db
):
    exchange.sync_emails.return_value = nonempty_batch(
        "cursor-1", includes_last=False
    )
    await coordinator.run_folder(8, "INBOX")
    assert await db.cursor_state(8, "INBOX") == "blocked_contract"
    await coordinator.run_folder(8, "INBOX")
    assert exchange.sync_emails.await_count == 1


async def test_empty_batch_atomically_advances_new_sync_state(
    coordinator, exchange, db
):
    exchange.sync_emails.return_value = empty_batch(
        cursor="cursor-2", includes_last=True
    )
    result = await coordinator.run_folder(8, "INBOX")
    assert result.status == "caught_up"
    assert await db.cursor_value(8, "INBOX") == "cursor-2"
    assert await db.inbox_count_for_last_sync_transaction() == 0


async def test_empty_nonterminal_v2_page_continues_from_committed_cursor(
    coordinator, exchange, db
):
    exchange.sync_emails.side_effect = [
        empty_batch(cursor="cursor-2", includes_last=False),
        empty_batch(cursor="cursor-3", includes_last=True),
    ]
    result = await coordinator.run_folder(8, "INBOX")
    assert result.status == "caught_up"
    assert exchange.sync_emails.await_args_list[1].args[2] == "cursor-2"
    assert await db.cursor_value(8, "INBOX") == "cursor-3"


async def test_approved_boundary_does_not_suppress_mail_arriving_after_preview(
    cold_start, coordinator, exchange, db, full_scope
):
    plan = await cold_start.preview(
        8,
        full_scope.canonical_key,
        actor="operator",
        reason="initial-history-review",
        idempotency_key="preview-1",
    )
    assert plan.boundary_cursor == "preview-boundary"
    exchange.append_change_after(plan.boundary_cursor, external_id="new-mail")
    await cold_start.approve(
        plan.id,
        actor="operator",
        reason="reviewed",
        idempotency_key="approve-1",
    )
    await cold_start.apply(plan.id)
    row = await db.inbox_by_external_id("new-mail")
    assert row.processing_policy == "full"
    assert await db.cursor_value(8, full_scope.canonical_key) != plan.boundary_cursor


async def test_unusable_approved_boundary_blocks_without_failing_open(
    cold_start, exchange, db
):
    plan = await cold_start.seed_approved(boundary_cursor="expired-boundary")
    exchange.sync_emails.side_effect = SyncCursorInvalidError("invalid")
    await cold_start.apply(plan.id)
    assert await db.plan_state(plan.id) == "blocked"
    assert await db.cursor_state(plan.account_id, plan.folder_key) == "blocked_contract"
    assert await db.inbox_count() == 0


async def test_cold_start_commands_are_idempotent_but_reject_key_reuse(
    cold_start
):
    kwargs = {
        "actor": "operator",
        "reason": "review-history",
        "idempotency_key": "preview-key",
    }
    first = await cold_start.preview(8, "INBOX", **kwargs)
    assert await cold_start.preview(8, "INBOX", **kwargs) == first
    with pytest.raises(IdempotencyConflict):
        await cold_start.preview(8, "INBOX", **{**kwargs, "reason": "different"})


@pytest.mark.integration
async def test_approve_commit_unknown_replays_append_only_receipt(
    cold_start, receipts, fault
):
    plan = await cold_start.seed_ready()
    fault.lose_ack_after_commit("cold_start.approve")
    with pytest.raises(CommitAcknowledgementLost):
        await cold_start.approve(
            plan.id,
            actor="operator",
            reason="reviewed",
            idempotency_key="approve-commit-unknown",
        )
    replay = await cold_start.approve(
        plan.id,
        actor="operator",
        reason="reviewed",
        idempotency_key="approve-commit-unknown",
    )
    assert replay.status == "approved"
    assert await receipts.count("cold_start.approve", "approve-commit-unknown") == 1


@pytest.mark.integration
async def test_preview_contract_failure_blocks_without_sealing_boundary(
    cold_start, exchange, db
):
    exchange.sync_emails.side_effect = SyncContractError("invalid empty cursor")
    plan = await cold_start.preview(
        8,
        "INBOX",
        actor="operator",
        reason="review-history",
        idempotency_key="preview-contract-failure",
    )
    assert plan.status == "blocked"
    assert plan.boundary_cursor is None
    assert await db.cursor_state(8, "INBOX") == "blocked_contract"


@pytest.mark.asyncio
async def test_cold_preview_empty_batch_seals_only_exact_opaque_boundary(
    cold_start, exchange
):
    exchange.sync_emails.return_value = empty_batch(
        cursor="opaque+Boundary/%3D", includes_last=True
    )
    plan = await cold_start.preview(
        8,
        "INBOX",
        actor="operator",
        reason="review-history",
        idempotency_key="preview-empty-exact",
    )
    assert plan.boundary_cursor == "opaque+Boundary/%3D"


@pytest.mark.asyncio
async def test_later_preview_empty_batch_may_advance_boundary(
    cold_start, exchange
):
    plan = await cold_start.seed_previewing(preview_cursor="cursor-1")
    exchange.sync_emails.return_value = empty_batch(
        cursor="cursor-2", includes_last=True
    )
    sealed = await cold_start.resume(plan.id)
    assert sealed.boundary_cursor == "cursor-2"
```

- [ ] **Step 2: Implement lock/fetch/commit loop**

Define one immutable, versioned `FolderScope(canonical_key, webhook_ids, sync_folder, event_policy_matrix, config_hash)` manifest per account. Canonical keys, Sync folders and every opaque Webhook folder ID must be unique; ambiguous/duplicate definitions make the policy snapshot unready. Folder identity alone never authorizes full processing. The shared resolver is `resolve(source, raw_event_type, change_kind, exact_folder_identity, snapshot) -> ProcessingPolicy` and its hashed matrix covers both transports: incoming Webhook `NewMailEvent` and Sync `create` use the scope's explicit `FULL`/`ARCHIVE` rule; Webhook `CreatedEvent` plus Sync create in Sent map to `ARCHIVE`; Drafts and other create shapes map to `IGNORED`; Modified/Delete and Sync update/delete/read-state changes are `METADATA_ONLY`, never a fresh model/notification run. Cross-transport tests lock these equivalences for inbox, archive, sent, drafts, update, delete and read-state cases.

The same `ProcessingPolicyResolver` drives Webhook and Sync, scheduler iteration and a permission probe for every configured account/folder. A genuinely unknown folder in a loaded, successfully refreshed, unambiguous snapshot maps to `IGNORED` so it can receive a terminal durable receipt. A missing, failed or ambiguous snapshot raises `PolicySnapshotUnavailableError`: Sync leaves the cursor unchanged and readiness false, while Webhook later maps it to 503. There is no default-FULL or stale-refresh fallback.

Derive two stable signed int32 lock keys from SHA-256 of `account_id + NUL + canonical folder`; never use Python `hash()`. `SyncCoordinator` receives a dedicated bounded Sync PostgreSQL pool and nonblocking folder-concurrency permit from Task 11; it never checks out the Webhook/Worker pool. Check out one Sync-pool connection for the entire run and acquire session `pg_try_advisory_lock(key1, key2)`. End database transactions before each Exchange HTTP call but keep that connection checked out. Each short transaction locks/compares the old cursor, inserts normalized Inbox rows using the resolver's explicit policy and advances the cursor atomically. A missing cursor row may only be inserted as `cold_start_pending` and returned without an Exchange request; `sync_state=None` is reserved for a bound ColdStart plan. In `finally`, execute `pg_advisory_unlock(key1, key2)` on the same connection before returning it to the Sync pool and releasing the concurrency permit; cancellation tests prove all three resources are released.

Every invocation has both `max_pages` and `max_run_seconds`. Each v2 page atomically commits its Inbox rows (possibly zero), exact returned cursor and `includes_last` observation before another network call. Only authenticated `includes_last=True` ends a caught-up run; an empty `includes_last=False` page still CASes the returned SyncState and continues within the page/time budget. Exhausting either budget returns `budget_exhausted`, releases the session lock/connection, and lets the next scheduled run resume from that durable cursor. A nonterminal page with an unchanged cursor is a contract invariant, not a retryable exception.

Persist error disposition in the same short CAS transaction while preserving the old cursor. `SyncCursorInvalidError` sets `reset_required`. A legacy/missing v2 contract, malformed/oversized 2xx, `limit + 1`, raw `read_flag_change`, a nonterminal non-advancing cursor or another `SyncContractError` sets `blocked_contract` with a bounded deterministic fingerprint, `blocked_at` and one audit; scheduler skips it until an explicit repair command. Authorization failure also blocks the account/folder readiness and records a safe audit, never polling every five minutes. A transient 429/5xx leaves status/cursor active, increments durable `transient_failures`, sets capped-jitter `retry_after_at`, and resets those fields on success. No error stores raw response data.

- [ ] **Step 3: Implement cold-start preview**

Migration `20260713_0005` is linear from `20260713_0004`. It creates `sync_cold_start_plans` with UUID plan ID, account/folder, expected production-cursor state/version, pipeline name plus ownership generation/fence, state (`previewing/ready/approved/completed/blocked/cancelled`), preview/boundary cursors, rolling page hash/count, bounded redacted samples, contract/FolderScope config fingerprints, plan hash, actor/reason, expiry and timestamps. The later `0007` revision adds the activation-only `target_reservation_id` FK; `0005` does not use an unverified free-form target build/fence. A partial unique index permits only one open plan per account/folder, and `ready -> approved` is a one-time CAS; an approved plan remains approved across retryable apply failures until the single atomic `completed` transition. It also creates generic append-only `pipeline_command_receipts(id, account_id, command_name, idempotency_key_hash, canonical_payload_hash, outcome, result_type, result_id, result_hash, authority_epoch, created_at)` with unique `(command_name,idempotency_key_hash)`, bounded fields and a trigger rejecting UPDATE/DELETE/TRUNCATE, and adds nonnegative `transient_failures` plus `retry_after_at` to `sync_cursors`. Runtime and maintenance may INSERT/SELECT receipts for commands whose state transition they already own, but neither may UPDATE/DELETE/TRUNCATE or insert a receipt for another role's command namespace; auditor is SELECT-only. Real-role tests cover both allowed namespaces and every cross-role/mutation denial. Exact schema/digest, all four ACL roles, checkpoint revision allowlists, offline SQL and a code-first real-PostgreSQL `0004 -> 0005` bridge are mandatory before the Sync candidate is eligible for later Phase-3 activation; Phase 2 still does not enable production Sync writes.

Preview alone may walk from `sync_state=None`. Each bounded invocation holds the same account/folder session advisory lock, but never changes production cursor or Inbox; after each authenticated v2 page it atomically advances the plan's preview cursor, rolling hash/count and bounded redacted samples, then yields and releases the lock on the same page/time budgets. A page may equal or differ from the requested preview cursor and is stored byte-for-code-point unchanged because EWS may advance SyncState with zero changes. Missing/legacy contract version, invalid cursor, raw read flag or bad `includes_last` raises `SyncContractError`; client trimming/normalization is forbidden and covered by preservation tests. Authorization, cursor or contract errors atomically set the plan and production cursor to `blocked_contract`, leave `boundary_cursor` unset and can never seal `ready`. Only transient errors leave the plan resumable. A valid page seals the exact returned token as terminal `boundary_cursor` only when `includes_last=True`, whether that last page is empty or non-empty; `includes_last=False` always remains previewing. Cursor, contract/profile fingerprint and immutable `plan_hash` seal in one transaction. The approved plan hash/count/samples plus audit are the aggregate historical-suppression fact; individual historical changes never become Inbox rows. Counts, samples, boundary and this limitation are shown before approval.

`preview(..., *, actor, reason, idempotency_key)` and `approve(plan_id, *, actor, reason, idempotency_key)` require bounded non-empty operator context. Every operator command first canonicalizes its complete non-secret payload. The state transition and immutable command receipt are committed in the same transaction: repeating the same key and payload returns the stored receipt/result without rerunning work, while reusing a key with any different payload raises `IdempotencyConflict`. A fault between state and receipt insertion rolls back both; a lost commit acknowledgement is recovered by reading the receipt. Task 10/11 reuse this exact repository for `plan`, `quiesce`, `ready` and Shadow switching, and Phase 3 reuses it for `switch`/`rollback`. Approval verifies expiry, expected production cursor state/version, ownership fence, current FolderScope config hash and plan hash, writes one authorization audit and CASes `ready -> approved`; it does not call Exchange inside the transaction. `apply()` is idempotent and holds the account/folder session advisory lock for its whole attempt. Before any HTTP call it reads the plan: `completed` returns the recorded result without another request; only unexpired `approved` with the same config/fence and expected cold/reset cursor may continue. It requests the first normal incremental batch *from the approved boundary*, never from `None`.

Only a valid v2 response may enter the final transaction. That transaction CASes the `cold_start_pending`/`reset_required` cursor at its expected version, inserts the first post-boundary batch using the current event-aware FolderScope policy and the returned batch cursor in dedupe identity, advances production directly to that returned cursor, then records plan `completed` plus audit only when `includes_last=True`; otherwise it persists the page and remains in bounded apply. A valid empty terminal response activates the boundary itself. Mail arriving between preview and approval is therefore processed normally rather than suppressed. `SyncCursorInvalidError`, legacy/invalid contract, malformed/oversized response, nonterminal unchanged cursor, config/fence/cursor drift or expiry sets plan/cursor `blocked_contract` without installing an invalid boundary. Transient failure leaves the plan `approved` for bounded retry. If commit acknowledgement is lost, the next attempt must read the completed/page receipt before deciding whether any HTTP is allowed.

Cutover preparation uses a stricter activation-target form of the same plan. Phase 3 must first call `prepare_target()` to persist one zero-work reservation; every configured `FolderScope` then has either an already-active cursor sealed against that reservation or a pre-switch `approved` cold-start boundary whose non-null `target_reservation_id` FK fixes the exact target pipeline/generation/fence/build/config. Activation-target `apply()` is forbidden before the authority switch and, immediately after the switch, starts from the sealed boundary—never from `None`—before the ordinary five-minute scheduler is enabled. The live barrier stores the complete folder-key-to-cursor-or-plan manifest, reservation ID and rolling hash. Missing scope entries/FKs, `cold_start_pending`, `reset_required`, `previewing`, `ready` but unapproved, `blocked`, `cancelled`, expired plans, reservation drift, or build/config/fence mismatch block production activation. A cutover integration test previews every scope, injects mail after preview but before switch, then proves controlled post-switch apply emits that mail with its normal `FULL` policy rather than historical suppression.

Task 2 deliberately emits `source_event_at=None` for Sync, so the preview cannot make per-item time claims. The reviewed aggregate plan is the terminal historical-suppression fact; no preview item runs model, Feishu, Exchange mutation or Qdrant, and no official run ever replays from `None`.

- [ ] **Step 4: Verify and commit**

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/unit/ingestion/test_policy.py tests/unit/ingestion/test_sync_coordinator.py tests/integration/ingestion/test_sync_atomicity.py tests/unit/ingestion/test_cold_start.py tests/integration/ingestion/test_command_receipts.py tests/unit/test_database_revision.py tests/unit/test_database_schema_contract.py tests/unit/test_checkpoint_repository_safety.py tests/unit/test_alembic_offline.py tests/integration/ingestion/test_schema.py tests/integration/ingestion/test_access_roles.py tests/integration/test_checkpoint_cleanup.py -q
git add alembic/versions/20260713_0005_sync_reconciliation_control.py src/ingestion/policy.py src/ingestion/sync.py src/ingestion/cold_start.py src/ingestion/command_receipts.py src/db/access_contract.py src/db/bootstrap.py src/db/schema.py src/db/schema_contract.py src/maintenance/checkpoint_repository.py tests/unit/ingestion/test_policy.py tests/unit/ingestion/test_sync_coordinator.py tests/integration/ingestion/test_sync_atomicity.py tests/unit/ingestion/test_cold_start.py tests/integration/ingestion/test_command_receipts.py tests/unit/test_database_revision.py tests/unit/test_database_schema_contract.py tests/unit/test_checkpoint_repository_safety.py tests/unit/test_alembic_offline.py tests/integration/ingestion/test_schema.py tests/integration/ingestion/test_access_roles.py tests/integration/test_checkpoint_cleanup.py
git commit -m "feat: add atomic sync reconciliation and cold start"
```

---

### Task 8: Add Fixed Durable Worker and Stamped Processing Adapter Boundary

**Files:**
- Create: `src/ingestion/processing.py`
- Create: `src/ingestion/legacy_adapter.py`
- Create: `src/ingestion/worker.py`
- Create: `tests/unit/ingestion/test_worker.py`
- Create: `tests/integration/ingestion/test_webhook_crash_recovery.py`
- Modify: `src/exchange_service.py`
- Modify: `tests/unit/test_exchange_service_refactor.py`

**Interfaces:**
- Consumes: `ProcessingOutcome`, Inbox leases, Phase 1 ContentStore
- Produces: `ProcessingAdapter`, `ProcessingAdapterRouter.select(stamped_lease, authority)`; `DurableInboxWorker.start()`, `run_once()`, `stop(grace_seconds)`

- [ ] **Step 1: Write fixed-count and recovery tests**

```python
@pytest.mark.asyncio
async def test_worker_has_fixed_consumer_count(worker):
    await worker.start()
    assert len(worker.tasks) == worker.concurrency
    await worker.stop(grace_seconds=1.0)


@pytest.mark.asyncio
async def test_durable_stamp_never_falls_back_to_legacy_adapter(
    worker, inbox, legacy_adapter
):
    lease = await inbox.seed_lease(
        pipeline_name="durable_candidate", generation=5, fencing_token=50
    )
    with pytest.raises(ProcessingAdapterUnavailable):
        await worker.process_lease(lease)
    legacy_adapter.process.assert_not_awaited()


@pytest.mark.integration
async def test_crash_before_effect_is_recovered_after_restart(
    worker, restart_worker, inbox, crash, clock
):
    event = await inbox.seed_pending()
    crash.before_external_effect = True
    with pytest.raises(SimulatedProcessCrash):
        await worker.run_once("worker-a")
    clock.advance(seconds=61)
    await (await restart_worker()).recover_expired_leases()
    assert (await inbox.get(event.id)).status == "retry_wait"


@pytest.mark.integration
async def test_crash_after_effect_marker_requires_manual_review_after_restart(
    worker, restart_worker, inbox, crash, clock
):
    event = await inbox.seed_pending()
    crash.after_external_effect_marker = True
    with pytest.raises(SimulatedProcessCrash):
        await worker.run_once("worker-a")
    clock.advance(seconds=61)
    await (await restart_worker()).recover_expired_leases()
    row = await inbox.get(event.id)
    assert row.status == "manual_review"
    assert row.effect_started_at is not None


async def test_lost_begin_effect_cas_stops_old_worker(
    worker, inbox, legacy_adapter
):
    stale = await inbox.seed_recovered_old_lease()
    await worker.process_lease(stale)
    legacy_adapter.external_effect.assert_not_awaited()


@pytest.mark.integration
async def test_heartbeat_keeps_long_marked_effect_leased(
    worker, inbox, legacy_adapter, clock
):
    lease = await inbox.seed_lease()
    legacy_adapter.block_after_first_effect = True
    task = asyncio.create_task(worker.process_lease(lease))
    await legacy_adapter.first_effect_started.wait()
    clock.advance(seconds=61)
    await worker.heartbeat_once(lease)
    assert await inbox.recover_expired_leases(limit=10) == 0
    legacy_adapter.release_first_effect.set()
    await task


@pytest.mark.integration
async def test_lost_heartbeat_after_marker_blocks_every_later_effect(
    worker, inbox, legacy_adapter, clock
):
    lease = await inbox.seed_lease()
    legacy_adapter.block_after_first_effect = True
    task = asyncio.create_task(worker.process_lease(lease))
    await legacy_adapter.first_effect_started.wait()
    worker.fail_next_heartbeat()
    clock.advance(seconds=61)
    assert await inbox.recover_expired_leases(limit=10) == 1
    legacy_adapter.release_first_effect.set()
    await task
    assert (await inbox.get(lease.id)).status == "manual_review"
    legacy_adapter.second_external_effect.assert_not_awaited()


async def test_timeout_after_effect_marker_goes_manual_and_stops_chain(
    worker, inbox, legacy_adapter
):
    lease = await inbox.seed_lease()
    legacy_adapter.first_external_effect.side_effect = httpx.ReadTimeout("unknown")
    await worker.process_lease(lease)
    assert (await inbox.get(lease.id)).status == "manual_review"
    legacy_adapter.second_external_effect.assert_not_awaited()


@pytest.mark.parametrize(
    ("policy", "allowed_effects"),
    [
        (
            ProcessingPolicy.FULL,
            {"detail", "content", "model", "feishu", "exchange_mutation", "qdrant"},
        ),
        (ProcessingPolicy.ARCHIVE, {"detail", "content", "qdrant"}),
        (ProcessingPolicy.METADATA_ONLY, set()),
        (ProcessingPolicy.HISTORICAL_SUPPRESSED, set()),
        (ProcessingPolicy.IGNORED, set()),
    ],
)
async def test_processing_policy_has_exact_external_effect_ceiling(
    worker, inbox, effect_probe, policy, allowed_effects
):
    lease = await inbox.seed_lease(processing_policy=policy)
    await worker.process_lease(lease)
    assert effect_probe.calls == allowed_effects


@pytest.mark.parametrize(
    "policy",
    [ProcessingPolicy.IGNORED, ProcessingPolicy.HISTORICAL_SUPPRESSED],
)
async def test_worker_noops_suppressed_lease_without_external_effects(
    worker, inbox, legacy_adapter, policy
):
    lease = await inbox.seed_anomalous_lease(processing_policy=policy)
    await worker.process_lease(lease)
    legacy_adapter.process.assert_not_awaited()
    assert (await inbox.get(lease.id)).status == "completed"
```

- [ ] **Step 2: Implement stamped adapter selection and compatibility adapter**

Define a typed `ProcessingAdapter` protocol and make `DurableInboxWorker` resolve it from the immutable lease `pipeline_name/generation/fencing_token` plus the freshly read database authority; the Worker must never instantiate or call `LegacyProcessingAdapter` directly. Phase 2 registers only `legacy_compat`: it is selectable for an exactly stamped legacy-authoritative/Shadow item or an already-owned draining item, and every outbound boundary remains subject to the Task-10 `LegacyEffectGuard`. The `durable_candidate` registry slot is deliberately unbound in this phase, so target standby claims zero work and a durable-stamped test lease fails closed instead of falling back to the legacy monolith. Phase 3 must supply `DurableProcessingAdapter`; Phase 4 replaces its candidate Graph/projection ports without changing this stamped selection contract.

Before constructing or invoking the compatibility adapter, the worker applies an explicit five-policy matrix. `FULL` may fetch detail, persist governed content, run the model, write Qdrant, notify through Feishu and mutate Exchange only as allowed by the event decision. `ARCHIVE` fetches detail and governed content, then calls `process_and_archive_email(..., skip_analysis=True)` so Qdrant is its only external mutation: no model, Feishu or Exchange mutation. `METADATA_ONLY` applies only the durable email projection/status/audit from the normalized envelope; it never fetches detail, stores body/attachments, invokes the legacy monolith, or calls model, Feishu, Exchange or Qdrant. `IGNORED` and `HISTORICAL_SUPPRESSED` are idempotent terminal no-ops with the same zero-effect ceiling; an anomalous pre-leased row is completed safely and uses the same unique suppression-audit identity rather than appending a duplicate. Parameterized tests drive a success branch for each policy and assert both required calls and every prohibited call.

For executable work, the adapter applies the event state decision and invokes the legacy path only when `should_process=True`. A dedicated heartbeat task starts with the lease and continues at a bounded interval through pre-effect work, `effect_started_at`, all in-flight outbound calls and the terminal repository CAS. Losing `renew()` authority sets a cancellation signal immediately; an already in-flight call is treated as outcome-unknown, and no later call may begin. Immediately before every outbound dependency/effect boundary, the adapter must win the repeatable repository `begin_effect(lease)` guard; a false result stops execution. Because the existing `process_and_archive_email()` is a monolith, this task must either split its outbound boundaries or inject and await a typed `before_external_effect(kind)` guard immediately before ContentStore, model, Feishu, Exchange and Qdrant calls—the worker may not wrap only the outer function. A real process crash is recovered only after lease expiry: before-marker work retries boundedly, while after-marker work becomes `manual_review`, never blind retry. An in-process timeout or transport error after the marker follows the same `manual_review` path immediately and must not call the normal retry branch or start a later effect. A healthy heartbeat prevents a long marked operation from being reaped; a lost heartbeat permits the reaper to move it to `manual_review` and the guard prevents every subsequent side effect.

- [ ] **Step 3: Verify and commit**

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/unit/ingestion/test_worker.py tests/integration/ingestion/test_webhook_crash_recovery.py tests/unit/test_exchange_service_refactor.py -q
git add src/ingestion/processing.py src/ingestion/legacy_adapter.py src/ingestion/worker.py src/exchange_service.py tests/unit/ingestion/test_worker.py tests/integration/ingestion/test_webhook_crash_recovery.py tests/unit/test_exchange_service_refactor.py
git commit -m "feat: add recoverable durable inbox workers"
```

---

### Task 9: Build Dormant Durable 202 Intake and Add Shadow Mode

**Files:**
- Create: `alembic/versions/20260713_0006_shadow_inputs.py`
- Create: `src/ingestion/webhook.py`
- Create: `src/ingestion/shadow.py`
- Create: `src/ingestion/shadow_decision.py`
- Create: `tests/contracts/test_shadow_decision_contract.py`
- Create: `tests/integration/ingestion/test_shadow_intake.py`
- Modify: `src/server.py:33-180`
- Modify: `src/ingestion/normalization.py`
- Modify: `src/ingestion/policy.py`
- Modify: `src/ingestion/models.py`
- Modify: `src/db/access_contract.py`
- Modify: `src/db/bootstrap.py`
- Modify: `src/db/schema.py`
- Modify: `src/db/schema_contract.py`
- Modify: `src/maintenance/checkpoint_repository.py`
- Modify: `tests/unit/test_exchange_webhook.py`
- Modify: `tests/unit/test_event_routing.py`
- Modify: `tests/unit/ingestion/test_normalization.py`
- Modify: `tests/unit/ingestion/test_policy.py`
- Modify: `tests/unit/test_database_revision.py`
- Modify: `tests/unit/test_database_schema_contract.py`
- Modify: `tests/unit/test_checkpoint_repository_safety.py`
- Modify: `tests/unit/test_alembic_offline.py`
- Modify: `tests/integration/ingestion/test_schema.py`
- Modify: `tests/integration/ingestion/test_access_roles.py`
- Modify: `tests/integration/test_checkpoint_cleanup.py`

**Interfaces:**
- Consumes: `IngressService.accept(*, raw_body: bytes, payload: Mapping[str, Any], header_event: str | None) -> IngressReceipt | ShadowIngressReceipt`; Task 7 `ProcessingPolicyResolver`
- Produces: injected `IngressService`; typed active/shadow receipts; deterministic `ShadowDecisionV1`; dormant 202 after active Inbox commit; legacy-authoritative Shadow response semantics

```python
@dataclass(frozen=True, slots=True)
class ShadowIngressReceipt:
    comparison_id: str
    duplicate: bool

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "comparison_id",
            _require_uuid("comparison_id", self.comparison_id),
        )
        _require_bool("duplicate", self.duplicate)


type IngressAcceptance = IngressReceipt | ShadowIngressReceipt


@dataclass(frozen=True, slots=True)
class ShadowDecisionV1:
    schema_version: Literal[1]
    event_key: str
    change_kind: ChangeKind
    processing_policy: ProcessingPolicy
    should_process: bool
    target_email_status: str
    cancel_pending_side_effects: bool
    reason_code: str
    normalizer_contract_hash: str
    policy_config_hash: str
```

`IngressReceipt.inbox_id` remains truthful and active-only. Shadow returns `ShadowIngressReceipt.comparison_id`; it must never forge an Inbox ID. The configured route mode narrows the union before selecting the active 202 response or continuing the legacy-authoritative shadow response.

- [ ] **Step 1: Write request-boundary tests**

```python
def test_webhook_returns_202_only_after_commit(client, ingress):
    ingress.accept.return_value = IngressReceipt(
        inbox_id="00000000-0000-4000-8000-000000000001",
        duplicate=False,
    )
    response = post_signed_webhook(client, EVENT)
    assert response.status_code == 202
    ingress.accept.assert_awaited_once()


def test_webhook_storage_failure_returns_503(client, ingress):
    ingress.accept.side_effect = DatabaseOperationError(
        operation="insert_inbox", retryable=True, message="database unavailable"
    )
    assert post_signed_webhook(client, EVENT).status_code == 503


def test_unready_policy_snapshot_returns_503_without_inbox_write(
    client, policy_resolver, inbox
):
    policy_resolver.mark_refresh_failed()
    response = post_signed_webhook(client, EVENT)
    assert response.status_code == 503
    assert inbox.count() == 0


def test_ready_unknown_folder_is_durably_ignored(client, inbox):
    response = post_signed_webhook(client, event_for_folder("unknown-folder-id"))
    assert response.status_code == 202
    row = inbox.only_row()
    assert row.processing_policy == "ignored"
    assert row.status == "completed"


def test_policy_snapshot_recovery_allows_exchange_retry(
    client, policy_resolver, inbox
):
    policy_resolver.mark_refresh_failed()
    assert post_signed_webhook(client, EVENT).status_code == 503
    policy_resolver.install(READY_FOLDER_SCOPE_SNAPSHOT)
    assert post_signed_webhook(client, EVENT).status_code == 202
    assert inbox.count() == 1


@pytest.mark.integration
async def test_shadow_input_survives_post_commit_crash_and_replays(
    shadow_ingress, restart_shadow_evaluator, raw_body, payload, event
):
    receipt = await shadow_ingress.accept(
        raw_body=raw_body,
        payload=payload,
        header_event="NewMailEvent",
    )
    evaluator = await restart_shadow_evaluator()
    replayed = await evaluator.claim(receipt.comparison_id)
    assert replayed.normalized_event == event
    assert await evaluator.event_inbox_count(event.dedupe_key) == 0


@pytest.mark.integration
async def test_shadow_does_not_consume_active_global_dedupe(
    shadow_ingress, active_ingress, raw_body, payload
):
    arguments = {
        "raw_body": raw_body,
        "payload": payload,
        "header_event": "NewMailEvent",
    }
    await shadow_ingress.accept(**arguments)
    active = await active_ingress.accept(**arguments)
    assert active.duplicate is False


@pytest.mark.integration
@pytest.mark.parametrize("change_input", [False, True])
async def test_concurrent_shadow_retries_are_first_write_wins_without_orphans(
    shadow_ingress, raw_body, payload, db, change_input
):
    retry_payload = (
        {**payload, "delivery_attempt": 2} if change_input else payload
    )
    first, retry = await asyncio.gather(
        shadow_ingress.accept(
            raw_body=raw_body,
            payload=payload,
            header_event="NewMailEvent",
        ),
        shadow_ingress.accept(
            raw_body=encode_signed(retry_payload),
            payload=retry_payload,
            header_event="NewMailEvent",
        ),
    )
    assert {first.duplicate, retry.duplicate} == {False, True}
    assert first.comparison_id == retry.comparison_id
    assert await db.shadow_input_count_for(first.comparison_id) == 1
    assert await db.shadow_comparison_count_for(first.comparison_id) == 1


@pytest.mark.integration
async def test_shadow_evaluator_claims_are_disjoint_and_fenced(
    shadow_repo_a, shadow_repo_b, pending_comparisons
):
    first, second = await asyncio.gather(
        shadow_repo_a.claim_batch("evaluator-a", limit=10, lease_seconds=30),
        shadow_repo_b.claim_batch("evaluator-b", limit=10, lease_seconds=30),
    )
    assert {row.id for row in first}.isdisjoint({row.id for row in second})


@pytest.mark.integration
async def test_shadow_evaluator_crash_is_recovered_with_bounded_attempts(
    shadow_repo, clock
):
    lease = await shadow_repo.seed_claimed_pending(attempts=1)
    clock.advance(seconds=31)
    assert await shadow_repo.recover_expired(limit=10) == 1
    row = await shadow_repo.get(lease.id)
    assert row.shadow_status == "pending"
    assert row.shadow_lease_owner is None
    assert row.shadow_attempts == 2


@pytest.mark.integration
async def test_crash_after_shadow_commit_before_legacy_does_not_suppress_authority(
    shadow_route, legacy, crash, raw_body, payload
):
    crash.after_shadow_commit_before_legacy = True
    with pytest.raises(SimulatedProcessCrash):
        await shadow_route.accept(raw_body=raw_body, payload=payload)
    receipt = await shadow_route.accept(raw_body=raw_body, payload=payload)
    assert receipt.shadow.duplicate is True
    legacy.process.assert_awaited_once()


@pytest.mark.integration
async def test_shadow_duplicate_or_failure_never_masks_legacy_failure(
    shadow_route, legacy, raw_body, payload
):
    await shadow_route.seed_existing_failed_comparison(raw_body, payload)
    legacy.process.side_effect = LegacyAuthoritativeUnavailable("retry")
    with pytest.raises(LegacyAuthoritativeUnavailable):
        await shadow_route.accept(raw_body=raw_body, payload=payload)
    assert await shadow_route.legacy_status(raw_body) != "completed"


def test_shadow_decision_v1_hash_is_literal_and_versioned():
    decision = ShadowDecisionV1(
        schema_version=1,
        event_key="a" * 64,
        change_kind=ChangeKind.CREATE,
        processing_policy=ProcessingPolicy.FULL,
        should_process=True,
        target_email_status="ingested",
        cancel_pending_side_effects=False,
        reason_code="first_create",
        normalizer_contract_hash="b" * 64,
        policy_config_hash="c" * 64,
    )
    assert canonical_shadow_decision_hash(decision) == (
        "0bac2b7c1b745d8607e4a28000ad512000548ac515bb45dc3d545983771199eb"
    )


@pytest.mark.integration
async def test_shadow_evidence_requires_clean_stable_sample_window(shadow_evidence):
    evidence = await shadow_evidence.seal(
        minimum_events=1_000,
        minimum_seconds=604_800,
        cutoff=shadow_evidence.current_high_water(),
    )
    assert evidence.pending_count == 0
    assert evidence.failed_count == 0
    assert evidence.diverged_count == 0
    assert evidence.candidate_build_id == shadow_evidence.only_build_id
    assert evidence.candidate_config_hash == shadow_evidence.only_config_hash


@pytest.mark.integration
def test_0006_refuses_nonempty_legacy_shadow_comparisons_without_mutation(
    schema_at_0005, alembic_runner
):
    schema_at_0005.seed_shadow_comparison_without_replayable_input()
    with pytest.raises(DBAPIError, match="shadow_comparisons_not_empty_for_0006"):
        alembic_runner.upgrade(schema_at_0005, "20260713_0006")
    assert schema_at_0005.revision == "20260713_0005"
```

- [ ] **Step 2: Implement Active and Shadow modes**

Migration `20260713_0006` is linear from `20260713_0005`; Task 10's runtime-activation revision starts at `0007`. Because existing comparison hashes are irreversible, the revision locks `pipeline_shadow_comparisons` and fails closed in the same migration transaction unless it is empty; the feature flag must still be off. It then creates append-only `pipeline_shadow_inputs`, a typed mirror of the normalized event envelope: account, source, raw event type, change kind, opaque external ID, folder key, source version/time, processing policy, event key, input hash and the governed JSONB payload. The payload repeats the Inbox object/`jsonb::text <= 262144` contract, uses `event.payload_for_storage()`, and has exact runtime/auditor ACL and real-PostgreSQL round-trip tests. A unique `(account_id, event_key, input_hash)` identity is referenced by `pipeline_shadow_comparisons` through a composite foreign key, so its pending evaluator can reconstruct the complete immutable input after a process crash instead of trying to reverse a hash or abusing 16 KiB `safe_metadata`.

The same revision creates append-only `pipeline_shadow_evidence`: account/candidate pipeline, `ShadowDecisionV1` schema version, exact candidate build/config and normalizer/policy hashes, window start, immutable `(created_at,id)` cutoff high-water, event/time minimums, total/matched/pending/failed/diverged counts, rolling comparison hash, evidence hash and sealed timestamp. The default activation-quality window is at least 1,000 events spanning seven consecutive days (`604800` seconds), all under one build/config/decision schema. Sealing fails unless every comparison at or below the cutoff is terminal, `pending_count=failed_count=diverged_count=0`, totals reconcile exactly and both thresholds are met. A newer build/config/schema starts a new window; mixed-version decisions are never compared or waived. Evidence identity and counts are immutable and become a required FK/hash input to Task 10's barrier.

`ShadowDecisionV1` is the only comparable decision contract. Both legacy observation and candidate evaluation are projected into exactly its ten fields and hashed as canonical schema-versioned UTF-8 JSON; delivery metadata, model prose, raw payload and timestamps are excluded. The same revision adds bounded Shadow evaluation lease state to `pipeline_shadow_comparisons`: owner, lease deadline, attempts, available-at and safe failure code with an exact state matrix. Evaluators claim disjoint pending rows through `FOR UPDATE SKIP LOCKED`; completion/failure CASes ID, owner, unexpired lease, generation and fence. A startup/claim-cycle reaper clears expired leases into capped retry and eventually a safe terminal `shadow_status='failed'`/`comparison_status='incomplete'`. It never leaves a crash-stuck `pending` row and never retries forever.

Advancing the head is a complete revision-contract change, not only a migration file. The same task updates `PHASE_2_DATABASE_REVISION`, disabled-flag compatibility, bootstrap pre/post expectations, exact schema digests, all four role/ACL manifests, checkpoint revision allowlists and offline SQL generation. Add a real PostgreSQL bridge test proving: current Task 9 code with all Phase 2 flags off runs on exact `0005`; bootstrap advances to exact `0006`; runtime/schema plus migration/runtime/maintenance/auditor gates pass; only then may shadow be enabled. This is again code-first only—an old `0005` process does not accept a database-first `0006` head.

Refactor the Task 2 Webhook normalizer internally into a strict verified-envelope parse and a policy-bound finalization while preserving its locked public signature. `IngressService.accept()` parses and validates the signed body once, passes the verified account/event/folder identity to Task 7's event-aware `ProcessingPolicyResolver`, then finalizes the immutable event with that explicit policy. Active and Shadow use this exact same path. If the policy snapshot is missing, ambiguous or its refresh failed, resolution stops before any Inbox/Shadow write and the route returns retryable 503; it never substitutes `FULL`, `IGNORED` or a stale snapshot. If the snapshot is ready and the exact event/folder is legitimately outside configured scopes, the resolver returns `IGNORED` and active intake commits the normal terminal audited receipt. Tests cover initial cache failure, refresh failure, successful recovery/retry, unknown folder, and the shared NewMail/Created/Modified/Delete versus Sync matrix.

The dormant Active implementation writes into `event_inbox` under the current executable pipeline generation, but Phase 2 runtime and CLI cannot authorize or expose it in production. Shadow never writes or claims `event_inbox`, because doing so would consume the global dedupe key before activation. Shadow intake first takes a transaction-scoped advisory lock derived from the exact seven-column candidate identity, then reads any existing comparison. The candidate identity and normalized `event_key` are authoritative and use first-write-wins semantics matching active intake: every retry for an existing candidate returns the original receipt with `duplicate=True`, including when delivery metadata, trusted time, payload or processing policy makes the recomputed `input_hash` drift while the locked event identity remains the same. Such drift increments a bounded, privacy-safe observability counter but does not block the legacy-authoritative request, insert another input, replace the canonical first input or create another comparison.

Shadow storage is an observation write, not an authority receipt. `ShadowIngressReceipt` proves only that Shadow input/comparison storage committed; it never means that the legacy handler completed and cannot determine the HTTP/business response. After either a fresh or duplicate Shadow receipt, the route still invokes the legacy-authoritative path unless durable authority was later activated by Phase 3. A Shadow duplicate, evaluator failure or comparison conflict never suppresses legacy execution, converts a legacy failure to success or returns Durable 202. A crash after Shadow commit but before legacy invocation is retried as a duplicate Shadow write followed by normal legacy invocation. Only an absent comparison allows insertion of the input/comparison pair, preventing orphans; the leased/reaped evaluator loads the canonical first input and records `ShadowDecisionV1` without network dependencies or model, Feishu, Exchange or Qdrant effects. No `event_inbox.execution_mode` column exists or may be inferred. Neither candidate path fetches email detail or runs a model in the HTTP request.

- [ ] **Step 3: Verify and commit**

```bash
.venv/bin/python -m pytest tests/unit/test_exchange_webhook.py tests/unit/test_event_routing.py tests/unit/ingestion/test_normalization.py tests/unit/ingestion/test_policy.py tests/contracts/test_shadow_decision_contract.py tests/unit/test_database_revision.py tests/unit/test_database_schema_contract.py tests/unit/test_checkpoint_repository_safety.py tests/unit/test_alembic_offline.py tests/integration/ingestion/test_shadow_intake.py tests/integration/ingestion/test_schema.py tests/integration/ingestion/test_access_roles.py tests/integration/test_checkpoint_cleanup.py -q
git add alembic/versions/20260713_0006_shadow_inputs.py src/ingestion/webhook.py src/ingestion/shadow.py src/ingestion/shadow_decision.py src/ingestion/normalization.py src/ingestion/policy.py src/ingestion/models.py src/server.py src/db/access_contract.py src/db/bootstrap.py src/db/schema.py src/db/schema_contract.py src/maintenance/checkpoint_repository.py tests/unit/test_exchange_webhook.py tests/unit/test_event_routing.py tests/unit/ingestion/test_normalization.py tests/unit/ingestion/test_policy.py tests/contracts/test_shadow_decision_contract.py tests/unit/test_database_revision.py tests/unit/test_database_schema_contract.py tests/unit/test_checkpoint_repository_safety.py tests/unit/test_alembic_offline.py tests/integration/ingestion/test_shadow_intake.py tests/integration/ingestion/test_schema.py tests/integration/ingestion/test_access_roles.py tests/integration/test_checkpoint_cleanup.py
git commit -m "feat: accept Exchange webhooks into durable inbox"
```

---

### Task 10: Add DB-authoritative Runtime Activation, Instance Leases, and Cutover Barriers

**Files:**
- Create: `alembic/versions/20260713_0007_runtime_activation.py`
- Create: `src/ingestion/runtime_authority.py`
- Create: `src/ingestion/cutover_barrier.py`
- Create: `tests/unit/ingestion/test_runtime_authority.py`
- Create: `tests/integration/ingestion/test_runtime_activation.py`
- Create: `tests/architecture/test_phase2_activation_boundary.py`
- Modify: `src/exchange_service.py`
- Modify: `src/server.py`
- Modify: `src/scheduler/polling.py`
- Modify: `src/utils/self_healing.py`
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
- Modify: `tests/integration/test_checkpoint_cleanup.py`

**Interfaces:**
- Produces: `RuntimeAuthorityRepository`; `RuntimeInstanceLease`; `CutoverBarrierService`; mandatory `LegacyEffectGuard`; linear database head `20260713_0007`

- [ ] **Step 1: Write authority, roster, and old-effect fence tests**

```python
@pytest.mark.integration
async def test_mixed_or_unregistered_deployment_cannot_be_ready(authority, barrier):
    await barrier.import_roster(
        expected={"web-1", "worker-1", "legacy-job-1"},
        deployment_revision="deploy-42",
    )
    await authority.heartbeat(instance_id="web-1", build_number=42, protocol=2)
    await authority.heartbeat(instance_id="worker-1", build_number=42, protocol=2)
    with pytest.raises(ActivationBlocked, match="roster"):
        await barrier.mark_ready()


@pytest.mark.integration
async def test_phase2_cannot_transition_to_durable_active_even_with_valid_evidence(
    authority, barrier
):
    candidate = await barrier.seed_phase3_candidate_with_valid_phase2_evidence()
    with pytest.raises(ActivationBlocked, match="phase3_activation_required"):
        await authority.transition(candidate.id, target_mode="durable_active")
    assert (await authority.get(candidate.account_id)).mode != "durable_active"


@pytest.mark.integration
async def test_legacy_effect_identity_is_immutable_and_state_is_monotonic(
    legacy_effects, runtime_role, maintenance_role
):
    effect = await legacy_effects.insert_started(runtime_role, kind="exchange_mark_read")
    await legacy_effects.finish(runtime_role, effect.id, state="unknown")
    with pytest.raises(DatabaseOperationError):
        await legacy_effects.rewrite_identity_for_test(runtime_role, effect.id)
    with pytest.raises(DatabaseOperationError):
        await legacy_effects.finish(runtime_role, effect.id, state="completed")
    await legacy_effects.reconcile(
        maintenance_role,
        effect.id,
        evidence_hash="a" * 64,
        actor="operator",
        reason="verified",
    )
    assert (await legacy_effects.get(effect.id)).state == "reconciled"


@pytest.mark.integration
async def test_crash_after_remote_before_finish_then_retry_never_repeats_effect(
    legacy_guard, remote, restart_legacy_runtime
):
    identity = LegacyEffectIdentity(
        account_id=8,
        event_key="a" * 64,
        authority_epoch=7,
        effect_kind="exchange_mark_read",
        ordinal=1,
        target_hash="b" * 64,
    )
    token = await legacy_guard.try_begin(identity)
    await remote.execute(token)
    await restart_legacy_runtime(crash_before_finish=True)
    with pytest.raises(ManualReconciliationRequired):
        await legacy_guard.try_begin(identity)
    assert remote.execute_count(identity) == 1


@pytest.mark.integration
async def test_completed_effect_replay_skips_remote_call(
    legacy_guard, remote
):
    identity = legacy_guard.identity_for(event_key="c" * 64, ordinal=1)
    token = await legacy_guard.try_begin(identity)
    await remote.execute(token)
    await legacy_guard.finish(token, state="completed")
    replay = await legacy_guard.try_begin(identity)
    assert replay.disposition == "already_completed"
    assert remote.execute_count(identity) == 1


@pytest.mark.integration
async def test_concurrent_same_effect_identity_yields_one_execute_token(
    legacy_guard
):
    identity = legacy_guard.identity_for(event_key="d" * 64, ordinal=1)
    results = await asyncio.gather(
        legacy_guard.try_begin(identity),
        legacy_guard.try_begin(identity),
        return_exceptions=True,
    )
    assert sum(getattr(result, "disposition", None) == "execute" for result in results) == 1
    assert sum(isinstance(result, ManualReconciliationRequired) for result in results) == 1


@pytest.mark.integration
async def test_target_profile_remains_standby_through_phase2(
    barrier, target_runtime
):
    standby = await target_runtime.register(profile="durable_sync")
    assert standby.lifecycle == "standby"
    assert standby.intake_count == standby.claim_count == standby.effect_count == 0
    await barrier.freeze_phase2_readiness(roster={standby.instance_id})
    assert (await target_runtime.refresh()).lifecycle == "standby"


@pytest.mark.integration
async def test_barrier_rejects_stale_backfill_or_shadow_evidence(
    barrier, completed_backfill, sealed_shadow_evidence
):
    candidate = barrier.candidate(
        backfill_plan_id=completed_backfill.id,
        shadow_evidence_id=sealed_shadow_evidence.id,
    )
    completed_backfill.mutate_target_hash_for_test()
    with pytest.raises(ActivationBlocked, match="backfill"):
        await barrier.mark_ready(candidate)
    completed_backfill.restore_for_test()
    sealed_shadow_evidence.mutate_cutoff_hash_for_test()
    with pytest.raises(ActivationBlocked, match="shadow"):
        await barrier.mark_ready(candidate)


@pytest.mark.integration
async def test_every_legacy_nonterminal_is_zero_or_exactly_quarantined(
    barrier, legacy_source, backfill
):
    legacy_source.seed_nonterminal(status="waiting_approval", count=2)
    plan = await backfill.complete_exact_quarantine(count=1)
    with pytest.raises(ActivationBlocked, match="legacy_nonterminal"):
        await barrier.freeze_evidence(backfill_plan_id=plan.id)
    await backfill.complete_exact_quarantine(count=1)
    evidence = await barrier.freeze_evidence(backfill_plan_id=plan.id)
    assert evidence.legacy_nonterminal_count == evidence.quarantined_count == 2


@pytest.mark.integration
async def test_quiescing_rejects_new_legacy_intake_but_drains_stamped_work(
    authority, legacy_queue, legacy_guard
):
    stamped = await legacy_queue.enqueue_before_quiesce()
    await authority.quiesce(8, actor="operator", reason="cutover")
    assert await legacy_queue.try_enqueue_new() is False
    assert await legacy_guard.authorize(stamped.token, "qdrant_write") is True


@pytest.mark.parametrize(
    "missing_proof",
    ["deployment_roster", "lb_isolation", "effect_secret", "legacy_db"],
)
async def test_every_external_isolation_proof_is_required(barrier, missing_proof):
    candidate = barrier.valid_candidate().without(missing_proof)
    with pytest.raises(ActivationBlocked):
        await barrier.mark_ready(candidate)


@pytest.mark.integration
async def test_cutover_barrier_state_machine_rejects_out_of_order_steps(barrier):
    plan = await barrier.plan()
    with pytest.raises(InvalidBarrierTransition):
        await barrier.record_drained(plan.id)
    await barrier.record_fence_verified(plan.id)
    await barrier.record_quiesced(plan.id)
    await barrier.record_drained(plan.id)
    await barrier.freeze_evidence(plan.id)
    await barrier.record_target_standby(plan.id)
    await barrier.record_legacy_isolated(plan.id)
    await barrier.record_fresh_proofs(plan.id)
    assert (await barrier.mark_ready(plan.id)).state == "ready"


@pytest.mark.integration
async def test_quiesce_commit_unknown_replays_same_command_receipt(
    authority, receipts, fault
):
    fault.lose_ack_after_commit("pipeline.quiesce")
    with pytest.raises(CommitAcknowledgementLost):
        await authority.quiesce(
            8,
            actor="operator",
            reason="cutover",
            idempotency_key="quiesce-1",
        )
    replay = await authority.quiesce(
        8,
        actor="operator",
        reason="cutover",
        idempotency_key="quiesce-1",
    )
    assert replay.mode == "quiescing"
    assert await receipts.count("pipeline.quiesce", "quiesce-1") == 1
```

- [ ] **Step 2: Add the linear runtime-authority migration**

Migration `20260713_0007` is linear from `20260713_0006`; Phase 3 therefore starts at `0008`. It creates seven governed control tables and adds `sync_cold_start_plans.target_reservation_id`:

- `pipeline_runtime_authority`: one versioned row per account with mode (`legacy_authoritative/shadow/quiescing/durable_active`), monotonic `authority_epoch`, ownership generation/fence/pipeline FK, policy readiness/config hash, minimum numeric build, minimum protocol, actor/reason and CAS version.
- `pipeline_runtime_instances`: per-account process lease with instance/workload/deployment IDs, numeric build plus display build ID, protocol/config/profile, lifecycle (`standby/active/draining`), observed epoch, legacy queue/in-flight/effect counts, heartbeat and lease deadline. Build ordering uses a CI-produced integer; Git SHA is never ordered lexically.
- `pipeline_target_reservations`: one active reservation per account/live barrier with an immutable target pipeline name, preallocated generation/fencing token, build/protocol/config hash, zero-work standby roster hash, expiry, state (`reserved/promoted/cancelled/expired`) and its reserved `pipeline_ownership` FK. `prepare_target()` takes the account advisory lock, creates the ownership row in non-current standby/quiescing state and allocates the final generation/fence exactly once. Folder plans and the live barrier reference this row by FK. Promotion never rotates the fence. Cancel/expiry under the same lock retires the unused ownership row and releases only the active-reservation slot; generation/fence are never reused.
- `pipeline_legacy_effects`: account/event-key/authority-epoch/generation/fence/instance/effect-kind/deterministic-ordinal/target-hash registrations with immutable identity, state (`started/completed/unknown/reconciled`), reconciliation disposition, idempotency/evidence hashes and timestamps. A UNIQUE constraint covers `(account_id,event_key,authority_epoch,effect_kind,ordinal,target_hash)`; ordinal identifies an intended distinct effect in the deterministic pipeline, never a retry attempt. The only transitions are `started -> completed|unknown` and maintenance-only `unknown -> reconciled`; completed/reconciled are terminal. UPDATE of identity/effect kind/ordinal/target/initial token, DELETE and TRUNCATE are rejected. A crash never deletes or auto-expires a started/unknown outcome.
- `pipeline_cutover_barriers` and `pipeline_cutover_barrier_members`: immutable predecessor barrier, target mode/config/build/protocol, exact external deployment roster and revision/hash, expected instance count, exact completed backfill-plan and sealed Shadow-evidence FKs plus their source/cutoff/high-water/build/config/count/hash facts, legacy nonterminal/quarantine counts and hash, versioned `exchange_sync_contract_v2` build/profile/page/continuation/read-flag probe hash, the complete configured `FolderScope` manifest plus one exact target-bound active cursor or unexpired approved cold-start boundary per folder and its rolling hash, expiring LB isolation, effect-secret rotation and legacy-DB-connection isolation proof hashes, and nullable Phase-3 approval/outbox/legacy-card/adapter-manifest evidence hashes that are mandatory for a `durable_active` target row. Barrier state is the strict sequence `planned -> fence_verified -> quiesced -> drained -> evidence_frozen -> target_standby -> legacy_isolated -> proof_fresh -> ready -> consumed`, with only pre-ready cancellation allowed.
- `legacy_backfill_plans`: account, ownership generation/fence, mapping/config versions, source count/high-water/rolling hash, legacy-nonterminal count, exact quarantine count/hash, target count/reconciliation hash, applied cursor/hash/counts, status, actor/reason and timestamps; it stores no email body, draft, response or raw identifier sample. Idempotency lives in the shared append-only command receipts, not a mutable key column on the plan.

All hashes are exact lowercase SHA-256, counters are bounded BIGINT, metadata is a bounded object, open barrier/backfill identities are unique, and forward-only state/identity guards prevent history rewrite. Runtime receives SELECT on authority/barriers, INSERT plus only heartbeat/lease/counter UPDATE columns on its own instance registrations, INSERT on `pipeline_legacy_effects`, and only `state/evidence_hash/completed_at` UPDATE needed to close its own `started -> completed|unknown`; it has no reconcile, DELETE, TRUNCATE, barrier or authority mutation privilege. Maintenance may classify a provably stale/crashed `started -> unknown`, performs `unknown -> reconciled` with actor/reason/evidence, and owns cutover/backfill transitions; it cannot rewrite identity or reopen a terminal row. Auditor has SELECT only; migration owns DDL. Real-role tests prove every allowed and forbidden operation. The same task updates exact schema digests, all four ACL manifests, checkpoint revision allowlists, bootstrap/offline SQL and proves a code-first real-PostgreSQL `0006 -> 0007` bridge with all activation profiles disabled.

- [ ] **Step 3: Implement the authority state machine and mandatory legacy fence**

Environment flags are never authority; they become only a requested runtime profile in Task 11. Phase 2 implements and exposes only `legacy_authoritative -> quiescing -> shadow` plus a pre-switch cancellation back to the prior legacy/Shadow mode. The schema reserves `durable_active`, but `RuntimeAuthorityRepository`, Phase-2 CLI and runtime must reject every Phase-2 attempt to enter it with `phase3_activation_required`. Phase 3 alone adds the transition `shadow -> quiescing -> durable_active` after fenced approval/send readiness; later rollback is `durable_active -> quiescing -> durable_active` with a new `legacy_compat` generation and can never resurrect the old in-memory queue or Shadow as authority.

Every legacy Webhook/poller/self-healer item is stamped at intake with account, event key, email ID and expected email version, authority epoch, generation and fence. `process_and_archive_email()` keeps the Task 8 typed `before_external_effect(kind, ordinal, target_hash)` hook mandatory in production. That hook cannot be a read-only authorize-then-call check: `try_begin_effect()` takes the same per-account transaction advisory lock later used by Phase 3 switch, then uses the fixed row-lock order email -> authority/ownership -> exact effect identity. Under the email lock it re-reads version, `create_seen_at`, status and `source_deleted_at`; a deleted/tombstoned aggregate or version that no longer authorizes work rejects before inserting an effect. Only an absent identity for a still-authorized aggregate inserts `state='started'` and returns an execute token. If effect-begin wins the delete race, that one registered call may finish outcome-known/unknown, but delete records `source_deleted_at` and every later effect begin re-locks/rechecks the email and is rejected. If delete wins, no remote call begins. A completed identity returns `already_completed` and the caller skips the remote call; `started` or `unknown` fails closed with `ManualReconciliationRequired` and can never call remotely again. A reconciled-completed identity also skips; a reconciled-not-executed outcome requires a separate maintenance-authorized new ordinal rather than reopening the old row. ContentStore, model, Feishu, Exchange and Qdrant boundaries each begin and finish their own registration. Runtime may close its own started row as completed/unknown but cannot reconcile; stale started rows become unknown only through maintenance, and unknown requires explicit maintenance reconciliation evidence. A crash after the remote effect but before `finish()` therefore leaves a durable started row; Webhook retry/restart sees it and never repeats the effect. Phase 3 switch takes the same lock and requires zero started/unknown registrations, giving effect-begin versus delete and epoch-switch one serialized winner with no authorization-to-call TOCTOU window.

The Worker additionally re-runs the Task-8 stamped adapter selection before each lease and each later effect boundary. `legacy_compat` may execute only for the matching legacy-authoritative/Shadow stamp or an explicitly draining old generation, always through the exact effect registration above. A target/durable stamp can never select that adapter; until Phase 3 registers an exact build/config `DurableProcessingAdapter`, it remains standby and claim-ineligible.

Quiescing stops new Webhook intake, polling, Sync, Shadow intake and new Durable claims but allows already-stamped legacy work to drain and begin registered effects while its epoch remains current. Phase 2 may complete only the side-effect-free Shadow transition; after P4-P6 capability successors and real v2 proof, the Phase-3 switch transaction alone increments the epoch and atomically changes ownership/authority, consumes the latest `production_ready` successor, appends the `pipeline.switch` command receipt and audit, and fences every old legacy token. Architecture tests forbid unguarded production calls from `server.py`, `exchange_service.py`, `polling.py` and `self_healing.py`; `tests/architecture/test_phase2_activation_boundary.py` additionally fails if a Phase-2 module can call a durable-active transition or start Durable intake/claim/Sync.

Instance registry cannot prove that a pre-protocol build which never registers is absent. Activation therefore additionally requires an external deployment/LB roster covering HTTP, Worker, poller, cron and jobs; all roster members must have fresh matching leases, and no extra active instance may exist. Only after `prepare_target()` persists the reservation may target-profile processes register against its exact generation/fence as `standby`: they heartbeat build/protocol/config/roster evidence but start zero intake, claim, scheduler or effect work. This breaks the activation handshake deadlock; after the atomic switch promotes that same ownership row without changing its fence, they observe the new epoch and become active, while old instances are fenced.

The live cutover stages are deliberately ordered and cannot be skipped: (1) obtain real v2 proof, prove every serving build fence-aware and create the distinct live barrier in `planned`; (2) `prepare_target(live_barrier_id)` under the account lock and register its exact zero-work standby roster; (3) seal/approve every FolderScope boundary against the reservation while legacy remains authoritative; (4) enter card freeze/quiescing; (5) drain/quarantine legacy work, finish old-card invalidation and freeze exact backfill/Shadow evidence; (6) remove old workloads/LB routes; (7) rotate effect/DB credentials and terminate old connections; (8) import fresh short-lived roster/isolation proof; (9) mark the live barrier ready; and (10) Phase 3 immediately promotes the reserved ownership row before proof expiry. State-machine tests reject reordering, a changed barrier/reservation FK/fence, any standby work before promotion, or switch-time fence rotation.

Every legacy source nonterminal row must either be absent at the frozen source high-water or have exactly one deterministic completed BACKFILL/HISTORICAL_SUPPRESSED quarantine Inbox fact covered by the backfill plan's count/hash. Known legacy approval/Lark cards additionally produce a bounded `legacy_card_invalidation_required` audit fact; Phase 2 does not claim they are invalidated during implementation. Phase3-P6 implementation capability successors may reference readiness/capability evidence but do not require or trigger live invalidation/quiesce/isolation. Only the later `production_ready` snapshot FK-binds a distinct live barrier and must include Phase-3 Outbox/action contracts plus completed old-card invalidation. Live readiness and final switch independently re-read the exact backfill, Shadow and real-v2 evidence. Pending/failed/diverged Shadow rows, a sample window below 1,000 events/seven days, mixed build/config/schema, unquarantined legacy state, the known-incompatible extension, missing v2 proof, reservation/version/profile drift, stale high-water or any missing/expired live proof blocks production progress without breaking current legacy service.

- [ ] **Step 4: Verify and commit**

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/unit/ingestion/test_runtime_authority.py tests/integration/ingestion/test_runtime_activation.py tests/architecture/test_phase2_activation_boundary.py tests/unit/test_database_revision.py tests/unit/test_database_schema_contract.py tests/unit/test_checkpoint_repository_safety.py tests/unit/test_alembic_offline.py tests/integration/ingestion/test_schema.py tests/integration/ingestion/test_access_roles.py tests/integration/test_checkpoint_cleanup.py -q
git add alembic/versions/20260713_0007_runtime_activation.py src/ingestion/runtime_authority.py src/ingestion/cutover_barrier.py src/exchange_service.py src/server.py src/scheduler/polling.py src/utils/self_healing.py src/db/access_contract.py src/db/bootstrap.py src/db/schema.py src/db/schema_contract.py src/maintenance/checkpoint_repository.py tests/unit/ingestion/test_runtime_authority.py tests/integration/ingestion/test_runtime_activation.py tests/architecture/test_phase2_activation_boundary.py tests/unit/test_database_revision.py tests/unit/test_database_schema_contract.py tests/unit/test_checkpoint_repository_safety.py tests/unit/test_alembic_offline.py tests/integration/ingestion/test_schema.py tests/integration/ingestion/test_access_roles.py tests/integration/test_checkpoint_cleanup.py
git commit -m "feat: add database-authoritative ingestion readiness"
```

---

### Task 11: Wire Shadow Runtime, Bounded Reconciliation, Governed Backfill, and Readiness CLI

**Files:**
- Create: `src/ingestion/runtime.py`
- Create: `src/ingestion/backfill.py`
- Create: `src/ingestion/cutover.py`
- Create: `src/ingestion/sync_resources.py`
- Create: `src/ingestion/sync_contract_probe.py`
- Create: `src/scheduler/sync_reconciliation.py`
- Create: `scripts/backfill_durable_ingestion.py`
- Create: `scripts/manage_pipeline.py`
- Create: `tests/unit/ingestion/test_runtime.py`
- Create: `tests/unit/ingestion/test_cutover.py`
- Create: `tests/unit/ingestion/test_backfill.py`
- Create: `tests/integration/ingestion/test_sync_resource_isolation.py`
- Create: `tests/integration/ingestion/test_sync_contract_probe.py`
- Create: `tests/architecture/test_phase2_runtime_stays_standby.py`
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
- Produces: one Phase-2 `IngestionRuntime`; DB-backed `/queue`/metrics including manual review; `BackfillService.plan/execute`; versioned read-only `SyncContractProbe`; `CutoverReadinessService.plan/quiesce/shadow_switch/ready/drain_status`; CLI exit codes 0 success, 2 blocked, 3 invariant failure

- [ ] **Step 1: Write profile, lifecycle, backfill, and rollback tests**

```python
@pytest.mark.parametrize(
    ("durable", "shadow", "sync", "expected"),
    [
        (False, False, False, "legacy"),
        (False, True, False, "shadow"),
        (True, False, False, "durable"),
        (True, False, True, "durable_sync"),
    ],
)
def test_valid_flag_profiles_are_only_requests(durable, shadow, sync, expected):
    assert requested_profile(durable, shadow, sync) == expected


@pytest.mark.parametrize(
    ("durable", "shadow", "sync"),
    [(False, False, True), (False, True, True), (True, True, False), (True, True, True)],
)
def test_invalid_flag_profiles_fail_before_any_component_starts(durable, shadow, sync):
    with pytest.raises(ConfigurationError):
        requested_profile(durable, shadow, sync)


@pytest.mark.asyncio
async def test_runtime_stops_scheduling_and_claiming_before_drain(runtime):
    await runtime.start()
    await runtime.stop(grace_seconds=1.0)
    assert runtime.scheduler.accepting is False
    assert runtime.worker.accepting is False
    assert runtime.worker.inflight_count == 0


async def test_backfill_unknown_status_blocks_without_side_effects(backfill, effects):
    plan = await backfill.plan(source_status="vendor_custom_status")
    assert plan.status == "blocked"
    await backfill.execute(plan.id)
    effects.assert_none()


async def test_backfill_replay_and_source_drift_are_safe(backfill, legacy_db, new_db):
    plan = await backfill.plan_current_source()
    await backfill.execute(plan.id, batch_size=500)
    await backfill.execute(plan.id, batch_size=500)
    assert await new_db.email_count() == plan.source_count
    legacy_db.mutate_row_after(plan.source_high_water)
    with pytest.raises(SourceSnapshotDrift):
        await backfill.execute(plan.id)


@pytest.mark.integration
async def test_completed_backfill_quarantine_has_no_inbox_error(
    backfill, legacy_db, new_db
):
    legacy_db.seed(status="waiting_approval", id="legacy-1")
    plan = await backfill.plan_current_source(
        actor="operator",
        reason="quarantine",
        idempotency_key="backfill-plan-1",
    )
    await backfill.execute(
        plan.id,
        actor="operator",
        reason="quarantine",
        idempotency_key="backfill-execute-1",
        batch_size=500,
    )
    inbox = await new_db.backfill_inbox_for("legacy-1")
    email = await new_db.email_for("legacy-1")
    assert (inbox.status, inbox.processing_policy) == (
        "completed",
        "historical_suppressed",
    )
    assert inbox.safe_error_code is inbox.safe_error_summary is None
    assert email.status == "manual_review"
    assert email.safe_error_code == "legacy_backfill_quarantined"
    assert email.processing_inbox_id == inbox.id


@pytest.mark.integration
async def test_small_slow_sync_pool_does_not_starve_webhook_or_worker(
    sync_runtime, main_runtime, slow_exchange
):
    sync_runtime.configure(pool_size=2, max_concurrent_folders=2)
    runs = [sync_runtime.run_folder(folder) for folder in slow_exchange.folders(5)]
    results = await asyncio.gather(*runs)
    assert sum(result.status == "busy_skip" for result in results) == 3
    assert await main_runtime.webhook_insert_completes_within(0.5)
    assert await main_runtime.worker_claim_completes_within(0.5)


@pytest.mark.integration
async def test_cancelled_sync_releases_http_db_lock_and_budget(
    sync_runtime, lock_probe
):
    task = asyncio.create_task(sync_runtime.run_blocked_folder("INBOX"))
    await sync_runtime.http_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert sync_runtime.available_db_connections == sync_runtime.pool_size
    assert sync_runtime.available_http_connections == sync_runtime.http_pool_size
    assert sync_runtime.available_folder_permits == sync_runtime.max_concurrent_folders
    assert await lock_probe.can_acquire(8, "INBOX") is True


@pytest.mark.integration
async def test_current_extension_contract_is_explicitly_blocked(
    sync_contract_probe, current_extension_fixture
):
    current_extension_fixture.seed_pending_changes(501)
    current_extension_fixture.emit_read_flag_change(item=None)
    evidence = await sync_contract_probe.run(
        account_id=8,
        folder="INBOX",
        request_limit=500,
        actor="operator",
        reason="activation-boundary",
        idempotency_key="sync-bound-probe-1",
    )
    assert evidence.status == "blocked_external"
    assert evidence.safe_code == "exchange_sync_contract_incompatible"
    assert evidence.failures == {"unbounded_result", "raw_read_flag_change"}


@pytest.mark.integration
async def test_v2_probe_requires_bounded_continuation_and_mapped_read_flag(
    sync_contract_probe, fixed_v2_extension_fixture
):
    fixed_v2_extension_fixture.seed_pending_changes(501)
    fixed_v2_extension_fixture.emit_mapped_read_update(is_read=True)
    evidence = await sync_contract_probe.run(
        account_id=8,
        folder="INBOX",
        request_limit=500,
        actor="operator",
        reason="activation-boundary",
        idempotency_key="sync-v2-probe-1",
    )
    assert evidence.status == "proved"
    assert evidence.contract_version == "exchange_sync_contract_v2"
    assert all(page.count <= 500 for page in evidence.pages)
    assert evidence.pages[-1].includes_last is True
    assert evidence.read_flag_profile == "map_to_update_v1"
```

- [ ] **Step 2: Lock requested profiles and lifecycle**

Add `DURABLE_INBOX_ENABLED`, `INGESTION_SHADOW_ENABLED`, `SYNC_RECONCILIATION_ENABLED`, `SYNC_INTERVAL_SECONDS=300`, `SYNC_BATCH_SIZE=500`, bounded pages/run seconds, Worker lease/concurrency/attempt/grace settings, runtime instance ID/build number/build ID/protocol and heartbeat/lease settings. Add a dedicated Sync PostgreSQL pool (`SYNC_DB_POOL_SIZE=2`), a dedicated Exchange Sync HTTP pool (`SYNC_HTTP_MAX_CONNECTIONS=2`) and nonblocking global folder budget (`SYNC_MAX_CONCURRENT_FOLDERS=2`). These resources are separate from the main Webhook/Worker database and HTTP capacity. If no folder permit or Sync connection is immediately available, the scheduler records a bounded `busy_skip` fact and does not enqueue an unbounded waiter. Cancellation must close the streaming response, advisory-unlock on the same connection, return DB/HTTP connections and release the permit in `finally`.

The only valid requested flag tuples are the four tests above, but in Phase 2 `durable`/`durable_sync` may only advertise standby capability: they never authorize work. Requested profiles must match DB authority mode, minimum build/protocol and policy hash before any legacy or Shadow component starts.

DB mode is authoritative: legacy permits only old guarded Webhook/poller; Shadow keeps legacy authoritative and allows side-effect-free candidate comparisons; quiescing permits no new intake/claim; the schema's durable mode is unavailable to Phase-2 production code. Durable candidate processes register only as standby with zero intake, claim, Sync, scheduler and effect work. `tests/architecture/test_phase2_runtime_stays_standby.py` scans `main.py`, `server.py`, runtime and CLI wiring and fails if Phase 2 can expose Durable 202, start Durable Worker claims or execute Sync writes. Phase 3 later authorizes those paths only after the latest complete `production_ready` successor.

Startup may probe Sync permission for every FolderScope, not only Inbox, but the probe never writes cursor/Inbox and does not start the five-minute scheduler in Phase 2. `AppContext` owns one runtime; shutdown stops Shadow schedules, drains boundedly, heartbeats the draining state, then closes dedicated Sync and main clients. The former `/list` loop cannot process directly; any compatibility import delegates to the authority-guarded legacy or dormant coordinator and cannot bypass the Phase-2 activation boundary.

- [ ] **Step 3: Implement fail-closed historical backfill**

The Backfill CLI requires `--account-id`, defaults to dry-run, and `plan`/`execute` both require `--actor --reason --idempotency-key`; execute additionally requires `--plan-id --batch-size`. Every command uses the shared append-only receipt: the plan/batch state and receipt commit in one transaction, same-key/same-payload replay returns the stored result, different payload conflicts, and commit-unknown reads the receipt/target state before any work. Planning locks the current ownership generation/fence and a canonical source snapshot `(count, max(updated_at,id), rolling row hash)`, exact mapping/config hashes, the complete legacy-nonterminal manifest and a bounded content-reference policy. Each execute batch rechecks source high-water and row hashes, then atomically inserts idempotent target facts and advances the DB plan cursor/hash. Unknown/NULL source status, source drift, invalid email ID/timestamp/JSON, foreign or malformed `content_ref`, ownership change, or mapping/config drift blocks/quarantines the plan before continuation.

The complete legacy status manifest is:

```python
LEGACY_EMAIL_STATUS_MAP = {
    "pending": "manual_review",
    "ingested": "manual_review",
    "analyzed": "manual_review",
    "drafted": "manual_review",
    "error": "manual_review",
    "recovering": "manual_review",
    "approved": "manual_review",
    "saving_draft": "manual_review",
    "modified": "manual_review",
    "sending": "send_unknown",
    "waiting_approval": "manual_review",
    "notified_readonly": "notified_readonly",
    "skipped": "no_action",
    "read": "no_action",
    "archived": "archived",
    "delivery_failed": "delivery_failed",
    "manual_review": "manual_review",
    "rejected": "rejected",
    "sent": "sent",
    "forwarded": "sent",
    "draft_saved": "draft_saved",
}
```

Every legacy nonterminal row is activation-safe only when the frozen snapshot contains zero such rows or the target contains exactly one deterministic quarantine fact for each source identity. The quarantine fact is `source='backfill'`, `change_kind='create'`, `processing_policy='historical_suppressed'`, `status='completed'`, with no lease/effect marker and **both `safe_error_code` and `safe_error_summary` null**. A linked `emails` row mapped to `manual_review` alone receives `safe_error_code='legacy_backfill_quarantined'`, a fixed non-sensitive summary and `processing_inbox_id` pointing to that completed Inbox fact. A legacy `sending` row remains explicit `send_unknown` quarantine and is never normalized into “sent”. The plan stores exact legacy-nonterminal count, quarantine count and rolling identity hash; completion requires exact equality and emits `legacy_card_invalidation_required` audit facts for known approval/card states, without claiming that Phase 2 invalidated a remote card.

Terminal/projection-only rows create no executable work. Preserve only a validated, account-owned typed `content_ref`; never copy draft/classification/reason/body data into Inbox payload, audit or logs. All backfill branches prohibit model, Feishu, Exchange and Qdrant effects. `app_kv_store` has no current Exchange Sync cursor schema—the initial cursor-key allowlist is therefore empty. No timestamp or arbitrary value may seed `sync_cursors`; every folder enters the Task 7 approved cold-start path unless a future versioned key schema is explicitly added and validated against Exchange.

- [ ] **Step 4: Implement cutover and rollback CLI**

Phase-2 CLI commands are `sync-contract-probe`, `plan`, `quiesce`, `barrier-status`, `shadow-switch`, `ready`, and `drain-status`, all with actor/reason/idempotency and append-only receipts. The probe is read-only with respect to mailbox items: it streams only until v2 proof or the first page/version/read-flag violation, never mutates an Exchange item or local production cursor, and commits its versioned evidence plus command receipt atomically. Same-key/same-payload returns that evidence; changed payload conflicts. Against the current extension it deterministically records `blocked_external_exchange_sync_contract`, not a potentially passing “unknown” result. Only a later extension build and complete real v2 probe can create `proved` evidence. `switch --target durable_active`, `rollback` and `retire` return exit 2 with `phase3_activation_required`; Phase 3 owns their real implementations. `ready` means immutable Phase-2 readiness evidence, not execution authority.

Before opening a **live** cutover barrier, the CLI requires version-matched, proved `exchange_sync_contract_v2` evidence for bounded pages, exact continuation/`includes_last` and the approved read-flag profile; implementation capability successors do not open/quiesce that barrier. The current extension cannot satisfy the live prerequisite, so this repository may complete implementation acceptance while reporting `blocked_external_exchange_sync_contract`, with legacy still authoritative. After the separately authorized extension release and real proof, the exact sequence is: verify serving builds and create the live barrier in `planned`; Phase-3 `prepare_target(live_barrier_id)` persists the reserved ownership generation/fence; register zero-work standbys and bind/approve every FolderScope plan by reservation FK; freeze cards and quiesce; drain/quarantine legacy work and finish invalidations; freeze backfill/Shadow evidence; isolate old workloads/LB/credentials/connections; import fresh proofs; mark that live barrier ready; stop. Phase 2 never performs the promotion, and Phase 3 may promote only that reserved row without rotating its fence. `drain-status`, `/queue` and metrics include `manual_review`, legacy nonterminal/quarantine counts, Shadow pending/failed/diverged counts, v2 proof/version/profile, reservation/standby identity and proof expiry.

- [ ] **Step 5: Run the complete Phase 2 gate and commit**

```bash
.venv/bin/python -m pytest tests/unit/ingestion tests/contracts/test_exchange_sync_contract.py tests/contracts/test_shadow_decision_contract.py tests/architecture/test_phase2_activation_boundary.py tests/architecture/test_phase2_runtime_stays_standby.py -q
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/integration/ingestion -q
.venv/bin/python -m pytest --cov=src.ingestion --cov-report=term-missing --cov-fail-under=90
.venv/bin/ruff check src/ tests/
.venv/bin/python -m pytest -q
git diff --check
git add src/ingestion src/scheduler src/config.py src/init_app.py src/main.py src/commands/handlers.py src/observability/metrics.py .env.example scripts/backfill_durable_ingestion.py scripts/manage_pipeline.py tests/unit/ingestion tests/integration/ingestion tests/contracts/test_exchange_sync_contract.py tests/contracts/test_shadow_decision_contract.py tests/architecture/test_phase2_activation_boundary.py tests/architecture/test_phase2_runtime_stays_standby.py tests/unit/test_polling_scheduler.py tests/unit/test_metrics.py tests/unit/test_command_router.py
git commit -m "feat: prepare fenced durable ingestion readiness"
```
