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
- 共享账户锁下，任何会触碰邮件的数据面路径固定为 email -> runtime authority（存在时）-> 按 generation 升序的 ownership/reservation -> exact Inbox/card -> Notification -> Mailbox -> Send/effect -> audit/receipt；只处理 lease 的路径可以 ownership -> Inbox，但之后不得再锁 email。freeze/switch/control 使用互斥账户锁后 authority -> ownership，因此不与两类数据面并发。多账户操作始终按 account_id 升序。
- `emails.version` 是业务/副作用授权版本，不是 folder/read 投影计数器；仅投影变化在 email 行锁下更新且不提升 version，不能让合法 Worker、effect token 或审批卡仅因已读标记变化而失效。

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
TEST_POSTGRES_ADMIN_URL=postgresql://user:password@localhost:5432/postgres TEST_POSTGRES_ROLE_DDL=1 .venv/bin/python -m pytest tests/integration/ingestion/test_schema.py -q
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
    pipeline_name: str
    generation: int
    fencing_token: int
    lease_owner: str
    attempts: int
    event: NormalizedIngressEvent
    received_at: datetime
    lease_until: datetime


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

The two `conftest.py` files define `db`, `ownership`, `repo`, `repo_a`, `repo_b`, `inbox_lease`, `leased_event`, `seeded_events`, `coordinator`, `exchange`, `worker`, `inbox`, `fault`, `crash`, `ingress`, `client`, `runtime`, `cutover`, deterministic `EVENT`, raw-body/payload fixtures, HMAC helper and seed methods referenced below. `InjectedFailure(RuntimeError)` and the fault/crash injectors expose only the named booleans used by tests. Integration setup reads `TEST_POSTGRES_ADMIN_URL` and requires `TEST_POSTGRES_ROLE_DDL=1`; the factory creates an isolated database plus migration/runtime/maintenance/auditor roles, upgrades Alembic to head, yields role-specific DSNs/pools and drops the database and roles in `finally`. Missing real-PostgreSQL prerequisites must be reported as a skip in local exploratory runs and are forbidden in release gates.

- [ ] **Step 5: Verify and commit**

```bash
TEST_POSTGRES_ADMIN_URL=postgresql://user:password@localhost:5432/postgres TEST_POSTGRES_ROLE_DDL=1 .venv/bin/python -m pytest tests/integration/ingestion/test_schema.py -q
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
- Create: `tests/architecture/test_pipeline_ownership_boundary.py`
- Modify: `src/domain/errors.py`
- Modify: `src/ingestion/__init__.py`

**Interfaces:**
- Produces: `StaleFence(ErrorKind.INTERNAL_INVARIANT)`; `bootstrap(account_id, pipeline_name)`, `get(account_id, generation)`, `current_ingress(account_id)`, `assert_fence(account_id, generation, fencing_token)`, `can_execute(lease)`, `quiesce()`, guarded `retire()`, and transaction-local ownership primitives consumed only by Phase 3 activation. Retirement is fail-closed unless a later task supplies complete Inbox/Outbox/high-water evidence through the mandatory guard.

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
async def test_quiesced_fence_allows_only_existing_stamped_work(
    ownership, inbox_lease
):
    current = await ownership.bootstrap(8, "legacy_compat")
    assert (
        inbox_lease.account_id,
        inbox_lease.generation,
        inbox_lease.fencing_token,
    ) == (
        current.account_id,
        current.generation,
        current.fencing_token,
    )
    await ownership.quiesce(
        account_id=8,
        expected_generation=current.generation,
        expected_fencing_token=current.fencing_token,
        actor="test",
        reason="drain",
    )
    assert await ownership.current_ingress(8) is None
    assert await ownership.can_execute(inbox_lease) is True
```

The immutable `InboxLease` value object already exists, but Task 4 owns its repository lifecycle and therefore owns the new-claim, lease renewal and completion CAS tests. Those SQL mutations must repeat the exact generation/fence predicate in the same statement or locked transaction; a prior `assert_fence()` result is never sufficient authorization.

- [ ] **Step 2: Implement transactional generation changes**

Take a stable per-account transaction advisory lock, configure bounded local lock/statement/idle-in-transaction timeouts, lock the exact ownership row with `FOR UPDATE`, and compare expected generation/token. `bootstrap()` is concurrent and idempotent only when no ownership history exists; `quiesce()` is the only Phase-2 production transition from `current_ingress -> quiescing`, appends one bounded audit fact, and forbids new claims while existing stamped work finishes. Phase 2 exposes no public generation switch.

The repository provides private transaction-local insert/state primitives, but they require the caller's already-open connection and transaction and are reachable in production only from Phase 3 `ActivationService`. The helper binds itself to the database's top-level transaction identity, rejects reuse across commits, and every mutating step reacquires the account lock and re-reads the exact persisted predecessor state; Python object state is never authority. A nested savepoint rollback therefore cannot leave a phantom draining state that later inserts a current generation. That later service atomically moves the old generation to draining and creates/promotes the reserved target together with authority/barrier/receipt/audit facts. Rollback must leave both ownership rows and audits unchanged. The runtime role's temporary raw ownership mutation privilege is tolerated only through `0006`; Task 10/`0007` must revoke it before any activation path is enabled and replace it with narrow database-enforced operations.

Standalone `assert_fence()`/`can_execute()` are diagnostic and permit continuation of already-stamped work in `current_ingress`, `quiescing`, or `draining`; they reject retired/stale identity. They never authorize a later claim, effect start, lease renewal or completion across a transaction boundary. Each such mutation must repeat the exact generation/fence/state predicate inside its own CAS statement or locked transaction.

`retire()` first rejects every unresolved state visible at this schema head, then invokes a mandatory retirement guard in the same transaction. The exact Inbox blocker set is `pending/retry_wait/leased/manual_review/dead_letter`; the exact email blocker set is `ingested/processing/retry_wait/manual_review/waiting_approval/send_queued/sending/accepted/send_unknown/dead_letter`. Email `send_failed` and `delivery_failed` are outcome-known terminal projections and do not block by themselves, while a recoverable, unaccounted `dead_letter` does. The default guard always denies because Outbox and high-water evidence do not exist at `0004`; successful retirement is enabled only after later tasks can prove zero nonterminal or `send_unknown` Outbox and matching high-water reconciliation. Missing future tables/evidence are never interpreted as zero. Historical terminal Inbox/Outbox rows remain append-only and may retain the ownership foreign key after retirement. Architecture tests fail if any Phase-2 runtime/CLI calls the transaction-local handoff primitives.

- [ ] **Step 3: Verify and commit**

```bash
TEST_POSTGRES_ADMIN_URL=postgresql://user:password@localhost:5432/postgres TEST_POSTGRES_ROLE_DDL=1 .venv/bin/python -m pytest tests/unit/ingestion/test_ownership.py tests/integration/ingestion/test_pipeline_fencing.py tests/architecture/test_pipeline_ownership_boundary.py -q
git add src/ingestion/ownership.py src/ingestion/__init__.py src/domain/errors.py tests/unit/ingestion/test_ownership.py tests/integration/ingestion/test_pipeline_fencing.py tests/architecture/test_pipeline_ownership_boundary.py
git commit -m "feat: add pipeline generations and fencing"
```

---

### Task 4: Implement Inbox Repository, Leases, Retries, and Dead Letters

**Files:**
- Create: `src/ingestion/repository.py`
- Create: `tests/unit/ingestion/test_repository.py`
- Create: `tests/integration/ingestion/test_inbox_repository.py`
- Create: `tests/architecture/test_inbox_repository_boundary.py`
- Modify: `src/ingestion/models.py`
- Modify: `src/ingestion/ownership.py`
- Modify: `src/ingestion/__init__.py`
- Modify: `tests/unit/ingestion/test_models.py`
- Modify: `tests/unit/ingestion/test_ownership.py`
- Modify: `tests/integration/ingestion/test_pipeline_fencing.py`

**Interfaces:**
- Consumes: ownership rows and `NormalizedIngressEvent`
- Produces: `InboxLease` with immutable `pipeline_name`; `insert(event, generation, fencing_token) -> IngressReceipt`; `claim_batch(worker_id, pipeline_names, limit, lease_seconds) -> list[InboxLease]`; rotating-token `renew(lease, lease_seconds) -> InboxLease | None`; repeatable `begin_effect(lease) -> bool`; `recover_expired_leases(limit) -> int`; `complete(lease) -> bool`; `fail(lease, error) -> InboxDisposition`; `stats() -> InboxStats`

- [ ] **Step 1: Write unit, real-PostgreSQL, architecture, and concurrent claim tests**

The test module owns a private `TypedFailure(kind: ErrorKind)` stub plus all inspection/seed helpers such as `get`, `status`, `audit_count`, `seed_lease`, `expire_for_test`, and `seed_pending_policy`; none of those helpers are production repository API. Tests cover the complete attempts boundary `old=0..6`, PostgreSQL BIGINT maximum, every `ErrorKind`, `DatabaseOperationError.retryable`, `ManualReviewRequired`, an unknown exception, and `asyncio.CancelledError` propagation. Production methods are limited to the interface above.

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
    stale = replace(leased_event, fencing_token=leased_event.fencing_token + 1)
    assert await repo.complete(stale) is False


@pytest.mark.integration
async def test_same_worker_reclaim_cannot_be_completed_by_old_lease(repo, leased_event):
    old = leased_event
    await repo.expire_and_reap_for_test(old)
    replacement = (await repo.claim_batch(
        worker_id=old.lease_owner,
        pipeline_names={old.pipeline_name},
        limit=1,
        lease_seconds=60,
    ))[0]
    assert replacement.attempts != old.attempts
    assert replacement.lease_until != old.lease_until
    assert await repo.complete(old) is False
    assert await repo.complete(replacement) is True


@pytest.mark.integration
async def test_renew_rotates_the_lease_token(repo, leased_event):
    renewed = await repo.renew(leased_event, lease_seconds=60)
    assert renewed is not None
    assert renewed.lease_until > leased_event.lease_until
    assert await repo.begin_effect(leased_event) is False
    assert await repo.begin_effect(renewed) is True


@pytest.mark.integration
async def test_retry_and_dead_letter_are_bounded(repo, leased_event):
    disposition = await repo.fail(
        leased_event, TypedFailure(ErrorKind.TRANSIENT_DEPENDENCY)
    )
    assert disposition.status == "retry_wait"
    assert disposition.available_at > leased_event.received_at
    fifth = await repo.seed_lease(attempts=5)
    disposition = await repo.fail(
        fifth, TypedFailure(ErrorKind.TRANSIENT_DEPENDENCY)
    )
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
    row = await repo.get(leased_event.id)
    if started and recovered:
        assert row.effect_started_at is not None
        assert row.status == "manual_review"
    else:
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
        leased_event, TypedFailure(ErrorKind.TRANSIENT_DEPENDENCY)
    )
    assert disposition.status == "manual_review"
    assert disposition.available_at is None
    assert await repo.audit_count(leased_event.id, "ingress.effect_unknown") == 1


@pytest.mark.integration
async def test_definite_failure_before_effect_marker_can_retry(repo, leased_event):
    disposition = await repo.fail(
        leased_event, TypedFailure(ErrorKind.TRANSIENT_DEPENDENCY)
    )
    assert disposition.status == "retry_wait"
```

- [ ] **Step 2: Implement claim SQL**

`insert()` recursively materializes the immutable payload only through `event.payload_for_storage()` before wrapping it in psycopg `Jsonb`; a real-PostgreSQL nested mapping/list round-trip test is mandatory. It takes the same per-account ownership advisory lock in shared transaction mode, so Task 3 quiesce/switch takes the exclusive form and is a real intake barrier. A new row requires the exact account/generation/fence in `current_ingress` and derives `pipeline_name` from that locked ownership fact rather than caller text. For `IGNORED` and `HISTORICAL_SUPPRESSED`, the first insert writes `status='completed'` plus one append-only `ingress.policy_suppressed` audit row in the same transaction. A duplicate dedupe key is global first-write-wins and returns the original receipt only while the same exact account/generation/fence remains `current_ingress`; Task 9G's greenfield successor supersedes the former post-quiesce replay behavior, so a quiesced or stale authority fails closed before duplicate lookup. A valid duplicate never changes payload/policy/ownership or appends another audit. Payload or policy drift never mutates durable state; if later observed through Task 11 telemetry it may use only a bounded low-cardinality counter. First `FULL` then duplicate `IGNORED` remains executable, and first `IGNORED` then duplicate `FULL` remains suppressed. A same-dedupe collision with a different account or immutable source identity fails closed with a fixed non-retryable invariant instead of disclosing another tenant's receipt.

Every repository transaction executes `SET LOCAL TRANSACTION ISOLATION LEVEL READ COMMITTED` as its first SQL statement, before timeout configuration. This is fail-closed against a pool/session default such as `REPEATABLE READ`: a candidate read performed before waiting for the account barrier cannot pin a stale ownership snapshot for the later mutation. Worker IDs, pipeline-name sets, limits and lease seconds are exact, nonempty and bounded before any pool access (`limit <= 500`, at most 64 pipelines, `lease_seconds <= 3600`). Claim first selects only the accounts represented by the next bounded candidate window, acquires their ownership advisory locks in shared mode and sorted account order, then executes the following single mutation statement restricted to those locked accounts. Thus a quiesce that wins first yields no lease, while a claim that wins first commits before quiesce can return; a statement snapshot can never publish a new lease after the barrier. The mutation still uses `SKIP LOCKED`, so workers holding compatible shared account locks claim disjoint Inbox rows concurrently. Claim writes `lease_until`, the first `processing_started_at`, and `updated_at` from one `statement_timestamp()`; expiry authority remains the moving `clock_timestamp()` boundary.

```sql
WITH claimable AS (
    SELECT e.id
    FROM event_inbox AS e
    JOIN pipeline_ownership AS p
      ON p.account_id = e.account_id
     AND p.generation = e.generation
     AND p.fencing_token = e.fencing_token
    WHERE e.status IN ('pending', 'retry_wait')
      AND e.available_at <= pg_catalog.statement_timestamp()
      AND e.processing_policy IN ('full', 'archive', 'metadata_only')
      AND e.pipeline_name = ANY(%s)
      AND e.account_id = ANY(%s)
      AND p.state IN ('current_ingress', 'draining')
    ORDER BY e.received_at, e.id
    FOR UPDATE OF e SKIP LOCKED
    LIMIT %s
)
UPDATE event_inbox AS e
SET status = 'leased',
    lease_owner = %s,
    lease_until = pg_catalog.statement_timestamp()
        + pg_catalog.make_interval(secs => %s),
    processing_started_at = COALESCE(
        e.processing_started_at,
        pg_catalog.statement_timestamp()
    ),
    safe_error_code = NULL,
    safe_error_summary = NULL,
    updated_at = pg_catalog.statement_timestamp()
FROM claimable AS c
WHERE e.id = c.id
RETURNING e.id, e.account_id, e.external_email_id, e.folder_key,
          e.source, e.raw_event_type, e.change_kind, e.dedupe_key,
          e.source_version, e.source_event_at, e.payload,
          e.processing_policy, e.pipeline_name, e.generation,
          e.fencing_token, e.lease_owner, e.lease_until, e.attempts,
          e.received_at
```

Every lease mutation first takes the shared account lock and matches the complete lease token: Inbox ID, account, pipeline, worker, attempts, exact `lease_until`, generation and fence. It also requires `status='leased'`, `lease_until > pg_catalog.clock_timestamp()` and an ownership state in `current_ingress/quiescing/draining`. This prevents same-worker ABA after expiry/reclaim and gives the reaper the complementary `lease_until <= pg_catalog.clock_timestamp()` boundary even after a statement waits on a row lock. `renew()` caps each requested TTL at 3,600 seconds, never shortens the current deadline, strictly advances it by at least one microsecond, and returns a replacement immutable lease; every pre-renew object is stale. `complete()` and `begin_effect()` return false, `renew()` returns `None`, and `fail()` raises fixed `StaleFence` after authority loss. All SQL uses explicit column lists and explicit `RETURNING` lists; `RETURNING *` is forbidden.

`fail` locks and reads the durable effect marker in the same token-bound transaction: only `effect_started_at IS NULL` may enter bounded retry/dead-letter classification. Any failure after the marker defaults to `manual_review`, clears the lease, stores a fixed outcome-unknown code and appends one idempotent safe audit; it is never scheduled automatically. A future adapter may bypass that rule only with a typed, tested proof that the remote operation definitely did not execute or uses an approved stable idempotency key—generic timeout/transport exceptions are never such proof. The explicit policy predicate is a defense in depth: even a malformed legacy `pending` ignored/historical row is never claimable. Shadow comparison rows live outside this claim path; no `event_inbox.execution_mode` column exists.

Expired leases are durable work, not permanent `leased` rows. A bounded reaper runs before claim cycles and on startup using the same account-lock-first order as retirement: it first scans at most 500 rows without row locks to discover candidate accounts, takes their shared account advisory locks in sorted order, and only then selects at most 500 expired Inbox rows with `FOR UPDATE SKIP LOCKED`. It mutates no more than the caller's validated `limit`, so both discovery/locking and writes have global bounds. Retirement's exclusive account lock therefore serializes before every Inbox row lock and can never commit `retired` concurrently with a reaper-created `retry_wait` orphan. The reaper may clean expired rows in `current_ingress/quiescing/draining`; a row attached to `retired` is never re-enqueued and instead fails closed to `manual_review` as a stale-ownership invariant. Retirement already blocks every leased row, so this branch is corruption defense rather than a normal path. If `effect_started_at IS NULL`, the reaper clears owner/deadline, increments attempts once without BIGINT overflow and applies the same capped retry/dead-letter policy as `fail()`. If the effect marker exists, it clears the lease into `manual_review` with a privacy-safe unknown-outcome code and one idempotent audit; it never retries. Each row mutation plus audit runs in a savepoint. Only a private append-and-compare audit mismatch rolls back that row, is remembered as a fixed privacy-safe invariant, and lets later rows in the bounded scan commit; after the outer transaction commits successful rows, the first fixed invariant is raised. Thus a poisoned oldest row cannot starve later work even with `limit=1`, while every other database/audit error still aborts the whole transaction. A begin-effect that linearizes before expiry may be skipped by the concurrent `SKIP LOCKED` pass and is quarantined by the next reaper; that path preserves the marker and ends only in `manual_review`, never `retry_wait`. `renew()` is a capped CAS available for the worker's entire lease lifetime, including after the effect marker. `begin_effect()` is a repeatable, idempotent authority guard: every invocation atomically requires the complete unexpired lease token, records only the first marker with `COALESCE(effect_started_at, pg_catalog.clock_timestamp())`, and returns false after reaping, renewal-token rotation or fencing. Completion/failure use the same token and fence, so an old worker cannot act after a reaper or replacement worker wins.

- [ ] **Step 3: Implement error disposition**

Before the effect marker, only `ErrorKind.TRANSIENT_DEPENDENCY` and `RATE_LIMITED`, plus an explicitly retryable `DatabaseOperationError`, may retry. `SEND_UNKNOWN` and the existing `ManualReviewRequired` go directly to `manual_review`; authentication, validation, policy, permanent-dependency, internal-invariant and unknown/untyped exceptions dead-letter. Classification uses this closed type/kind matrix and fixed repository-owned safe codes/summaries; it never persists exception text, URLs, IDs, response bodies or arbitrary `safe_code` attributes. If reading an ordinary exception's `kind` property raises any `Exception`, classification fails closed to the fixed unknown/internal disposition; exception text is not exposed and the row cannot remain leased. `CancelledError`, `SystemExit`, `KeyboardInterrupt`, and other `BaseException` control flow are never swallowed.

`attempts` is exactly the count of already committed failed or expired lease dispositions before the current claim. Insert starts at zero; claim and renew never increment it; every successful `fail()` or expiry-reaper disposition increments it exactly once, including terminal/manual dispositions, while a stale/lost CAS increments nothing. Let `new_attempts = min(old_attempts + 1, POSTGRES_BIGINT_MAX)`. With `MAX_RETRIES=5`, a retryable pre-effect failure enters `retry_wait` only when `new_attempts <= 5`; otherwise it enters `dead_letter`. Thus initial execution plus at most five retries gives six executions. Backoff is based on `new_attempts`, starts at five seconds, doubles, and caps at 900 seconds. An already-maximal counter remains maximal without PostgreSQL overflow. Task 10/`0007` adds a distinct monotonic `execution_epoch` to the lease token before any authenticated administrator requeue exists. Recovery increments that epoch, resets attempts only through its governed audited function, and can never resurrect an old `(execution_epoch,attempts,lease_until)` token; either BIGINT counter at maximum blocks recovery.

Every terminal state transition and suppression audit is in the same transaction as its Inbox mutation. Audit rows use `email_id=NULL`, `object_type='event_inbox'`, a SHA-256 fingerprint of the Inbox UUID, a fixed actor/result/reason, and bounded safe metadata containing no payload, exception text, URL, external email/folder ID, or lease owner. The deterministic event key includes Inbox UUID, action and resulting attempts. `ON CONFLICT(event_key) DO NOTHING` is followed by a read-and-compare of action, object fingerprint, result, actor, reason and canonical safe metadata; any mismatch raises a fixed invariant and rolls back the Inbox transition. Concurrent/replayed suppression, completion, dead-letter and manual-review tests prove exactly one matching audit, and an injected audit failure proves atomic rollback.

Because schema `0004` keeps `available_at NOT NULL`, terminal rows retain that physically irrelevant timestamp while the typed disposition projects `available_at=None`; only `pending/retry_wait` may use it for scheduling. `InboxStats`, repository SQL, `/queue`, metrics and drain/retire reporting expose `manual_review` as a first-class operator backlog. Exact stats intentionally use one filtered aggregate and therefore one table pass; the frozen `0004` claim/expiry indexes cannot make all exact status counts index-selective without a migration, and splitting the query would repeat scans. Task 11 must gate live activation on high-cardinality load evidence and either prove this scan fits its latency/load budget or introduce a bounded-staleness cache/incremental summary with tested reconciliation. Real-role integration coverage constructs the pool exactly like production (`autocommit=True`, `row_factory=dict_row`) from `schema.runtime_dsn` and exercises insert, nested JSONB round-trip, duplicate, claim, renew, begin-effect, complete, fail, reaper and stats. Tuple-row unit fakes remain supported, but production code never relies on test-only seed/read helpers.

`tests/architecture/test_inbox_repository_boundary.py` scans `src/` and `scripts/` and forbids direct `event_inbox` mutation outside this repository, including schema-qualified, quoted, and composed `INSERT`, `UPDATE`, `MERGE`, `DELETE`, `TRUNCATE`, and `COPY FROM`; migrations, schema manifests, and the test harness are the only exceptions. This Python boundary is supplemental and cannot prove runtime database authority. Durable intake remains dormant while runtime still has raw table DML. Task 10/`0007` must revoke every raw runtime/maintenance `event_inbox` mutation privilege and replace every repository mutation with fixed-`search_path`, source-digest-locked, narrowly granted database functions before activation can become reachable.

- [ ] **Step 4: Verify and commit**

```bash
TEST_POSTGRES_ADMIN_URL=postgresql://user:password@localhost:5432/postgres TEST_POSTGRES_ROLE_DDL=1 .venv/bin/python -m pytest tests/unit/ingestion/test_repository.py tests/integration/ingestion/test_inbox_repository.py tests/architecture/test_inbox_repository_boundary.py tests/unit/ingestion/test_models.py tests/unit/ingestion/test_ownership.py tests/integration/ingestion/test_pipeline_fencing.py -q
git add src/ingestion/repository.py src/ingestion/models.py src/ingestion/ownership.py src/ingestion/__init__.py tests/unit/ingestion/test_repository.py tests/integration/ingestion/test_inbox_repository.py tests/architecture/test_inbox_repository_boundary.py tests/unit/ingestion/test_models.py tests/unit/ingestion/test_ownership.py tests/integration/ingestion/test_pipeline_fencing.py
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
- Modify: `src/ingestion/__init__.py`

**Interfaces:**
- Consumes: a complete, unexpired `InboxLease`; a naked `NormalizedIngressEvent` is not sufficient authority to mutate an email aggregate
- Produces: immutable `EmailEventDecision(should_process, should_cancel, new_status, cancel_pending_side_effects, create_seen, reason)`; immutable `EmailEventApplication(decision, email_id, persisted_status, version, disposition, may_complete_without_processing)`; `InboxRepository.apply_email_event(lease) -> EmailEventApplication`; and `InboxRepository.transaction(connection).apply_email_event(lease) -> EmailEventApplication`

The transaction-bound form never acquires or commits a connection. It binds itself to the current top-level transaction identity and is the Phase-3 composition point for atomically applying a source deletion with approval/Outbox cancellation. The convenience wrapper opens one short `READ COMMITTED` transaction and delegates to the same primitive. Processing authorization additionally requires a first-write-wins append-only `audit_events` receipt for the exact `(inbox_id, execution_epoch, attempts)` processing attempt; `processing_inbox_id` equality alone is never execution authority. The current `0004` schema uses an explicit fixed epoch `0`; Task 10/`0007` materializes and governs epoch changes before administrator requeue exists. Task 5 adds no migration and never writes an Outbox/card relation or calls Lark/Exchange.

- [ ] **Step 1: Write ordered and out-of-order tests**

```python
@pytest.mark.parametrize(
    ("current", "event", "expected", "should_process"),
    [
        (None, "create", "processing", True),
        ("processing", "create", "processing", False),
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
        current_status="waiting_approval",
        create_seen=True,
        kind=ChangeKind.DELETE,
        source_is_read=False,
        external_effects_started=True,
    )
    assert decision.new_status == "waiting_approval"
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


def test_first_create_after_metadata_shell_atomically_elects_processing():
    decision = decide_email_event(
        current_status="ingested",
        create_seen=False,
        kind=ChangeKind.CREATE,
        source_is_read=False,
    )
    assert (decision.new_status, decision.should_process) == ("processing", True)
    assert decision.create_seen is True


@pytest.mark.integration
async def test_webhook_and_sync_create_elect_exactly_one_processor(repo):
    webhook = await claimed_lease(
        event(source="webhook", external_email_id="m1", dedupe_key="a" * 64)
    )
    sync = await claimed_lease(
        event(source="sync", external_email_id="m1", dedupe_key="b" * 64)
    )
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
    deleted = await claimed_lease(
        event(source="sync", kind="delete", external_email_id="m2")
    )
    await repo.apply_email_event(deleted)
    created = await claimed_lease(
        event(source="webhook", kind="create", external_email_id="m2")
    )
    late = await repo.apply_email_event(created)
    row = await repo.email(account_id=8, external_email_id="m2")
    assert row.status == "cancelled"
    assert row.source_deleted_at is not None
    assert row.create_seen_at is None
    assert late.should_process is False


@pytest.mark.integration
async def test_reclaimed_processing_owner_resumes_but_an_unrelated_create_does_not(
    repo, claimed_create
):
    first = await repo.apply_email_event(claimed_create)
    reclaimed = await repo.reclaim_same_inbox(claimed_create)
    retry = await repo.apply_email_event(reclaimed)
    duplicate_retry = await repo.apply_email_event(reclaimed)
    unrelated = await repo.apply_email_event(
        await repo.claim_distinct_create_for_same_aggregate(claimed_create)
    )
    assert first.disposition == "creator_elected"
    assert retry.disposition == "processing_resumed"
    assert retry.should_process is True
    assert duplicate_retry.disposition == "processing_already_elected"
    assert duplicate_retry.should_process is False
    assert duplicate_retry.may_complete_without_processing is False
    assert unrelated.disposition == "aggregate_noop"
    assert unrelated.should_process is False
    assert unrelated.may_complete_without_processing is True
```

- [ ] **Step 2: Implement pure transition function and repository CAS**

Define the complete allowed-transition table for the exact database vocabulary: `ingested`, `processing`, `retry_wait`, `manual_review`, `waiting_approval`, `notified_readonly`, `send_queued`, `sending`, `accepted`, `sent`, `send_failed`, `delivery_failed`, `send_unknown`, `no_action`, `archived`, `rejected`, `draft_saved`, `expired`, `cancelled` and `dead_letter`. A manifest test fails if either the schema CHECK or transition table gains/loses a status independently. The decision takes explicit `create_seen`, `processing_owner_matches` and the locked row's `external_effects_started_at IS NOT NULL`; caller/event defaults never supply effect authority. Only the first create sets `create_seen_at`, atomically enters `processing`, and binds the exact CREATE Inbox as `processing_inbox_id`. A retry-wait row owned by that Inbox may return to processing; manual-review/dead-letter never recover from an ordinary event. Actual `should_process=True` additionally requires the transaction to insert the unique append-only `audit_events` authorization receipt for that Inbox's exact `(execution_epoch, attempts)`. Task 5 uses fixed epoch zero. The same active lease, a renewed lease with unchanged attempt identity, or a repeat of the same reclaimed attempt cannot authorize twice; a reaper-incremented attempt may return `processing_resumed` exactly once. A same-attempt receipt hit returns `processing_already_elected` with `may_complete_without_processing=False`, so the duplicate caller cannot complete/fail/renew/begin-effect and fence the elected executor. A different CREATE Inbox is a side-effect-free `aggregate_noop` with `may_complete_without_processing=True`, because it owns a separate loser Inbox that is safe to complete. Processing election requires current `emails.version <= BIGINT_MAX-2`, reserving one increment for entering processing and one for Task 8's mandatory terminal/failure CAS. At `MAX-1`/`MAX`, an independently harmless duplicate/projection event can still no-op, but an event that would require a fresh processing election raises fixed non-retryable `email.processing_version_exhausted`; it never returns a safely completable no-op and never mints a receipt. This closes both the crash window between recording `create_seen_at` and starting work and the commit-ack/concurrent re-entry window. The application DTO validates the exact disposition/reason/processing/completion combination rather than trusting its caller. An unknown update/read creates or updates an `ingested` metadata shell with `create_seen_at=NULL` and no effect, so a later first create may still process. A CREATE on a brand-new row uses its own validated folder/read projection. A delayed CREATE winning over a pre-existing shell cannot assume it is newer: it fills unknown read data, but preserves conflicting known read/folder data and sets the existing `is_read_refresh_required` projection-refresh bit for Task 11. Task 5 may set but never clear a persisted refresh bit—even a later known READ/UPDATE or exact CREATE agreement preserves it. An unknown CREATE read value also preserves the shell and requires refresh. An unknown delete atomically creates `status='cancelled'` plus `source_deleted_at` tombstone with no effect; every later create/update/read preserves that tombstone.

Webhook and Sync use transport-specific Inbox dedupe keys, so aggregate election must not rely on Inbox dedupe. In one short transaction, `apply_email_event()` takes the shared account advisory lock, inserts only a neutral `ingested/create_seen_at=NULL/processing_inbox_id=NULL` shell with validated initial projection through `ON CONFLICT (account_id,external_email_id) DO NOTHING RETURNING ...`, then uses a second `READ COMMITTED` statement to lock/reload the elected or existing email row. A data-modifying CTE is forbidden because its old statement snapshot can miss a concurrent conflict winner; `ON CONFLICT DO UPDATE` and `SKIP LOCKED` are also forbidden. Under the email lock it locks the incoming and sticky ownership rows in generation order, then the exact Inbox lease, before running the authority-version CAS and inserting/validating the processing-attempt receipt. At this schema head that receipt has a SHA-256 event key over the literal `email-processing-attempt-v1`, Inbox ID, fixed execution epoch `0`, and attempts; it is FK-bound to the locked account/email, exact-compares epoch/generation/fence/attempts plus all fixed fields, deliberately excludes mutable `lease_until`, random surrogate ID and automatic creation timestamp, and uses insert-on-conflict followed by immutable equality comparison. Task 10 replaces the fixed zero with the governed lease epoch. The conflict loser waits for/reloads the committed row and returns a normal typed no-op; it never exposes `UniqueViolation`, retries the business event or runs a second effect. Status/create/delete/processing changes bump the business version; folder/read-only projection and a one-way external-effect marker are row-locked but leave version unchanged unless the same transaction also changes business authority, so neither can self-fence a valid effect/card. An ordinary changing authority path at `BIGINT_MAX` fails deterministically; processing election fails already at `BIGINT_MAX-1` so its terminal CAS budget is never exhausted. Real PostgreSQL concurrency tests with distinct Webhook/Sync dedupe keys and repeated identical active/reclaimed leases prove one email, one immutable `create_seen_at`, one processing authorization per attempt, a non-finalizing same-attempt duplicate and side-effect-free distinct losers. Transaction tests additionally prove top-level XID binding/reuse, non-`READ COMMITTED` rejection, outer rollback, full persisted-lease comparison, collision drift rejection and lost-commit-ack replay.

The incoming lease generation/fence is always validated in an executable state. If it differs from the sticky email owner, CREATE may process only when `create_seen_at` is already non-null and therefore becomes a no-op; a late first CREATE fails to manual review instead of transferring ownership. Cross-generation UPDATE/READ may change only folder/read projection without advancing the business version. Cross-generation DELETE may cancel an unresolved sticky owner only while that owner is current/quiescing/draining, and Phase 3 later cancels resources with the sticky generation/fence. A retired sticky owner may receive only terminal projection/deletion history; any unresolved retired row is an invariant failure. Required real-PostgreSQL tests cover the full A-draining/B-current and A-retired/B-current 2x4 CREATE/UPDATE/READ/DELETE matrix, including late-first-CREATE failure, duplicate/terminal CREATE no-op, projection-only version stability, retired-unresolved DELETE failure and terminal retired deletion-marker preservation.

The first event fixes `owner_generation` and `owner_fencing_token`. An UPDATE/READ shell created by generation A cannot later bind generation B's CREATE Inbox as processing owner: that late-first-CREATE fails closed into manual review rather than mutating sticky ownership or bypassing the processing-Inbox FK. Duplicate/terminal cross-generation CREATE events may return a no-op. Read projection never coerces transport values, never regresses known `True` to `False` under ambiguous ordering, and sets `is_read_refresh_required=True` when the schema lacks enough source-version evidence.

Phase 2 owns only the monotonic email decision and CAS: a delete sets `source_deleted_at` exactly once and returns `cancel_pending_side_effects=True` only when the current aggregate is still cancellable. It must not update a Notification/Mailbox/Send Outbox, mutate a card resource or call Lark—those relations and their race-safe cancellation transaction do not exist until Phase 3. Once external effects have started, delete preserves the current status, sets only `source_deleted_at` and returns `cancel_pending_side_effects=False`. `tests/architecture/test_phase2_delete_has_no_outbox_mutation.py` scans only the Task-5 email-event call graph; it must not flag the guarded legacy Lark/Exchange path that remains required until Phase 6. Phase 3 consumes the transaction-bound decision under the same email-first lock order and owns the real delete-versus-send-start race. Task 5 proves the cancellation intent and absence of Phase-3 mutations; the deterministic external-effect race remains a mandatory Task-10/Phase-3 integration gate, not a fake Task-5 fixture.

`manual_review`/`dead_letter` recovery requires an authenticated administrator reason and creates audit. Architecture test scans nodes/handlers and fails on direct `UPDATE emails SET status`; all mutations call the repository CAS.

- [ ] **Step 3: Verify and commit**

```bash
.venv/bin/python -m pytest tests/unit/ingestion/test_email_events.py tests/integration/ingestion/test_email_event_concurrency.py -q
git add src/ingestion/email_events.py src/ingestion/repository.py src/ingestion/__init__.py tests/unit/ingestion/test_email_events.py tests/integration/ingestion/test_email_event_concurrency.py tests/architecture/test_email_state_repository_boundary.py tests/architecture/test_phase2_delete_has_no_outbox_mutation.py
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
TEST_POSTGRES_ADMIN_URL=postgresql://user:password@localhost:5432/postgres TEST_POSTGRES_ROLE_DDL=1 .venv/bin/python -m pytest tests/unit/ingestion/test_policy.py tests/unit/ingestion/test_sync_coordinator.py tests/integration/ingestion/test_sync_atomicity.py tests/unit/ingestion/test_cold_start.py tests/integration/ingestion/test_command_receipts.py tests/unit/test_database_revision.py tests/unit/test_database_schema_contract.py tests/unit/test_checkpoint_repository_safety.py tests/unit/test_alembic_offline.py tests/integration/ingestion/test_schema.py tests/integration/ingestion/test_access_roles.py tests/integration/test_checkpoint_cleanup.py -q
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
- Create: `tests/integration/ingestion/test_email_processing_completion.py`
- Modify: `src/ingestion/repository.py`
- Modify: `src/exchange_service.py`
- Modify: `tests/unit/test_exchange_service_refactor.py`

**Interfaces:**
- Consumes: `ProcessingOutcome`, Task-5 `EmailEventApplication`, Inbox leases, Phase 1 ContentStore
- Produces: immutable `ProcessingCompletion(target_status, legacy_outcome, safe_error_code, safe_error_summary)`; invocation-scoped `LeaseAuthority.current()`/`run_with_current()`/`stop_and_freeze()`; transaction-bound/public `finish_email_processing(lease, email_id, expected_authority_version, completion)` and `finish_email_processing_failure(lease, email_id, expected_authority_version, error)`; `ProcessingAdapter`; `ProcessingAdapterRouter.select(stamped_lease, authority)`; `DurableInboxWorker.start()`, `run_once()`, `stop(grace_seconds)`

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
    authority = LeaseAuthority(lease)
    legacy_adapter.block_after_first_effect = True
    task = asyncio.create_task(worker.process_invocation(authority))
    await legacy_adapter.first_effect_started.wait()
    clock.advance(seconds=61)
    await worker.heartbeat_once(authority)
    assert await inbox.recover_expired_leases(limit=10) == 0
    legacy_adapter.release_first_effect.set()
    await task


@pytest.mark.integration
async def test_renewed_token_is_used_for_effect_and_finish(
    worker, inbox, legacy_adapter
):
    original = await inbox.seed_lease()
    authority = LeaseAuthority(original)
    renewed = await worker.heartbeat_once(authority)
    await worker.process_invocation(authority)
    assert await inbox.begin_effect(original) is False
    assert authority.current() == renewed
    assert (await inbox.get(original.id)).status == "completed"


@pytest.mark.integration
async def test_renew_and_finalize_have_one_latest_token_winner(worker, inbox, barrier):
    lease = await inbox.seed_lease()
    authority = LeaseAuthority(lease)
    barrier.pause_renew_after_cas()
    task = asyncio.create_task(worker.process_invocation(authority))
    await barrier.renew_cas_committed.wait()
    barrier.release_renew.set()
    await task
    assert (await inbox.get(lease.id)).status == "completed"
    assert await inbox.active_lease_count(lease.id) == 0


@pytest.mark.integration
async def test_effect_begin_holds_authority_across_token_bound_cas(
    worker, inbox, barrier
):
    authority = LeaseAuthority(await inbox.seed_lease())
    barrier.pause_effect_after_token_read_before_cas()
    effect = asyncio.create_task(worker.begin_effect_for_test(authority))
    await barrier.effect_token_read.wait()
    renew = asyncio.create_task(worker.heartbeat_once(authority))
    await asyncio.sleep(0)
    assert renew.done() is False
    barrier.release_effect_cas.set()
    assert await effect is True
    await renew


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


@pytest.mark.integration
async def test_success_atomically_finishes_email_and_inbox(repo, elected_lease):
    applied = await repo.apply_email_event(elected_lease)
    completed = await repo.finish_email_processing(
        elected_lease,
        applied.email_id,
        applied.version,
        ProcessingCompletion.waiting_approval(),
    )
    assert completed.email_status == "waiting_approval"
    assert completed.inbox_status == "completed"


@pytest.mark.integration
async def test_failure_atomically_moves_email_and_inbox_to_same_disposition(
    repo, elected_lease, transient_error
):
    applied = await repo.apply_email_event(elected_lease)
    failed = await repo.finish_email_processing_failure(
        elected_lease,
        applied.email_id,
        applied.version,
        transient_error,
    )
    assert failed.email_status == failed.inbox_status == "retry_wait"


@pytest.mark.integration
async def test_commit_unknown_replays_processing_completion_receipt(
    repo, elected_lease, fault
):
    applied = await repo.apply_email_event(elected_lease)
    fault.lose_ack_after_processing_commit = True
    with pytest.raises(SimulatedCommitUnknown):
        await repo.finish_email_processing(
            elected_lease,
            applied.email_id,
            applied.version,
            ProcessingCompletion.no_action(),
        )
    replay = await repo.finish_email_processing(
        elected_lease,
        applied.email_id,
        applied.version,
        ProcessingCompletion.no_action(),
    )
    assert replay.replayed is True
    assert await repo.processing_completion_receipt_count(elected_lease.id) == 1


@pytest.mark.integration
@pytest.mark.parametrize("blocker", ["stale_version", "source_deleted"])
async def test_completion_rejects_stale_version_or_source_delete_atomically(
    repo, elected_lease, blocker
):
    applied = await repo.apply_email_event(elected_lease)
    await repo.arrange_completion_blocker(applied.email_id, blocker)
    with pytest.raises(ProcessingCompletionRejected):
        await repo.finish_email_processing(
            elected_lease,
            applied.email_id,
            applied.version,
            ProcessingCompletion.no_action(),
        )
    assert await repo.processing_pair(applied.email_id) == (
        "processing",
        "leased",
    )


@pytest.mark.integration
async def test_completion_receipt_rejects_changed_result(repo, elected_lease):
    applied = await repo.apply_email_event(elected_lease)
    await repo.finish_email_processing(
        elected_lease,
        applied.email_id,
        applied.version,
        ProcessingCompletion.no_action(),
    )
    with pytest.raises(IdempotencyConflict):
        await repo.finish_email_processing(
            elected_lease,
            applied.email_id,
            applied.version,
            ProcessingCompletion.archived(),
        )


@pytest.mark.integration
async def test_nonterminal_completion_never_lands_at_bigint_max(repo):
    lease, applied = await repo.elect_create_from_version(BIGINT_MAX - 2)
    result = await repo.finish_email_processing(
        lease,
        applied.email_id,
        applied.version,
        ProcessingCompletion.waiting_approval(),
    )
    assert (result.email_status, result.inbox_status) == (
        "dead_letter",
        "dead_letter",
    )
    assert await repo.approval_authority_count(applied.email_id) == 0


@pytest.mark.integration
async def test_expiry_reaper_updates_email_and_inbox_without_lock_inversion(
    repo, elected_lease, clock
):
    applied = await repo.apply_email_event(elected_lease)
    clock.advance(seconds=61)
    reap, late_apply = await asyncio.gather(
        repo.recover_expired_processing(limit=10),
        repo.apply_email_event(elected_lease),
        return_exceptions=True,
    )
    assert not any(
        isinstance(value, psycopg.errors.DeadlockDetected)
        for value in (reap, late_apply)
    )
    assert await repo.processing_pair(applied.email_id) in {
        ("retry_wait", "retry_wait"),
        ("manual_review", "manual_review"),
        ("dead_letter", "dead_letter"),
    }


@pytest.mark.integration
@pytest.mark.parametrize(
    ("effect_started", "expected"),
    [(False, "retry_wait"), (True, "manual_review")],
)
async def test_reaper_projects_pre_and_post_effect_to_both_rows(
    repo, elected_lease, clock, effect_started, expected
):
    applied = await repo.apply_email_event(elected_lease)
    if effect_started:
        assert await repo.begin_effect(elected_lease) is True
    clock.advance(seconds=61)
    await repo.recover_expired_processing(limit=10)
    assert await repo.processing_pair(applied.email_id) == (expected, expected)


@pytest.mark.integration
async def test_expired_max_minus_one_processing_dead_letters_both_rows(
    repo, clock
):
    lease, applied = await repo.elect_create_from_version(BIGINT_MAX - 2)
    assert applied.version == BIGINT_MAX - 1
    clock.advance(seconds=61)
    await repo.recover_expired_processing(limit=10)
    assert await repo.processing_pair(applied.email_id) == (
        "dead_letter",
        "dead_letter",
    )


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

`ProcessingCompletion.target_status` is exact and may be only `waiting_approval`, `notified_readonly`, `no_action` or `archived`; `ProcessingOutcome.PROCESSED` is never blindly mapped without the adapter's persisted legacy-result projection, `ARCHIVED` maps only to `archived`, and an unexpected `DUPLICATE` while the new aggregate is `processing` fails to manual review. Success locks in the global data-plane order, revalidates the exact processing attempt receipt, email ID/version/status/processing Inbox, lease, authority/ownership and absence of source deletion, then atomically moves the email to the target status, clears processing owner/error fields, completes the Inbox, appends audit and a unique canonical completion receipt. Same-key/same-result commit-unknown retries replay that receipt; changed results conflict. A nonterminal completion may never consume the final BIGINT version: `waiting_approval` requires its resulting version to remain strictly below `BIGINT_MAX`. If the only remaining CAS would produce `waiting_approval@BIGINT_MAX`, the same transaction instead uses that last CAS to put both email and Inbox in `dead_letter` with fixed safe code `email.version_exhausted_before_nonterminal_completion`, writes the canonical failure receipt/audit, and creates no approval/card/Outbox authority. Terminal `notified_readonly`, `no_action` and `archived` may use the final version because they require no later business CAS.

The failure form reuses Task 4's closed failure classifier and performs the email plus Inbox transition in the same transaction. `retry_wait`, `manual_review` and `dead_letter` retain the exact processing Inbox and write the same fixed safe error facts required by both schema matrices; Inbox attempts/backoff and email authority version advance together. A retry disposition is allowed only when the resulting email version remains `<= BIGINT_MAX-2`; otherwise that failure becomes terminal `dead_letter` in the same last available CAS. A later reclaimed `retry_wait` lease uses Task 5's exact `(inbox_id,execution_epoch,attempts)` receipt to return to `processing`; epoch is zero before `0007`, while authenticated administrative recovery later increments it before resetting attempts. Manual-review/dead-letter never recover through an ordinary event. Completion tests cover stale expected version, source deletion, changed-result receipt conflict and commit-unknown replay. No path may complete/fail the Inbox first and leave `emails.status='processing'`, or commit an email terminal state while the Inbox remains leased. Phase 3 extends the same transaction to enqueue business Outboxes.

Before Task 8 can activate a Worker, it replaces the Task-4 Inbox-only expiry mutation for processing-linked rows; it must never extend the old `Inbox FOR UPDATE -> email` path. The aggregate-aware reaper first performs a bounded no-row-lock candidate scan, acquires shared account locks in ascending order, and for each candidate takes email -> runtime authority when present -> required ownership rows -> exact Inbox. Only then does it recheck the complete expired lease token and atomically move both email and Inbox to the same `retry_wait`/`manual_review`/`dead_letter` disposition with version/attempt/audit facts. Its pre-effect retry uses the same capacity rule as explicit failure: `retry_wait` is legal only when the resulting email version remains `<= BIGINT_MAX-2`; otherwise the last available CAS atomically moves both rows to `dead_letter`, so no `retry_wait@BIGINT_MAX` can become permanently unelectable. Each candidate retains Task 4's savepoint poison isolation. A lease with no processing-linked email may remain on the lease-only account -> ownership -> Inbox path, but that branch can never acquire an email later. Real PostgreSQL tests repeatedly race aggregate apply/failure/reap, include expired `processing@BIGINT_MAX-1`, and require matching email/Inbox dispositions, bounded progress and zero deadlocks.

For executable work, the adapter applies the event state decision and invokes the legacy path only when `should_process=True`. A result with `may_complete_without_processing=True` is an owned metadata/tombstone/distinct-CREATE no-op and may be completed immediately. `processing_already_elected` has `may_complete_without_processing=False`: the duplicate caller returns without complete/fail/renew/begin-effect, leaving the elected invocation or reaper as the sole lease-lifecycle owner.

Each elected invocation owns one `LeaseAuthority`, not a captured immutable lease forever. The heartbeat accepts that authority, serializes renewal through it and atomically replaces its current token with the returned `InboxLease`; production code has no `heartbeat_once(immutable_lease)` shortcut. An old token becomes unreachable immediately. Every token-bound effect guard uses `run_with_current()` and holds the authority mutex continuously from reading the latest token through commit/rollback of the complete repository `begin_effect()` CAS; it releases the mutex before the remote network call. Heartbeat therefore cannot renew in the snapshot-to-CAS window and make the effect self-fence. Before terminal success/failure, the worker signals heartbeat stop, awaits any in-flight renewal, freezes the final latest token, and uses exactly it for the email+Inbox CAS. A renewal loss sets cancellation immediately; no later effect or finalize may begin. This protocol gives renew-vs-effect and renew-vs-finalize one current-token winner and prevents a successful renewal from making either path use its stale predecessor. Tests prove old-token rejection, renew->effect->finish success, mutex coverage across the token-bound effect CAS, renewal/finalize serialization and no leased orphan.

The dedicated heartbeat continues at a bounded interval through pre-effect work, `effect_started_at` and all in-flight outbound calls until that stop-and-freeze boundary. Immediately before every outbound dependency/effect boundary, the adapter must win the repeatable repository `begin_effect(latest_lease)` guard; a false result stops execution. Because the existing `process_and_archive_email()` is a monolith, this task must either split its outbound boundaries or inject and await a typed `before_external_effect(kind)` guard immediately before ContentStore, model, Feishu, Exchange and Qdrant calls—the worker may not wrap only the outer function. A real process crash is recovered only after lease expiry: before-marker work retries boundedly, while after-marker work becomes `manual_review`, never blind retry. An in-process timeout or transport error after the marker follows the same `manual_review` path immediately and must not call the normal retry branch or start a later effect. A healthy heartbeat prevents a long marked operation from being reaped; a lost heartbeat permits the aggregate-aware reaper to move both linked rows to `manual_review` and the latest-token guard prevents every subsequent side effect.

- [ ] **Step 3: Verify and commit**

```bash
TEST_POSTGRES_ADMIN_URL=postgresql://user:password@localhost:5432/postgres TEST_POSTGRES_ROLE_DDL=1 .venv/bin/python -m pytest tests/unit/ingestion/test_worker.py tests/integration/ingestion/test_webhook_crash_recovery.py tests/integration/ingestion/test_email_processing_completion.py tests/unit/test_exchange_service_refactor.py -q
git add src/ingestion/processing.py src/ingestion/legacy_adapter.py src/ingestion/worker.py src/ingestion/repository.py src/exchange_service.py tests/unit/ingestion/test_worker.py tests/integration/ingestion/test_webhook_crash_recovery.py tests/integration/ingestion/test_email_processing_completion.py tests/unit/test_exchange_service_refactor.py
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
        target_email_status="processing",
        cancel_pending_side_effects=False,
        reason_code="first_create",
        normalizer_contract_hash="b" * 64,
        policy_config_hash="c" * 64,
    )
    assert canonical_shadow_decision_hash(decision) == (
        "50d215b241c34df75546ae83db7ef7131421291190a37e29f39df3a3d5a49261"
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
- Modify: `src/domain/email_state.py`
- Modify: `src/ingestion/repository.py`
- Modify: `src/ingestion/ownership.py`
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
- Modify: `tests/unit/ingestion/test_ownership.py`
- Modify: `tests/integration/ingestion/test_pipeline_fencing.py`
- Modify: `tests/integration/ingestion/test_inbox_repository.py`
- Modify: `tests/architecture/test_inbox_repository_boundary.py`
- Modify: `tests/architecture/test_pipeline_ownership_boundary.py`
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

Migration `20260713_0007` is linear from `20260713_0006`; Phase 3 therefore starts at `0008`. It creates seven governed control tables, adds `sync_cold_start_plans.target_reservation_id`, and adds `event_inbox.execution_epoch BIGINT NOT NULL DEFAULT 0` with an explicit nonnegative/bounded check. `execution_epoch` becomes part of every exact `InboxLease` token and processing-attempt/completion receipt. The only administrator requeue function atomically transitions the linked Inbox and `emails` aggregate from `manual_review`/`dead_letter` to `retry_wait`, increments the Inbox epoch, resets attempts to zero, clears stale lease ownership/deadline, advances email authority version, and records the authenticated reason/command receipt/audit. It requires the locked current `emails.version <= BIGINT_MAX-3`, reserving one increment for recovery, one for re-entering processing and one for terminal completion. If any external marker exists, it additionally requires completed reconciliation evidence and zero unreconciled `started/unknown` effect identities. It refuses recovery when the epoch or attempts is already `BIGINT_MAX` or the email version lacks that three-CAS budget; ordinary claim, renew, fail and reap never reset the epoch.

- `pipeline_runtime_authority`: one versioned row per account with mode (`legacy_authoritative/shadow/quiescing/durable_active`), monotonic `authority_epoch`, ownership generation/fence/pipeline FK, policy readiness/config hash, minimum numeric build, minimum protocol, actor/reason and CAS version.
- `pipeline_runtime_instances`: per-account process lease with instance/workload/deployment IDs, numeric build plus display build ID, protocol/config/profile, lifecycle (`standby/active/draining`), observed epoch, legacy queue/in-flight/effect counts, heartbeat and lease deadline. Build ordering uses a CI-produced integer; Git SHA is never ordered lexically.
- `pipeline_target_reservations`: one active reservation per account/live barrier with an immutable target pipeline name, preallocated generation/fencing token, build/protocol/config hash, zero-work standby roster hash, expiry, state (`reserved/promoted/cancelled/expired`) and its reserved `pipeline_ownership` FK. Migration `0007` extends the ownership state contract with an explicit non-executable `reserved` state; it must never overload `quiescing`. `prepare_target()` takes the account advisory lock, creates exactly one `reserved` ownership row and allocates the final generation/fence exactly once. The only ownership exits are `reserved -> current_ingress` during the atomic promotion or `reserved -> retired` during cancellation/expiry; direct `reserved -> quiescing|draining` is rejected. Folder plans and the live barrier reference this row by FK. Promotion never rotates the fence. Cancel/expiry under the same lock retires the unused ownership row and releases only the active-reservation slot; generation/fence are never reused.
- `pipeline_legacy_effects`: account/event-key/authority-epoch/generation/fence/instance/effect-kind/deterministic-ordinal/target-hash registrations with immutable identity, state (`started/completed/unknown/reconciled`), reconciliation disposition, idempotency/evidence hashes and timestamps. A UNIQUE constraint covers `(account_id,event_key,authority_epoch,effect_kind,ordinal,target_hash)`; ordinal identifies an intended distinct effect in the deterministic pipeline, never a retry attempt. The only transitions are `started -> completed|unknown` and maintenance-only `unknown -> reconciled`; completed/reconciled are terminal. UPDATE of identity/effect kind/ordinal/target/initial token, DELETE and TRUNCATE are rejected. A crash never deletes or auto-expires a started/unknown outcome.
- `pipeline_cutover_barriers` and `pipeline_cutover_barrier_members`: immutable predecessor barrier, target mode/config/build/protocol, exact external deployment roster and revision/hash, expected instance count, exact completed backfill-plan and sealed Shadow-evidence FKs plus their source/cutoff/high-water/build/config/count/hash facts, legacy nonterminal/quarantine counts and hash, versioned `exchange_sync_contract_v2` build/profile/page/continuation/read-flag probe hash, the complete configured `FolderScope` manifest plus one exact target-bound active cursor or unexpired approved cold-start boundary per folder and its rolling hash, a fresh terminal projection-refresh cycle hash proving zero unresolved read/folder refresh rows at those cursor high-waters, expiring LB isolation, effect-secret rotation and legacy-DB-connection isolation proof hashes, and nullable Phase-3 approval/outbox/legacy-card/adapter-manifest evidence hashes that are mandatory for a `durable_active` target row. Barrier state is the strict sequence `planned -> fence_verified -> quiesced -> drained -> evidence_frozen -> target_standby -> legacy_isolated -> proof_fresh -> ready -> consumed`, with only pre-ready cancellation allowed.
- `legacy_backfill_plans`: account, ownership generation/fence, mapping/config versions, source count/high-water/rolling hash, legacy-nonterminal count, exact quarantine count/hash, target count/reconciliation hash, applied cursor/hash/counts, status, actor/reason and timestamps; it stores no email body, draft, response or raw identifier sample. Idempotency lives in the shared append-only command receipts, not a mutable key column on the plan.

All hashes are exact lowercase SHA-256, counters are bounded BIGINT, metadata is a bounded object, open barrier/backfill identities are unique, and forward-only state/identity guards prevent history rewrite. Before any activation API exists, `0007` revokes runtime's direct `INSERT`/`UPDATE` authority over `pipeline_ownership`, `event_inbox`, and `emails`. It also revokes **all** direct table- and column-level `INSERT` privileges on `audit_events` from runtime and maintenance; PostgreSQL ACL cannot safely grant by `action` namespace. Every legal runtime or maintenance audit append—including Task-5 processing authorization—must occur only inside a source-digest-locked, fixed-`search_path`, narrowly granted `SECURITY DEFINER` state-transition function that constructs the fixed action/result/actor/reason/metadata itself. No general `append_audit` entry point accepts caller-selected namespaces.

Bootstrap, quiesce, target preparation/promotion/cancellation, evidence-backed retirement, Inbox insert/claim/renew/begin/complete/fail/reap, monotonic email-event apply/status CAS with its exact `(inbox_id,execution_epoch,attempts)` receipt, and authenticated administrator recovery all use those governed functions. `PipelineOwnershipRepository` and `InboxRepository` are updated in this task to call the governed entry points; their Phase-2 direct DML implementations cannot remain reachable after the revoke. Inbox and email functions enforce the Task-4/Task-5 transition matrices, exact epoch-bearing lease token, expiry boundary, attempts delta/overflow rule, processing-policy whitelist, ownership state/fence, immutable identity, audit equality and advisory-lock order inside the database. The global partial order is: shared account advisory lock -> email -> runtime authority -> required ownership/reservation rows in ascending generation order -> exact Inbox/card -> Notification -> Mailbox -> Send/effect -> audit/receipt. A lease-only function may use shared account -> ownership -> Inbox only if it never locks an email later. Freeze/switch/control takes the exclusive account lock and then authority -> ownership, so it cannot overlap either data-plane class; multi-account operations acquire account locks in ascending account order. General runtime DML, ad-hoc SQL, or a Python-only service cannot create a second current row, forge/reopen/steal Inbox work, mint a processing authorization, reopen terminal mail, promote an unreserved target, rotate a reserved fence, skip an audit/receipt, or retire without evidence. If a separate control credential is introduced instead, it must be explicit in the ACL/bootstrap contract and no broader than those same operations; it cannot inherit migration/maintenance power.

Runtime otherwise receives SELECT on authority/barriers/Inbox/effect facts, EXECUTE only on the exact Inbox, email, effect and ownership functions needed by its role, and INSERT plus only heartbeat/lease/counter UPDATE columns on its own instance registrations. It has no direct `pipeline_legacy_effects` INSERT/UPDATE: narrowly separated begin/finish functions create a fixed identity and close only the caller's exact started token. It has no raw Inbox/email/ownership/audit mutation, reconcile, DELETE, TRUNCATE, barrier or authority mutation privilege. Maintenance receives only separately named recovery/reaper/reconciliation functions plus the control transitions it owns; it cannot use runtime claim/effect functions, directly insert audit/effect rows, rewrite identity or reopen a terminal row without the authenticated recovery contract. Auditor has SELECT only; migration owns DDL. Real-role tests prove raw runtime and maintenance Inbox/email/audit/effect `INSERT`, `UPDATE`, `MERGE`, `DELETE`, `TRUNCATE`, and `COPY FROM` are rejected—including `audit_events` table and column INSERT—while each narrow function can append only its fixed audit/effect. They also prove every allowed and forbidden governed transition, concurrent promotion/replay/reclaim, stale lease/reservation/fence/epoch, changed-payload recovery conflict, audit mismatch and rollback. Real PostgreSQL deadlock gates run apply-vs-reap, delete-vs-effect, approval-vs-delete, resolution-vs-delete and switch-vs-data-plane races repeatedly with bounded lock timeouts and require zero deadlocks plus one legal serialized outcome. Architecture scans are supplementary and cannot substitute for database ACL/behavior tests. The same task updates exact schema/function-source digests, all four ACL manifests, checkpoint revision allowlists, bootstrap/offline SQL and proves a code-first real-PostgreSQL `0006 -> 0007` bridge with all activation profiles disabled.

- [ ] **Step 3: Implement the authority state machine and mandatory legacy fence**

Environment flags are never authority; they become only a requested runtime profile in Task 11. Phase 2 implements and exposes only `legacy_authoritative -> quiescing -> shadow` plus a pre-switch cancellation back to the prior legacy/Shadow mode. The schema reserves `durable_active`, but `RuntimeAuthorityRepository`, Phase-2 CLI and runtime must reject every Phase-2 attempt to enter it with `phase3_activation_required`. Phase 3 alone adds the transition `shadow -> quiescing -> durable_active` after fenced approval/send readiness; later rollback is `durable_active -> quiescing -> durable_active` with a new `legacy_compat` generation and can never resurrect the old in-memory queue or Shadow as authority.

Every legacy Webhook/poller/self-healer item is stamped at intake with account, event key, email ID and expected email version, authority epoch, generation and fence. `process_and_archive_email()` keeps the Task 8 typed `before_external_effect(kind, ordinal, target_hash)` hook mandatory in production. That hook cannot be a read-only authorize-then-call check: `try_begin_effect()` takes the shared per-account transaction advisory lock later taken exclusively by Phase 3 switch, then uses the fixed row-lock order email -> authority/ownership -> exact effect identity. Under the email lock it re-reads version, `create_seen_at`, status and `source_deleted_at`; a deleted/tombstoned aggregate or version that no longer authorizes work rejects before inserting an effect. Only an absent identity for a still-authorized aggregate may insert `state='started'`; in that same transaction it writes `emails.external_effects_started_at = COALESCE(external_effects_started_at, clock_timestamp())`, then returns an execute token only after commit. Setting that one-way marker alone does not advance the business version or invalidate the effect it just authorized. Every later Phase-3 Notification/Mailbox/Send begin guard has the same email-first responsibility. DELETE derives `external_effects_started` only from this locked persisted marker.

If effect-begin wins the delete race, that one registered call may finish outcome-known/unknown, but delete records `source_deleted_at` and every later effect begin re-locks/rechecks the email and is rejected. If delete wins, no remote call begins. A completed identity returns `already_completed` and the caller skips the remote call; `started` or `unknown` fails closed with `ManualReconciliationRequired` and can never call remotely again. A reconciled-completed identity also skips; a reconciled-not-executed outcome requires a separate maintenance-authorized new ordinal rather than reopening the old row. ContentStore, model, Feishu, Exchange and Qdrant boundaries each begin and finish their own registration. Runtime may close its own started row as completed/unknown but cannot reconcile; stale started rows become unknown only through maintenance, and unknown requires explicit maintenance reconciliation evidence. A crash after the remote effect but before `finish()` therefore leaves a durable started row; Webhook retry/restart sees it and never repeats the effect. Real PostgreSQL begin-vs-delete tests assert that a committed execute token always has both the effect row and email marker, while a winning delete yields zero token/remote calls. Phase 3 switch takes the exclusive account lock and requires zero started/unknown registrations, giving effect-begin versus delete and epoch-switch one serialized winner with no authorization-to-call TOCTOU window.

The Worker additionally re-runs the Task-8 stamped adapter selection before each lease and each later effect boundary. `legacy_compat` may execute only for the matching legacy-authoritative/Shadow stamp or an explicitly draining old generation, always through the exact effect registration above. A target/durable stamp can never select that adapter; until Phase 3 registers an exact build/config `DurableProcessingAdapter`, it remains standby and claim-ineligible.

Quiescing stops new Webhook intake, polling, Sync, Shadow intake and new Durable claims but allows already-stamped legacy work to drain and begin registered effects while its epoch remains current. Phase 2 may complete only the side-effect-free Shadow transition; after P4-P6 capability successors and real v2 proof, the Phase-3 switch transaction alone increments the epoch and atomically changes ownership/authority, consumes the latest `production_ready` successor, appends the `pipeline.switch` command receipt and audit, and fences every old legacy token. Architecture tests forbid unguarded production calls from `server.py`, `exchange_service.py`, `polling.py` and `self_healing.py`; `tests/architecture/test_phase2_activation_boundary.py` additionally fails if a Phase-2 module can call a durable-active transition or start Durable intake/claim/Sync.

Instance registry cannot prove that a pre-protocol build which never registers is absent. Activation therefore additionally requires an external deployment/LB roster covering HTTP, Worker, poller, cron and jobs; all roster members must have fresh matching leases, and no extra active instance may exist. Only after `prepare_target()` persists the reservation may target-profile processes register against its exact generation/fence as `standby`: they heartbeat build/protocol/config/roster evidence but start zero intake, claim, scheduler or effect work. This breaks the activation handshake deadlock; after the atomic switch promotes that same ownership row without changing its fence, they observe the new epoch and become active, while old instances are fenced.

The live cutover stages are deliberately ordered and cannot be skipped: (1) obtain real v2 proof, prove every serving build fence-aware and create the distinct live barrier in `planned`; (2) `prepare_target(live_barrier_id)` under the account lock and register its exact zero-work standby roster; (3) seal/approve every FolderScope boundary against the reservation while legacy remains authoritative; (4) enter card freeze/quiescing; (5) drain/quarantine legacy work, finish old-card invalidation and freeze exact backfill/Shadow evidence; (6) remove old workloads/LB routes; (7) rotate effect/DB credentials and terminate old connections; (8) import fresh short-lived roster/isolation proof; (9) mark the live barrier ready; and (10) Phase 3 immediately promotes the reserved ownership row before proof expiry. State-machine tests reject reordering, a changed barrier/reservation FK/fence, any standby work before promotion, or switch-time fence rotation.

Every legacy source nonterminal row must either be absent at the frozen source high-water or have exactly one deterministic completed BACKFILL/HISTORICAL_SUPPRESSED quarantine Inbox fact covered by the backfill plan's count/hash. Known legacy approval/Lark cards additionally produce a bounded `legacy_card_invalidation_required` audit fact; Phase 2 does not claim they are invalidated during implementation. Phase3-P6 implementation capability successors may reference readiness/capability evidence but do not require or trigger live invalidation/quiesce/isolation. Only the later `production_ready` snapshot FK-binds a distinct live barrier and must include Phase-3 Outbox/action contracts plus completed old-card invalidation. Live readiness and final switch independently re-read the exact backfill, Shadow and real-v2 evidence. Pending/failed/diverged Shadow rows, a sample window below 1,000 events/seven days, mixed build/config/schema, unquarantined legacy state, the known-incompatible extension, missing v2 proof, reservation/version/profile drift, stale high-water or any missing/expired live proof blocks production progress without breaking current legacy service.

- [ ] **Step 4: Verify and commit**

```bash
TEST_POSTGRES_ADMIN_URL=postgresql://user:password@localhost:5432/postgres TEST_POSTGRES_ROLE_DDL=1 .venv/bin/python -m pytest tests/unit/ingestion/test_runtime_authority.py tests/integration/ingestion/test_runtime_activation.py tests/integration/ingestion/test_inbox_repository.py tests/architecture/test_phase2_activation_boundary.py tests/architecture/test_pipeline_ownership_boundary.py tests/architecture/test_inbox_repository_boundary.py tests/unit/ingestion/test_ownership.py tests/integration/ingestion/test_pipeline_fencing.py tests/unit/test_database_revision.py tests/unit/test_database_schema_contract.py tests/unit/test_checkpoint_repository_safety.py tests/unit/test_alembic_offline.py tests/integration/ingestion/test_schema.py tests/integration/ingestion/test_access_roles.py tests/integration/test_checkpoint_cleanup.py -q
git add alembic/versions/20260713_0007_runtime_activation.py src/domain/email_state.py src/ingestion/repository.py src/ingestion/ownership.py src/ingestion/runtime_authority.py src/ingestion/cutover_barrier.py src/exchange_service.py src/server.py src/scheduler/polling.py src/utils/self_healing.py src/db/access_contract.py src/db/bootstrap.py src/db/schema.py src/db/schema_contract.py src/maintenance/checkpoint_repository.py tests/unit/ingestion/test_runtime_authority.py tests/unit/ingestion/test_ownership.py tests/integration/ingestion/test_runtime_activation.py tests/integration/ingestion/test_pipeline_fencing.py tests/integration/ingestion/test_inbox_repository.py tests/architecture/test_phase2_activation_boundary.py tests/architecture/test_pipeline_ownership_boundary.py tests/architecture/test_inbox_repository_boundary.py tests/unit/test_database_revision.py tests/unit/test_database_schema_contract.py tests/unit/test_checkpoint_repository_safety.py tests/unit/test_alembic_offline.py tests/integration/ingestion/test_schema.py tests/integration/ingestion/test_access_roles.py tests/integration/test_checkpoint_cleanup.py
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
- Create: `src/ingestion/read_refresh.py`
- Create: `src/scheduler/sync_reconciliation.py`
- Create: `scripts/backfill_durable_ingestion.py`
- Create: `scripts/manage_pipeline.py`
- Create: `tests/unit/ingestion/test_runtime.py`
- Create: `tests/unit/ingestion/test_cutover.py`
- Create: `tests/unit/ingestion/test_backfill.py`
- Create: `tests/integration/ingestion/test_sync_resource_isolation.py`
- Create: `tests/integration/ingestion/test_sync_contract_probe.py`
- Create: `tests/integration/ingestion/test_read_refresh.py`
- Create: `tests/architecture/test_phase2_runtime_stays_standby.py`
- Modify: `src/config.py`
- Modify: `src/init_app.py`
- Modify: `src/main.py`
- Modify: `src/commands/handlers.py`
- Modify: `src/ingestion/repository.py`
- Modify: `src/observability/metrics.py`
- Modify: `.env.example`
- Modify: `tests/unit/test_metrics.py`
- Modify: `tests/unit/test_command_router.py`
- Replace: `src/scheduler/polling.py`
- Replace: `tests/unit/test_polling_scheduler.py`

**Interfaces:**
- Produces: one Phase-2 `IngestionRuntime`; DB-backed `/queue`/metrics including manual review; `BackfillService.plan/execute`; versioned read-only `SyncContractProbe`; bounded `ProjectionRefreshService` plus transaction-bound `apply_projection_refresh`; `CutoverReadinessService.plan/quiesce/shadow_switch/ready/drain_status`; CLI exit codes 0 success, 2 blocked, 3 invariant failure

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


@pytest.mark.integration
async def test_authoritative_refresh_can_clear_true_and_move_folder_without_version_bump(
    projection_refresh, db, fixed_v2_extension_fixture
):
    row = await db.seed_email(
        is_read=True,
        is_read_refresh_required=True,
        source_folder_key="INBOX",
        version=7,
    )
    fixed_v2_extension_fixture.detail_current.return_value = {
        "is_read": False,
        "folder": "ARCHIVE",
    }
    evidence = await projection_refresh.run_account(row.account_id)
    refreshed = await db.email(row.id)
    assert (refreshed.is_read, refreshed.source_folder_key) == (False, "ARCHIVE")
    assert refreshed.is_read_refresh_required is False
    assert refreshed.version == 7
    assert evidence.final_sync_terminal is True
    assert evidence.unresolved_count == 0


@pytest.mark.integration
async def test_change_during_refresh_is_reconciled_before_evidence_seals(
    projection_refresh, db, fixed_v2_extension_fixture
):
    row = await db.seed_refresh_required_email(is_read=True)
    fixed_v2_extension_fixture.change_after_detail(row.external_email_id, is_read=False)
    evidence = await projection_refresh.run_account(row.account_id)
    assert evidence.sealed is False
    assert (await db.email(row.id)).is_read_refresh_required is True
```

- [ ] **Step 2: Lock requested profiles and lifecycle**

Add `DURABLE_INBOX_ENABLED`, `INGESTION_SHADOW_ENABLED`, `SYNC_RECONCILIATION_ENABLED`, `SYNC_INTERVAL_SECONDS=300`, `SYNC_BATCH_SIZE=500`, bounded pages/run seconds, Worker lease/concurrency/attempt/grace settings, runtime instance ID/build number/build ID/protocol and heartbeat/lease settings. Add a dedicated Sync PostgreSQL pool (`SYNC_DB_POOL_SIZE=2`), a dedicated Exchange Sync HTTP pool (`SYNC_HTTP_MAX_CONNECTIONS=2`) and nonblocking global folder budget (`SYNC_MAX_CONCURRENT_FOLDERS=2`). These resources are separate from the main Webhook/Worker database and HTTP capacity. If no folder permit or Sync connection is immediately available, the scheduler records a bounded `busy_skip` fact and does not enqueue an unbounded waiter. Cancellation must close the streaming response, advisory-unlock on the same connection, return DB/HTTP connections and release the permit in `finally`.

The only valid requested flag tuples are the four tests above, but in Phase 2 `durable`/`durable_sync` may only advertise standby capability: they never authorize work. Requested profiles must match DB authority mode, minimum build/protocol and policy hash before any legacy or Shadow component starts.

DB mode is authoritative: legacy permits only old guarded Webhook/poller; Shadow keeps legacy authoritative and allows side-effect-free candidate comparisons; quiescing permits no new intake/claim; the schema's durable mode is unavailable to Phase-2 production code. Durable candidate processes register only as standby with zero intake, claim, Sync, scheduler and effect work. `tests/architecture/test_phase2_runtime_stays_standby.py` scans `main.py`, `server.py`, runtime and CLI wiring and fails if Phase 2 can expose Durable 202, start Durable Worker claims or execute Sync writes. Phase 3 later authorizes those paths only after the latest complete `production_ready` successor.

Startup may probe Sync permission for every FolderScope, not only Inbox, but the probe never writes cursor/Inbox and does not start the five-minute scheduler in Phase 2. `AppContext` owns one runtime; shutdown stops Shadow schedules, drains boundedly, heartbeats the draining state, then closes dedicated Sync and main clients. The former `/list` loop cannot process directly; any compatibility import delegates to the authority-guarded legacy or dormant coordinator and cannot bypass the Phase-2 activation boundary.

Ambiguous event ordering deliberately leaves `emails.is_read_refresh_required=True`; Task 11 supplies the only authoritative clearing path. `ProjectionRefreshService.run_account()` loads the immutable configured `FolderScope` manifest and acquires **every** corresponding Sync session advisory lock in ascending canonical-key order on one dedicated connection before it may clear a flag. It first reaches an authenticated terminal Sync high-water for every scope, freezes the full folder->cursor manifest/hash, fetches a bounded batch of current message detail using read-only Exchange calls outside every database transaction, then calls transaction-bound `apply_projection_refresh(account_id, external_email_id, observed_source_folder_key, observed_is_read, base_cursor_manifest_hash, observation_hash)`. The repository takes shared account -> email -> authority/ownership, revalidates the exact all-scope cursor manifest and a strict boolean/current canonical folder, may authoritatively change `True` back to `False`, clears the refresh flag, and updates only projection/`updated_at`; it never increments the business version, changes status/owner/content, inserts an Inbox effect, or calls an external mutation. A single-folder background pass may detect/set ambiguity but cannot clear it or seal evidence.

Before sealing evidence, the service performs a final bounded Sync catch-up for **every configured scope** while retaining the complete canonical lock set. Any source or destination-folder change that occurred during the detail window is therefore applied after the refresh; if that event makes ordering ambiguous again, the flag remains set and the cycle cannot seal. It re-reads and hashes the final all-scope cursor manifest before releasing locks. Deleted/not-found messages use the ordinary source-delete Inbox path rather than a projection shortcut. Timeout, malformed detail, cursor/manifest drift, ownership drift, lock loss or partial coverage preserves the flag and blocks evidence. Activation readiness requires a fresh hash over the exact folder cursor high-waters, bounded candidate/result counts and `unresolved_count=0` across every configured scope; folder projection is explicitly best-effort during normal ingestion but fully reconciled at this gate. Tests prove canonical all-lock acquisition/release, INBOX->ARCHIVE concurrency, true-to-false refresh, folder correction, unchanged authority version, outer rollback, concurrent approval/effect non-fencing, change-during-refresh repetition and zero external mutation. The currently read-only extension remains an external blocker until its future v2/detail contract passes this probe; Task 11 does not modify that repository.

Before any live activation gate can pass, Task 11 runs high-cardinality load tests against the exact single-pass `InboxStats` aggregate used by `/queue`, metrics, and drain reporting, including concurrent claim/reaper traffic and the configured statement-timeout budget. If exact aggregation does not meet the documented latency and database-load SLO at the approved retention cardinality, activation remains blocked until a bounded-staleness cache or transactionally maintained incremental summary is implemented with restart/reconciliation, drift detection, and exact fallback tests. No unmeasured full-table stats scan is accepted merely because it is correct on an empty development database.

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
TEST_POSTGRES_ADMIN_URL=postgresql://user:password@localhost:5432/postgres TEST_POSTGRES_ROLE_DDL=1 .venv/bin/python -m pytest tests/integration/ingestion -q
.venv/bin/python -m pytest --cov=src.ingestion --cov-report=term-missing --cov-fail-under=90
.venv/bin/ruff check src/ tests/
.venv/bin/python -m pytest -q
git diff --check
git add src/ingestion src/scheduler src/config.py src/init_app.py src/main.py src/commands/handlers.py src/observability/metrics.py .env.example scripts/backfill_durable_ingestion.py scripts/manage_pipeline.py tests/unit/ingestion tests/integration/ingestion tests/contracts/test_exchange_sync_contract.py tests/contracts/test_shadow_decision_contract.py tests/architecture/test_phase2_activation_boundary.py tests/architecture/test_phase2_runtime_stays_standby.py tests/unit/test_polling_scheduler.py tests/unit/test_metrics.py tests/unit/test_command_router.py
git commit -m "feat: prepare fenced durable ingestion readiness"
```
