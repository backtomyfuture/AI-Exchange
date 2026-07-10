# AI-Exchange Phase 3 Approval and Send Reliability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让飞书审批、通知、标记已读和 Exchange 发送全部通过不可变快照、事务 CAS 与 Outbox 执行，确保重复卡片不会重复发送，结果不确定时不会自动重试。

**Architecture:** Graph 只生成不可变 `draft_versions` 和审批通知。审批服务在单一事务校验操作者、卡片/草稿版本和邮件 CAS，冻结新的 `send_intents` 并创建唯一 Send Outbox。Send Worker 在网络请求前写 `request_started_at`；任何开始后的崩溃或结果缺失都进入 `send_unknown`。

**Tech Stack:** Python 3.12、psycopg 3、Alembic、PostgreSQL Outbox/CAS、FastAPI、lark-oapi、httpx、pytest。

## Global Constraints

- Phase 2 Durable Inbox、`emails.version`、generation/fencing 必须已经合入。
- 模型和 Graph 不能创建 Send Outbox；只有审批事务可以创建 `send_intents` 和 `send_outbox`。
- 每个草稿版本、发送意图和发送尝试都不可变。
- 卡片远端投递是 at-least-once；所有重复卡片共享稳定 `card_key`、审批版本和动作去重键。
- Send Worker 不使用通用“租约过期自动重试”规则。
- `request_started_at` 一旦存在，崩溃恢复只能进入 `send_unknown`。
- 普通 send 2xx 进入 `accepted`；reply/forward 明确 2xx 才进入 `sent`。
- `send_unknown` 重新发送必须创建新的 approval action、send intent 和 outbox，并保留 supersedes 因果链。
- Notification、Mailbox、Send 和后续 Projection Outbox 都必须固化 `generation` 与 `fencing_token`；claim 和 complete 均验证，`draining` 只能排空原代次任务。

---

## File Map

| Area | Files | Responsibility |
|---|---|---|
| Immutable facts | `src/approval/models.py`, `src/approval/drafts.py`, `src/approval/repository.py` | Draft versions, card resources, approval actions and frozen send intents |
| Approval | `src/approval/service.py`, `src/approval/send_resolution.py` | Atomic approve/reject/edit and audited manual resolution/reauthorization |
| Lark adapter | `src/integrations/lark/approval_handler.py`, `src/outbox/notification.py` | Parse signed card events and deliver stable card identities at least once |
| Mailbox adapter | `src/outbox/mailbox.py` | Idempotent mark-read after durable business/notification evidence |
| Send adapter | `src/integrations/exchange_send.py`, `src/outbox/send.py` | Typed remote results, write-ahead attempt and send-unknown recovery |
| Runtime | `src/outbox/repository.py`, `src/outbox/runtime.py` | Shared fenced claim/complete and owned Worker lifecycle |
| Test harness | `tests/unit/approval/conftest.py`, `tests/unit/outbox/conftest.py`, `tests/integration/approval/conftest.py` | Explicit fakes, fault injector and migrated PostgreSQL transaction fixtures used below |

### Task 1: Add Approval, Draft, and Outbox Schema

**Files:**
- Create: `alembic/versions/20260710_0004_approval_send_outboxes.py`
- Create: `src/approval/__init__.py`
- Create: `src/approval/models.py`
- Create: `src/outbox/__init__.py`
- Create: `tests/integration/approval/test_schema.py`
- Create: `tests/unit/approval/conftest.py`
- Create: `tests/unit/outbox/conftest.py`
- Create: `tests/integration/approval/conftest.py`

**Interfaces:**
- Produces: `DraftVersion`, `PersistedApprovalResource`, `ApprovalCommand`, `ApprovalResult`, `SendIntent`, `SendAttempt`, `OutboxStatus`; named unit/integration fixtures used in Tasks 1-9

- [ ] **Step 1: Write uniqueness and immutability tests**

```python
@pytest.mark.integration
async def test_one_send_intent_per_approval_action(db, approval_action):
    await insert_send_intent(db, approval_action.id)
    with pytest.raises(psycopg.errors.UniqueViolation):
        await insert_send_intent(db, approval_action.id)


@pytest.mark.integration
async def test_one_send_outbox_per_intent(db, send_intent):
    await insert_send_outbox(db, send_intent.id)
    with pytest.raises(psycopg.errors.UniqueViolation):
        await insert_send_outbox(db, send_intent.id)
```

- [ ] **Step 2: Add the migration**

Create `draft_versions`, `approval_actions`, `send_intents`, `notification_outbox`, `send_outbox`, and `mailbox_action_outbox`. Include unique constraints for `(email_id, version)`, `action_key`, `approval_action_id`, `send_intent_id`, notification `business_key`, and mailbox `(email_id, action_type, target_hash)`.

Send Outbox must contain:

```text
id, email_id, send_intent_id, generation, fencing_token, status,
attempt_id, request_started_at, lease_owner, lease_until,
remote_operation_id, http_status, response_fingerprint,
last_error_code, created_at, updated_at, resolved_at
```

`notification_outbox` and `mailbox_action_outbox` also contain non-null `generation` and `fencing_token`, plus lease owner/until, attempts, available time and safe error code. Every Outbox has a unique business key and a foreign key to the owning email/generation. `draft_versions` and `send_intents` use a trigger that raises on UPDATE/DELETE outside the dedicated retention role.

- [ ] **Step 3: Define immutable models**

```python
@dataclass(frozen=True)
class DraftSnapshotInput:
    account_id: int
    email_id: str
    operation: str
    reference_email_id: str | None
    to: Sequence[str]
    cc: Sequence[str]
    subject: str
    body_ref: ContentRef
    attachment_refs: Sequence[ContentRef]


@dataclass(frozen=True)
class ApprovalCommand:
    action_key: str
    card_key: str
    email_id: str
    draft_version_id: str
    content_hash: str
    expected_email_version: int
    approval_version: int
    actor_open_id: str
    decision: Literal["approve", "reject", "approve_edited"]


@dataclass(frozen=True)
class PersistedApprovalResource:
    card_key: str
    email_id: str
    draft_version_id: str
    approval_version: int
    allowed_approver_ids: frozenset[str]
    expires_at: datetime
    generation: int
    fencing_token: int


@dataclass(frozen=True)
class ApprovalResult:
    code: Literal["accepted", "rejected", "already_processed", "stale_card", "expired", "unauthorized"]
    approval_action_id: str | None
    send_intent_id: str | None


@dataclass(frozen=True)
class DraftVersion:
    id: str
    email_id: str
    version: int
    snapshot: DraftSnapshotInput
    content_hash: str


@dataclass(frozen=True)
class SendIntent:
    id: str
    approval_action_id: str
    draft_version_id: str
    snapshot: DraftSnapshotInput
    content_hash: str
    generation: int
    fencing_token: int
    supersedes_send_intent_id: str | None


@dataclass(frozen=True)
class SendAttempt:
    outbox_id: str
    attempt_id: str
    request_started_at: datetime
    send_intent_id: str


class OutboxStatus(StrEnum):
    PENDING = "pending"
    LEASED = "leased"
    SENDING = "sending"
    ACCEPTED = "accepted"
    SENT = "sent"
    FAILED = "failed"
    SEND_UNKNOWN = "send_unknown"
```

In the three `conftest.py` files, define the referenced `db`, `repo`, `service`, `worker`, `lark`, `exchange`, `adapter`, `command`, `snapshot`, `draft`, `approval_action`, `send_intent`, `unknown_job`, `actor`, `fault`, `compiled_graph` and event fixtures, plus `make_intent(operation)` and `SimulatedCrashAfterRemoteSend`. The integration `db` fixture reads `TEST_DATABASE_URL`, creates a per-test schema, runs Alembic to head, yields a psycopg pool and drops that schema in `finally`; `InjectedFailure(RuntimeError)` is raised by `FaultInjector` at named transaction boundaries.

- [ ] **Step 4: Verify and commit**

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/integration/approval/test_schema.py -q
git add alembic/versions/20260710_0004_approval_send_outboxes.py src/approval src/outbox tests/unit/approval/conftest.py tests/unit/outbox/conftest.py tests/integration/approval/conftest.py tests/integration/approval/test_schema.py
git commit -m "feat: add immutable approval and outbox schema"
```

---

### Task 2: Persist Immutable Draft Versions and Canonical Hashes

**Files:**
- Create: `src/approval/drafts.py`
- Create: `tests/unit/approval/test_drafts.py`
- Create: `tests/integration/approval/test_draft_repository.py`

**Interfaces:**
- Produces: `canonical_draft_hash()`, `DraftRepository.create()` and `get()`

- [ ] **Step 1: Write deterministic snapshot tests**

```python
def test_hash_is_order_stable(snapshot):
    reordered = replace(snapshot, to=tuple(reversed(snapshot.to)), cc=tuple(reversed(snapshot.cc)))
    assert canonical_draft_hash(snapshot) == canonical_draft_hash(reordered)


@pytest.mark.integration
async def test_existing_version_cannot_be_updated(repo, draft):
    stored = await repo.create(draft)
    assert not hasattr(repo, "replace_body")
    assert not hasattr(repo, "update")
    with pytest.raises(psycopg.errors.RaiseException, match="immutable draft version"):
        await repo.execute_unsafe_for_test(
            "UPDATE draft_versions SET subject=%s WHERE id=%s",
            ("different", stored.id),
        )
```

- [ ] **Step 2: Implement canonicalization**

Normalize email addresses to lower-case, sort/deduplicate To/Cc, preserve subject/body/attachment order, serialize compact UTF-8 JSON with sorted keys, and return SHA-256. Repository insert assigns the next version while locking the email row; no update method is exposed.

- [ ] **Step 3: Verify and commit**

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/unit/approval/test_drafts.py tests/integration/approval/test_draft_repository.py -q
git add src/approval/drafts.py tests/unit/approval/test_drafts.py tests/integration/approval/test_draft_repository.py
git commit -m "feat: persist immutable draft versions"
```

---

### Task 3: Implement Atomic Approval Service with Actor Authorization

**Files:**
- Create: `src/approval/repository.py`
- Create: `src/approval/service.py`
- Create: `tests/unit/approval/test_service.py`
- Create: `tests/integration/approval/test_approval_atomicity.py`

**Interfaces:**
- Consumes: `ApprovalCommand`; Phase 1 `is_lark_operator_allowed(open_id, settings) -> bool`; `NotificationRepository.get_approval_resource(card_key) -> PersistedApprovalResource`; `PipelineOwnershipRepository.assert_fence(account_id, generation, fencing_token) -> None`
- Produces: `ApprovalService.handle(command) -> ApprovalResult`

- [ ] **Step 1: Write duplicate, stale, expired, and unauthorized tests**

```python
@pytest.mark.asyncio
async def test_duplicate_action_creates_one_send_job(service, command, repo):
    first, second = await asyncio.gather(service.handle(command), service.handle(command))
    assert {first.code, second.code} == {"accepted", "already_processed"}
    assert await repo.count_send_outbox(command.email_id) == 1


@pytest.mark.asyncio
async def test_stale_draft_hash_is_rejected(service, command):
    stale = replace(command, content_hash="0" * 64)
    result = await service.handle(stale)
    assert result.code == "stale_card"


@pytest.mark.asyncio
async def test_reject_never_creates_send_records(service, command, repo):
    result = await service.handle(replace(command, decision="reject"))
    assert result.code == "rejected"
    assert await repo.email_status(command.email_id) == "rejected"
    assert await repo.count_send_intents(command.email_id) == 0
    assert await repo.count_send_outbox(command.email_id) == 0


@pytest.mark.asyncio
async def test_edited_approval_creates_new_version_before_intent(service, command, repo):
    result = await service.handle(
        replace(command, decision="approve_edited"),
        edited_draft=repo.edited_input(),
    )
    intent = await repo.get_send_intent(result.send_intent_id)
    assert intent.draft_version == 2
    assert intent.content_hash == await repo.draft_hash(version=2)
```

- [ ] **Step 2: Implement one approval transaction**

In one transaction: insert `approval_actions` by `action_key`; load the trusted `PersistedApprovalResource`; verify actor, expiry, card version, email state and draft hash; assert generation/fence; then branch. `reject` CASes to `rejected`, writes audit and notification invalidations, and creates no send row. `approve_edited` first creates a new immutable draft version and revalidates its hash. Only `approve`/`approve_edited` CAS from `waiting_approval`, freeze a new `send_intents`, insert one fenced `send_outbox` and append audit. Expiry racing approval uses the same row lock, so exactly one terminal transition commits. Any failure rolls back all rows.

- [ ] **Step 3: Verify rollback after injected faults**

```python
@pytest.mark.integration
async def test_fault_after_email_cas_rolls_back_everything(service, command, fault, repo):
    fault.raise_after_email_cas = True
    with pytest.raises(InjectedFailure):
        await service.handle(command)
    assert await repo.email_status(command.email_id) == "waiting_approval"
    assert await repo.count_send_outbox(command.email_id) == 0
```

- [ ] **Step 4: Run and commit**

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/unit/approval/test_service.py tests/integration/approval/test_approval_atomicity.py -q
git add src/approval/repository.py src/approval/service.py tests/unit/approval/test_service.py tests/integration/approval/test_approval_atomicity.py
git commit -m "feat: make email approval atomic and idempotent"
```

---

### Task 4: Move Lark Cards to Notification Outbox

**Files:**
- Create: `src/outbox/repository.py`
- Create: `src/outbox/notification.py`
- Create: `src/integrations/__init__.py`
- Create: `src/integrations/lark/__init__.py`
- Create: `src/integrations/lark/approval_handler.py`
- Create: `tests/unit/outbox/test_notification_worker.py`
- Create: `tests/unit/approval/test_lark_handler.py`
- Modify: `src/utils/card_builder.py`
- Modify: `src/utils/lark_messaging.py`
- Modify: `src/utils/lark_app.py:320-894`
- Modify: `src/exchange_service.py`
- Modify: `tests/unit/test_lark_app.py`

**Interfaces:**
- Produces: stable `card_key`, durable notification delivery records, `ApprovalCommand` from card actions

- [ ] **Step 1: Write duplicate remote-delivery tests**

```python
@pytest.mark.asyncio
async def test_lost_ack_reuses_stable_card_identity(worker, lark, outbox):
    lark.create_card.side_effect = [TimeoutError("lost response"), "msg-2"]
    await worker.run_once()
    await worker.run_once()
    rows = await outbox.deliveries("card-1")
    assert rows.card_key == "card-1"
    assert rows.attempt_count == 2


def test_action_key_does_not_depend_on_message_id(handler, event_a, event_b):
    assert handler.to_command(event_a).action_key == handler.to_command(event_b).action_key


@pytest.mark.asyncio
async def test_notification_completion_rejects_stale_fence(worker, outbox, lark):
    job = await outbox.seed_notification(generation=4, fencing_token=40)
    await outbox.rotate_fence(job.account_id, generation=4, fencing_token=41)
    with pytest.raises(StaleFence):
        await worker.deliver(job)
    lark.create_card.assert_not_called()
```

- [ ] **Step 2: Add card-bound approval fields**

Every approval action value contains only `card_key`, action type and an opaque action nonce. Trusted `email_id`, draft ID/hash, `approval_version`, expected email version, `expires_at`, owner/audience, `generation` and `fencing_token` are loaded from the persisted notification/approval resource. Message ID is delivery metadata and is excluded from `action_key`.

- [ ] **Step 3: Implement at-least-once Worker**

Claim notification rows with generic lease rules and matching generation/fencing token. Call synchronous Lark SDK via `asyncio.to_thread`. Record every known external message ID. Completion verifies lease owner and the same fence. When approval/rejection/expiry/new draft occurs, enqueue updates for all known cards. Duplicate cards remain business-safe because actions share the same command key and CAS.

- [ ] **Step 4: Cut direct card sends from the processing path**

`_dispatch_notification()` creates fenced Outbox rows transactionally with the email state. It no longer calls `send_approval_card()` or `send_read_only_card()` directly. Keep wrappers only for tests/compatibility until Phase 6 removal.

- [ ] **Step 5: Verify and commit**

```bash
.venv/bin/python -m pytest tests/unit/outbox/test_notification_worker.py tests/unit/approval/test_lark_handler.py tests/unit/test_lark_app.py -q
git add src/outbox src/integrations src/utils/card_builder.py src/utils/lark_messaging.py src/utils/lark_app.py src/exchange_service.py tests/unit/outbox/test_notification_worker.py tests/unit/approval/test_lark_handler.py tests/unit/test_lark_app.py
git commit -m "feat: deliver stable approval cards through outbox"
```

---

### Task 5: Move mark_read to Mailbox Action Outbox

**Files:**
- Create: `src/outbox/mailbox.py`
- Create: `tests/unit/outbox/test_mailbox_worker.py`
- Create: `tests/integration/approval/test_mark_read_ordering.py`
- Modify: `src/exchange_service.py`
- Modify: `tests/unit/test_two_phase_mark_read.py`

**Interfaces:**
- Produces: `MailboxActionWorker`; notification success creates `mark_read` transactionally

- [ ] **Step 1: Write ordering tests**

```python
@pytest.mark.integration
async def test_mark_read_created_only_after_notification_delivery(repo):
    email = await repo.seed_waiting_notification()
    assert await repo.mailbox_action_count(email.id) == 0
    await repo.record_notification_delivered(email.notification_id, "msg-1")
    assert await repo.mailbox_action_count(email.id) == 1


@pytest.mark.asyncio
async def test_mark_read_retry_is_idempotent(worker, exchange):
    exchange.mark_as_read.side_effect = [TimeoutError("temporary"), True]
    await worker.run_once()
    await worker.run_once()
    assert exchange.mark_as_read.await_count == 2


@pytest.mark.asyncio
async def test_mailbox_worker_rejects_stale_fence_before_remote_call(worker, repo, exchange):
    job = await repo.seed_mailbox_action(generation=4, fencing_token=40)
    await repo.rotate_fence(job.account_id, generation=4, fencing_token=41)
    with pytest.raises(StaleFence):
        await worker.deliver(job)
    exchange.mark_as_read.assert_not_awaited()
```

- [ ] **Step 2: Implement mailbox Worker**

Notification delivered creates `mark_read`; `no_action` creates it after policy state commit. Use generic retry because `mark_read` is idempotent. Claim and complete require the row generation/fencing token and lease owner; a stale Worker cannot commit even if its remote call returns. Match incoming read projection against `email_id + expected_read_state` to complete pending action without reprocessing the email. Reject unsupported `move` with a permanent capability error.

- [ ] **Step 3: Verify and commit**

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/unit/outbox/test_mailbox_worker.py tests/integration/approval/test_mark_read_ordering.py tests/unit/test_two_phase_mark_read.py -q
git add src/outbox/mailbox.py src/exchange_service.py tests/unit/outbox/test_mailbox_worker.py tests/integration/approval/test_mark_read_ordering.py tests/unit/test_two_phase_mark_read.py
git commit -m "feat: make mailbox state changes durable and ordered"
```

---

### Task 6: Return Typed Exchange Send Results

**Files:**
- Create: `src/integrations/exchange_send.py`
- Create: `tests/unit/outbox/test_exchange_send_adapter.py`
- Modify: `src/utils/exchange_api.py:322-591`
- Modify: `tests/unit/test_exchange_api.py`

**Interfaces:**
- Produces: `ExchangeSendAdapter.send(intent) -> ExchangeResult`

- [ ] **Step 1: Write outcome-classification tests**

```python
@pytest.mark.parametrize(
    ("operation", "status", "expected"),
    [
        ("reply", 200, "sent"),
        ("forward", 200, "sent"),
        ("send", 200, "accepted"),
        ("reply", 400, "proven_not_sent"),
        ("reply", 500, "unknown"),
    ],
)
async def test_http_result_mapping(adapter, operation, status, expected):
    adapter.transport.respond(status, {"data": {"log_id": "log-1"}})
    result = await adapter.send(make_intent(operation))
    assert result.outcome == expected
```

- [ ] **Step 2: Implement conservative transport mapping**

Only a transport result that proves zero request bytes were written may be `proven_not_sent`. DNS/connect setup failure qualifies only when the transport exposes that proof; every ambiguous connect failure is conservative `unknown`. Timeout, disconnect after dispatch, any 5xx, malformed success, and cancellation after `request_started_at` are `unknown`. Persist only a SHA-256 response fingerprint and safe error code. Save `log_id` for normal send.

- [ ] **Step 3: Verify and commit**

```bash
.venv/bin/python -m pytest tests/unit/outbox/test_exchange_send_adapter.py tests/unit/test_exchange_api.py -q
git add src/integrations/exchange_send.py src/utils/exchange_api.py tests/unit/outbox/test_exchange_send_adapter.py tests/unit/test_exchange_api.py
git commit -m "feat: classify Exchange send outcomes explicitly"
```

---

### Task 7: Implement Write-ahead Send Worker and send_unknown Recovery

**Files:**
- Create: `src/outbox/send.py`
- Create: `tests/unit/outbox/test_send_worker.py`
- Create: `tests/integration/approval/test_send_recovery.py`
- Modify: `src/outbox/repository.py`

**Interfaces:**
- Consumes: immutable Send Intent and `ExchangeSendAdapter`
- Produces: `SendOutboxRepository.claim_unstarted(worker_id: str, generation: int, fencing_token: int, limit: int) -> list[SendJob]`; `begin_attempt(outbox_id, worker_id, now) -> SendAttempt`; `record_result(attempt, result) -> None`; `recover_started_as_unknown(now) -> int`

- [ ] **Step 1: Write crash-after-remote-send test**

```python
@pytest.mark.integration
async def test_started_job_never_requeues_after_crash(repo, worker, exchange):
    job = await repo.seed_send_job()
    exchange.send.side_effect = SimulatedCrashAfterRemoteSend()
    with pytest.raises(SimulatedCrashAfterRemoteSend):
        await worker.run_once()
    await repo.recover_started_as_unknown()
    assert await repo.status(job.id) == "send_unknown"
    assert await repo.claim_unstarted("worker-b", generation=job.generation, fencing_token=job.fencing_token, limit=10) == []


@pytest.mark.integration
async def test_only_matching_sender_generation_can_claim_and_complete(repo):
    old = await repo.seed_send_job(generation=4, fencing_token=40)
    assert await repo.claim_unstarted("new", generation=5, fencing_token=50, limit=10) == []
    claimed = await repo.claim_unstarted("old", generation=4, fencing_token=40, limit=10)
    assert [job.id for job in claimed] == [old.id]
    await repo.rotate_fence(account_id=old.account_id, generation=4, fencing_token=41)
    with pytest.raises(StaleFence):
        await repo.begin_attempt(old.id, worker_id="old", now=repo.now())
```

- [ ] **Step 2: Implement write-ahead attempt state**

`claim_unstarted()` only returns rows with `request_started_at IS NULL`, matching sticky email generation and current/draining ownership. A new generation cannot claim an old intent. `begin_attempt()` rechecks lease owner/generation/fence and commits UUID `attempt_id`, timestamp and `sending` before the network call. If result is unknown, update email/send row/audit and create a high-priority fenced Notification Outbox in one transaction. A stale Worker cannot commit a remote result; an attempt already started is conservatively recorded `send_unknown` by the active recovery coordinator. Recovery changes expired `sending` to `send_unknown`; it never clears the start marker.

- [ ] **Step 3: Verify accepted, sent, failed, and unknown**

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/unit/outbox/test_send_worker.py tests/integration/approval/test_send_recovery.py -q
```

- [ ] **Step 4: Commit**

```bash
git add src/outbox/send.py src/outbox/repository.py tests/unit/outbox/test_send_worker.py tests/integration/approval/test_send_recovery.py
git commit -m "feat: make uncertain Exchange sends stop for review"
```

---

### Task 8: Add Manual Send Resolution and Reauthorization

**Files:**
- Create: `src/approval/send_resolution.py`
- Create: `tests/unit/approval/test_send_resolution.py`
- Create: `tests/integration/approval/test_send_reauthorization.py`
- Modify: `src/integrations/lark/approval_handler.py`

**Interfaces:**
- Produces: `confirm_sent()`, `cancel()`, `reauthorize()`

- [ ] **Step 1: Write causality tests**

```python
@pytest.mark.integration
async def test_reauthorize_creates_new_chain(service, unknown_job, actor):
    replacement = await service.reauthorize(unknown_job.id, actor, "verified not sent")
    assert replacement.approval_action_id != unknown_job.approval_action_id
    assert replacement.send_intent_id != unknown_job.send_intent_id
    assert replacement.send_outbox_id != unknown_job.send_outbox_id
    assert replacement.supersedes_send_intent_id == unknown_job.send_intent_id
    assert await service.status(unknown_job.id) == "send_unknown"
    assert await service.snapshot(unknown_job.id) == unknown_job
```

- [ ] **Step 2: Implement three explicit actions**

`confirm_sent` resolves aggregate as sent; `cancel` preserves unknown attempt and cancels further work; `reauthorize` verifies an allowed actor and reason, creates a new approval action/send intent/outbox, and links supersedes IDs. No method mutates or resets the old Send Outbox.

- [ ] **Step 3: Verify and commit**

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/unit/approval/test_send_resolution.py tests/integration/approval/test_send_reauthorization.py -q
git add src/approval/send_resolution.py src/integrations/lark/approval_handler.py tests/unit/approval/test_send_resolution.py tests/integration/approval/test_send_reauthorization.py
git commit -m "feat: add audited send uncertainty resolution"
```

---

### Task 9: Wire Outbox Runtime and Remove Graph Direct Sending

**Files:**
- Create: `src/outbox/runtime.py`
- Create: `tests/unit/outbox/test_runtime.py`
- Modify: `src/init_app.py`
- Modify: `src/main.py`
- Modify: `src/graph/builder.py`
- Modify: `src/nodes/sender.py`
- Modify: `src/utils/lark_app.py`

**Interfaces:**
- Produces: one `OutboxRuntime`; Graph contains no direct Exchange node and retains its existing no-side-effect human boundary until Phase 4 adds `await_human`

- [ ] **Step 1: Write wiring tests**

```python
def test_graph_contains_no_direct_sender(compiled_graph):
    assert "sender" not in compiled_graph.get_graph().nodes


@pytest.mark.asyncio
async def test_runtime_starts_all_workers(runtime):
    await runtime.start()
    assert runtime.worker_names == {"notification", "mailbox", "send"}
    await runtime.stop(grace_seconds=1.0)
```

- [ ] **Step 2: Wire runtime and compatibility handlers**

AppContext owns repositories/services/runtime. Lark handlers call `ApprovalService`; they never update Graph approval status or resume a sender node. Keep `src/nodes/sender.py` as a compatibility function that raises a clear retired-path error; remove it in Phase 6.

- [ ] **Step 3: Run the Phase 3 gate**

```bash
.venv/bin/python -m pytest tests/unit/approval tests/unit/outbox -q
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/integration/approval -q
.venv/bin/python -m pytest --cov=src.approval --cov=src.outbox --cov-report=term-missing --cov-fail-under=90
.venv/bin/ruff check src/ tests/
.venv/bin/python -m pytest -q
git diff --check
```

- [ ] **Step 4: Commit**

```bash
git add src/outbox/runtime.py src/init_app.py src/main.py src/graph/builder.py src/nodes/sender.py src/utils/lark_app.py tests/unit/outbox/test_runtime.py
git commit -m "refactor: make outbox workers the only side-effect path"
```
