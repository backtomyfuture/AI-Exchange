# AI-Exchange Phase 3 Approval and Send Reliability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让飞书审批、通知、标记已读和 Exchange 发送全部通过不可变快照、事务 CAS 与 Outbox 执行，并交付旧卡失效后唯一可执行的 Durable activation 能力；当前 extension 的 `exchange_sync_contract_v2` 外部 gate 未解除时实现可验收但生产 switch 必须保持 blocked。

**Architecture:** Durable Graph 只生成不可变 `draft_versions` 和审批通知。审批服务在单一事务校验操作者、当前数据库 authority、卡片/草稿版本和邮件 CAS，冻结新的 `send_intents` 并创建唯一 Send Outbox。Send Worker 在网络请求前写 `request_started_at`；任何开始后的崩溃或结果缺失都进入 `send_unknown`。Phase 2 留在 Shadow/readiness；本阶段实现唯一 authority/effect-serialized switch 与 base successor contract，P4-P6 追加能力 evidence，未来真实 v2 生成完整 `production_ready` 后同一入口才可切换。当前 extension 不兼容时测试使用 synthetic terminal fixture 仅验证原子 mutation，真实命令返回 external-blocked，绝不声称生产已激活。

**Tech Stack:** Python 3.12、psycopg 3、Alembic、PostgreSQL Outbox/CAS、FastAPI、lark-oapi、httpx、pytest。

## Global Constraints

- Phase 2 Durable Inbox、`emails.version`、generation/fencing 必须已经合入。
- 发布方案 B 是硬约束：Phase 3 的 Notification/Mailbox/Send Outbox、Lark action authority、source-delete cancellation race 和全部旧卡失效证明未完成前，数据库 authority 必须保持 legacy/Shadow，Durable 202、Worker claim 和 Sync 写入均不得启动。
- 即使本阶段代码/测试全部通过，当前 extension 的分页与 read-flag contract 仍使生产命令返回 `blocked_external_exchange_sync_contract`；mock/v2 fixture 只能证明 AI 实现能力，不能替代后续 extension 修复后的真实探针证据。
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
| Activation | `src/ingestion/activation.py`, Phase 2 runtime-authority/barrier tables, `pipeline_activation_barrier_successors` | Phase-3-only versioned successor consumer, atomic switch, command receipt and Durable rollback |
| Test harness | `tests/unit/approval/conftest.py`, `tests/unit/outbox/conftest.py`, `tests/integration/approval/conftest.py` | Explicit fakes, fault injector and migrated PostgreSQL transaction fixtures used below |

### Task 1: Add Approval, Draft, and Outbox Schema

**Files:**
- Create: `alembic/versions/20260713_0008_approval_send_outboxes.py`
- Create: `src/approval/__init__.py`
- Create: `src/approval/models.py`
- Create: `src/outbox/__init__.py`
- Create: `tests/integration/approval/test_schema.py`
- Create: `tests/unit/approval/conftest.py`
- Create: `tests/unit/outbox/conftest.py`
- Create: `tests/integration/approval/conftest.py`
- Modify: `src/db/access_contract.py`
- Modify: `src/db/bootstrap.py`
- Modify: `src/db/schema.py`
- Modify: `src/db/schema_contract.py`
- Modify: `src/maintenance/checkpoint_repository.py`
- Modify: `tests/unit/test_database_revision.py`
- Modify: `tests/unit/test_database_schema_contract.py`
- Modify: `tests/unit/test_checkpoint_repository_safety.py`
- Modify: `tests/unit/test_alembic_offline.py`
- Modify: `tests/integration/ingestion/test_access_roles.py`

**Interfaces:**
- Produces: `DraftVersion`, `PersistedApprovalResource`, `ApprovalCommand`, `ApprovalResult`, `SendIntent`, `SendAttempt`, `OutboxStatus`; named unit/integration fixtures used in Tasks 1-10

Migration revision is exactly `20260713_0008` with linear `down_revision = "20260713_0007"`; creating a second Phase 2/3 head is forbidden. Phase 2 owns `0005` Sync control, `0006` Shadow input and `0007` runtime activation.

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


@pytest.mark.integration
async def test_legacy_card_terminal_evidence_is_immutable(db, legacy_card):
    row = await db.insert_legacy_card_invalidation(legacy_card)
    await db.complete_legacy_card_invalidation(row.id, status="invalidated")
    with pytest.raises(psycopg.errors.RaiseException):
        await db.reopen_legacy_card_for_test(row.id)
```

- [ ] **Step 2: Add the migration**

Create `draft_versions`, `approval_actions`, `send_intents`, `notification_outbox`, `send_outbox`, `mailbox_action_outbox`, immutable `send_resolution_actions`, and `legacy_lark_card_invalidations`. Include unique constraints for `(email_id, version)`, `action_key`, `approval_action_id`, `send_intent_id`, a partial unique non-null `send_intents.supersedes_send_intent_id`, one resolution action per unknown Send Outbox, notification `business_key`, mailbox `(email_id, action_type, target_hash)`, and one invalidation identity per known legacy `card_key`.

Send Outbox must contain:

```text
id, email_id, send_intent_id, generation, fencing_token, status,
attempt_id, request_started_at, lease_owner, lease_until,
remote_operation_id, http_status, response_fingerprint,
last_error_code, created_at, updated_at, resolved_at
```

`notification_outbox` and `mailbox_action_outbox` also contain non-null `generation` and `fencing_token`, plus lease owner/until, attempts, available time and safe error code. Every Outbox has a unique business key and a foreign key to the owning email/generation. `draft_versions` and `send_intents` use a trigger that raises on UPDATE/DELETE outside the dedicated retention role.

`legacy_lark_card_invalidations` contains account/card resource identity, legacy-card/audit source fingerprint, generation/fence/authority epoch, status (`pending/leased/invalidated/not_found/failed`), lease/attempt fields, remote evidence hash, safe error code and timestamps. Identity and terminal evidence are immutable; failed/pending cards block activation. The migration extends all three Outbox status checks with `cancelled`, and creates no activation row automatically.

`send_resolution_actions` stores the unknown Outbox/attempt/intent identity, expected version, decision (`confirm_sent/cancel/reauthorize`), actor/reason fingerprints, generation/fence, optional superseding intent, command-receipt reference and timestamp. Its identity and decision are append-only; one unknown Outbox can have exactly one row, and a non-null superseding intent is unique.

The same migration creates `pipeline_activation_barrier_successors` with immutable fields: `id`, `chain_id`, `account_id`, nullable `phase2_cutover_barrier_id` FK to the Phase-2 barrier table, nullable `predecessor_successor_id` self-FK, nullable `target_generation`/`target_fencing_token`, `capability_stage`, `app_schema_head`, `schema_digest`, `runtime_build`, `config_hash`, `evidence_manifest_hash`, `external_contract_status`, `superseded_by`, `created_at`. The two predecessor columns are never interchangeable: implementation capability stages self-chain through `predecessor_successor_id` and leave target fields null; only the activation-time `production_ready` row must bind both a distinct Phase-2 **live ready cutover barrier** and the reserved target generation/fence. A partial unique constraint permits one latest non-superseded leaf per `(account_id,chain_id)`; triggers forbid identity/evidence UPDATE, DELETE and TRUNCATE except the single atomic predecessor `superseded_by IS NULL -> new_id` link committed with the new successor. Stage order is `phase3_base -> phase4_graph_projection -> phase5_security_governance -> phase6_implementation_complete_external_blocked -> production_ready`. Only a full `production_ready` snapshot may be consumed; it includes the real v2 proof rather than pointing to mutable evidence elsewhere.

Migration `0008` also creates append-only `pipeline_activation_consumptions(id, account_id, successor_id, live_cutover_barrier_id, switch_receipt_id, target_generation, target_fencing_token, authority_epoch, transaction_id, created_at)`. FKs bind the exact latest `production_ready` successor, its same `phase2_cutover_barrier_id`, the committed `pipeline.switch` receipt and target ownership; unique constraints permit one consumption per successor/live barrier/receipt/target epoch. INSERT occurs in the same transaction as authority/ownership promotion, live-barrier consume, receipt and audit; UPDATE/DELETE/TRUNCATE are forbidden. Commit-unknown recovery reads this row plus the receipt. Real-PostgreSQL tests cover stage order, current-leaf uniqueness, both FK domains, immutability, exact transaction relation and lost-ack replay. Roles receive only their stage-specific append/read namespace; runtime can consume through `ActivationService` but cannot fabricate later-stage evidence or standalone consumption facts.

Advancing to `0008` is a complete code-first revision-contract change. Update the exact schema digest, application-head constant, all four role manifests, bootstrap pre/post checks, checkpoint allowlists and offline SQL tests in the same task. Runtime receives only the INSERT/SELECT and lease/CAS columns required by approval and Notification/Mailbox/Send execution; it cannot rewrite immutable snapshots or terminal evidence. Maintenance receives the bounded INSERT/SELECT/lease/terminal-update privileges needed only for `legacy_lark_card_invalidations` and cannot create approval/send work. Auditor is SELECT-only and migration owns DDL. Real-role tests prove all permitted paths plus cross-role UPDATE/DELETE/TRUNCATE and authority-mutation denials. With every Phase-3 execution profile off, exact `0007` code remains usable until bootstrap advances to `0008`; an old `0007` process never accepts a database-first `0008` head.

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
    CANCELLED = "cancelled"
```

In the three `conftest.py` files, define the referenced `db`, `repo`, `service`, `worker`, `lark`, `exchange`, `adapter`, `command`, `snapshot`, `draft`, `approval_action`, `send_intent`, `unknown_job`, `actor`, `fault`, `compiled_graph` and event fixtures, plus `make_intent(operation)` and `SimulatedCrashAfterRemoteSend`. The integration `db` fixture reads `TEST_DATABASE_URL`, creates a per-test schema, runs Alembic to head, yields a psycopg pool and drops that schema in `finally`; `InjectedFailure(RuntimeError)` is raised by `FaultInjector` at named transaction boundaries.

- [ ] **Step 4: Verify and commit**

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/integration/approval/test_schema.py tests/integration/ingestion/test_access_roles.py tests/unit/test_database_revision.py tests/unit/test_database_schema_contract.py tests/unit/test_checkpoint_repository_safety.py tests/unit/test_alembic_offline.py -q
git add alembic/versions/20260713_0008_approval_send_outboxes.py src/approval src/outbox src/db/access_contract.py src/db/bootstrap.py src/db/schema.py src/db/schema_contract.py src/maintenance/checkpoint_repository.py tests/unit/approval/conftest.py tests/unit/outbox/conftest.py tests/integration/approval/conftest.py tests/integration/approval/test_schema.py tests/integration/ingestion/test_access_roles.py tests/unit/test_database_revision.py tests/unit/test_database_schema_contract.py tests/unit/test_checkpoint_repository_safety.py tests/unit/test_alembic_offline.py
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
- Consumes: `ApprovalCommand`; Phase 1 `is_lark_operator_allowed(open_id, settings) -> bool`; `NotificationRepository.get_approval_resource(card_key) -> PersistedApprovalResource`; `RuntimeAuthorityRepository.assert_lark_action_authority(account_id, authority_epoch, generation, fencing_token) -> None`
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


@pytest.mark.asyncio
async def test_old_or_non_authoritative_card_action_is_stale_without_send(
    service, command, repo, authority
):
    authority.rotate_epoch_for_test()
    result = await service.handle(command)
    assert result.code == "stale_card"
    assert await repo.count_send_outbox(command.email_id) == 0
    assert await repo.audit_count("lark.action_stale_authority") == 1


@pytest.mark.asyncio
async def test_unauthorized_first_does_not_consume_action_key(
    service, command, repo
):
    denied = replace(command, actor_open_id="unauthorized")
    assert (await service.handle(denied)).code == "unauthorized"
    assert await repo.count_approval_actions(action_key=command.action_key) == 0
    accepted = await service.handle(command)
    assert accepted.code == "accepted"
    assert await repo.count_approval_actions(action_key=command.action_key) == 1


@pytest.mark.asyncio
async def test_concurrent_unauthorized_cannot_race_authorized_nonce(
    service, command, repo
):
    denied, allowed = await asyncio.gather(
        service.handle(replace(command, actor_open_id="unauthorized")),
        service.handle(command),
    )
    assert denied.code == "unauthorized"
    assert allowed.code == "accepted"
    assert await repo.count_send_outbox(command.email_id) == 1
```

- [ ] **Step 2: Implement one approval transaction**

In one transaction, use untrusted `card_key` only to locate and lock the trusted persisted card resource, then lock its email and current database authority in the fixed order. First verify the trusted audience/actor and exact signed command/resource binding. Only an authorized exact-payload retry may then SELECT an existing `approval_actions` row and replay its stored result, even if the resource expired after that action committed; changed payload conflicts. When no action exists, verify expiry, card/approval version, persisted email/draft identity and hash, email CAS version, and require `durable_active` plus the card's exact authority epoch, generation and fence. Only after all validation succeeds may the transaction insert `approval_actions` by `action_key` and proceed to the business CAS. Environment flags cannot authorize an action.

Unauthorized, stale-authority/card/hash/version and expired attempts never insert `approval_actions`, never consume the unique `action_key`/nonce and never create draft/send/outbox rows. They append only an independently deduplicated, bounded security attempt audit keyed by a privacy-safe attempt fingerprint; retrying an invalid event cannot grow it without bound. Thus an unauthorized-first or concurrent unauthorized request cannot reserve the nonce and deny a later legitimate actor. A legacy, pre-activation, quiescing, draining-other-generation or otherwise stale card returns `stale_card`; expired returns `expired`. Neither can later become a business action under the same stale resource, while a fresh authoritative card receives a new resource/version/nonce. Concurrent valid attempts serialize on the trusted rows: one inserts the action and the other returns `already_processed` from that committed fact.

After authority validation, `reject` CASes to `rejected`, writes audit and notification invalidations, and creates no send row. `approve_edited` first creates a new immutable draft version and revalidates its hash. Only `approve`/`approve_edited` CAS from `waiting_approval`, freeze a new `send_intents`, insert one fenced `send_outbox` and append audit. Expiry racing approval uses the same row lock, so exactly one terminal transition commits. Any failure rolls back all rows.

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
- Create: `tests/integration/approval/test_source_delete_cancellation.py`
- Create: `tests/integration/approval/test_legacy_card_invalidation.py`
- Modify: `src/utils/card_builder.py`
- Modify: `src/utils/lark_messaging.py`
- Modify: `src/utils/lark_app.py:320-894`
- Modify: `src/exchange_service.py`
- Modify: `src/ingestion/repository.py`
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


@pytest.mark.integration
async def test_source_delete_and_send_start_have_one_serialized_winner(
    repo, send_worker
):
    email = await repo.seed_waiting_approval_with_all_unstarted_outboxes()
    deleted, started = await asyncio.gather(
        repo.apply_source_delete(email.id, expected_version=email.version),
        send_worker.try_begin_attempt(email.send_outbox_id),
    )
    assert (deleted.cancelled_side_effects, started is not None) in {
        (True, False),
        (False, True),
    }
    current = await repo.get_email(email.id)
    assert current.source_deleted_at is not None
    if deleted.cancelled_side_effects:
        assert await repo.unstarted_outbox_statuses(email.id) == {"cancelled"}
        assert await repo.card_invalidation_count(email.id) >= 1
    else:
        assert await repo.send_request_started_at(email.id) is not None


@pytest.mark.integration
async def test_every_known_legacy_card_has_terminal_invalidation_evidence(
    legacy_card_worker, repo, lark
):
    lark.invalidate.side_effect = ["invalidated", "not_found"]
    await repo.seed_legacy_cards_from_backfill_audit(count=2)
    await legacy_card_worker.drain()
    evidence = await repo.legacy_card_invalidation_evidence()
    assert evidence.pending == evidence.failed == 0
    assert evidence.invalidated + evidence.not_found == 2
```

- [ ] **Step 2: Add card-bound approval fields**

Every approval action value contains only `card_key`, action type and an opaque action nonce. Trusted `email_id`, draft ID/hash, `approval_version`, expected email version, `expires_at`, owner/audience, `generation` and `fencing_token` are loaded from the persisted notification/approval resource. Message ID is delivery metadata and is excluded from `action_key`.

- [ ] **Step 3: Implement at-least-once Worker**

Claim notification rows with generic lease rules and matching generation/fencing token. Call synchronous Lark SDK via `asyncio.to_thread`. Record every known external message ID. Completion verifies lease owner and the same fence. When approval/rejection/expiry/new draft occurs, enqueue updates for all known cards. Duplicate cards remain business-safe because actions share the same command key, DB action authority and CAS.

`LegacyCardInvalidationWorker` materializes the Phase-2 `legacy_card_invalidation_required` audit manifest into unique invalidation rows, locally revokes every card action before the remote call, and then records remote `invalidated` or `not_found` evidence. A timeout/failure remains `failed` with a safe code; it cannot be treated as invalidated and blocks final activation. No raw card payload is copied. A stable evidence hash covers the exact source manifest, terminal identities and result counts.

This invalidation worker is a bounded maintenance operation explicitly authorized by the Phase-3 cutover plan while production authority remains quiescing/Shadow; it is not one of the dormant Notification/Mailbox/Send execution workers. Its lease scope is restricted to the frozen Phase-2 card manifest, and it cannot create approvals, send mail, claim Inbox rows or process newly created cards.

- [ ] **Step 4: Route card delivery by stamped pipeline authority**

On the new Durable path, `_dispatch_notification()` creates fenced Notification Outbox rows transactionally with the email state and never calls `send_approval_card()` or `send_read_only_card()` directly. That rule is scoped to the Durable candidate/current generation; it must not silently retire the production legacy path before activation. While authority is `legacy_authoritative` or `shadow`, an exactly legacy-stamped item continues the existing direct card behavior only through `LegacyProcessingAdapter.before_external_effect("lark_notification", ordinal, target_hash)` and `pipeline_legacy_effects`; the dormant Durable candidate may prepare code/schema but creates and claims no business Outbox. During quiescing/draining, only work stamped before the boundary may finish through the same guard. After the atomic switch, the current target generation uses Notification Outbox exclusively, while the old generation may only drain its already-stamped guarded effects. Keep the legacy wrappers until the Phase-6 post-activation stability/contraction gate, not merely until this task lands.

Phase 2's delete decision only carries `cancel_pending_side_effects` and sets `source_deleted_at`. Phase 3 extends the repository transaction: lock the email first, then Notification, Mailbox and Send Outboxes in fixed order. If every remote request is unstarted, CAS the email, cancel those rows and enqueue all card invalidations atomically. `SendOutboxRepository.begin_attempt()` takes the same email-first lock order. If begin-attempt wins, delete sets only `source_deleted_at` and cannot cancel or rewrite that attempt; if delete wins, begin-attempt sees `cancelled` and makes no Exchange call. The concurrent test permits exactly those two outcomes.

- [ ] **Step 5: Verify and commit**

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/unit/outbox/test_notification_worker.py tests/unit/approval/test_lark_handler.py tests/unit/test_lark_app.py tests/integration/approval/test_source_delete_cancellation.py tests/integration/approval/test_legacy_card_invalidation.py -q
git add src/outbox src/integrations src/utils/card_builder.py src/utils/lark_messaging.py src/utils/lark_app.py src/exchange_service.py src/ingestion/repository.py tests/unit/outbox/test_notification_worker.py tests/unit/approval/test_lark_handler.py tests/unit/test_lark_app.py tests/integration/approval/test_source_delete_cancellation.py tests/integration/approval/test_legacy_card_invalidation.py
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
        ("reply", 400, "unknown"),
        ("reply", 500, "unknown"),
    ],
)
async def test_http_result_mapping(adapter, operation, status, expected):
    adapter.transport.respond(status, {"data": {"log_id": "log-1"}})
    result = await adapter.send(make_intent(operation))
    assert result.outcome == expected


async def test_zero_request_bytes_transport_proof_is_proven_not_sent(adapter):
    adapter.transport.fail_before_write_with_proof()
    result = await adapter.send(make_intent("reply"))
    assert result.outcome == "proven_not_sent"


@pytest.mark.parametrize(
    "body",
    [
        {"data": {"log_id": "success-looking"}},
        {"code": "PRE_EXECUTION", "message": "proxy supplied"},
        b"malformed",
    ],
)
async def test_generic_or_proxy_400_after_write_is_unknown(adapter, body):
    adapter.transport.respond(400, body, request_bytes_written=True)
    result = await adapter.send(make_intent("reply"))
    assert result.outcome == "unknown"
    assert result.remote_operation_id is None
```

- [ ] **Step 2: Implement conservative transport mapping**

Only a transport result that proves zero request bytes were written may currently be `proven_not_sent`. DNS/connect setup failure qualifies only when the transport exposes that proof; every ambiguous connect failure is conservative `unknown`. Once any request byte may have been written, generic/malformed HTTP 400 or other 4xx—including a proxy-injected success-looking `log_id` or unauthenticated “pre-execution” code—is `unknown`, as are timeout, disconnect, every 5xx, malformed success and cancellation after `request_started_at`. A future 4xx may qualify only under a separately versioned, documented and cryptographically authenticated extension discriminator that proves rejection before execution; the current extension has no such discriminator, so status code alone never proves non-send. Persist only a SHA-256 response fingerprint and safe error code. Save `log_id` only from a schema-valid normal-send success, never from an error body.

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
- Produces: `confirm_sent(..., *, actor, reason, idempotency_key, expected_version)`, `cancel(...)`, `reauthorize(...)`; immutable resolution fact and command receipt

- [ ] **Step 1: Write causality tests**

```python
@pytest.mark.integration
async def test_reauthorize_creates_new_chain(service, unknown_job, actor):
    replacement = await service.reauthorize(
        unknown_job.id,
        actor=actor,
        reason="verified not sent",
        idempotency_key="reauthorize-1",
        expected_version=unknown_job.version,
    )
    assert replacement.approval_action_id != unknown_job.approval_action_id
    assert replacement.send_intent_id != unknown_job.send_intent_id
    assert replacement.send_outbox_id != unknown_job.send_outbox_id
    assert replacement.supersedes_send_intent_id == unknown_job.send_intent_id
    assert await service.status(unknown_job.id) == "send_unknown"
    assert await service.snapshot(unknown_job.id) == unknown_job


@pytest.mark.integration
async def test_three_resolution_actions_have_one_serialized_winner(
    service, unknown_job, actor
):
    commands = [
        service.confirm_sent(
            unknown_job.id, actor=actor, reason="evidence", idempotency_key="r-c", expected_version=1
        ),
        service.cancel(
            unknown_job.id, actor=actor, reason="cancel", idempotency_key="r-x", expected_version=1
        ),
        service.reauthorize(
            unknown_job.id, actor=actor, reason="retry", idempotency_key="r-r", expected_version=1
        ),
    ]
    results = await asyncio.gather(*commands, return_exceptions=True)
    assert sum(getattr(result, "committed", False) for result in results) == 1
    assert await service.resolution_count(unknown_job.id) == 1
    assert await service.superseding_intent_count(unknown_job.send_intent_id) <= 1


@pytest.mark.integration
async def test_reauthorize_commit_unknown_replays_receipt_once(
    service, unknown_job, actor, fault
):
    fault.lose_commit_ack_once("send_resolution.reauthorize")
    first = await service.reauthorize(
        unknown_job.id,
        actor=actor,
        reason="verified",
        idempotency_key="reauthorize-unknown-1",
        expected_version=unknown_job.version,
    )
    replay = await service.reauthorize(
        unknown_job.id,
        actor=actor,
        reason="verified",
        idempotency_key="reauthorize-unknown-1",
        expected_version=unknown_job.version,
    )
    assert replay == first
    assert await service.superseding_intent_count(unknown_job.send_intent_id) == 1
    with pytest.raises(IdempotencyConflict):
        await service.reauthorize(
            unknown_job.id,
            actor=actor,
            reason="different",
            idempotency_key="reauthorize-unknown-1",
            expected_version=unknown_job.version,
        )


@pytest.mark.integration
@pytest.mark.parametrize("blocked_by", ["source_deleted", "retired", "stale_fence"])
async def test_reauthorize_rejects_deleted_or_non_draining_old_work(
    service, unknown_job, actor, blocked_by
):
    await service.arrange_blocker_for_test(unknown_job.id, blocked_by)
    with pytest.raises(SendResolutionBlocked):
        await service.reauthorize(
            unknown_job.id,
            actor=actor,
            reason="retry",
            idempotency_key=f"reauthorize-{blocked_by}",
            expected_version=unknown_job.version,
        )
    assert await service.superseding_intent_count(unknown_job.send_intent_id) == 0
```

- [ ] **Step 2: Implement three explicit actions**

All three commands require an authorized actor, bounded reason, idempotency key and expected resolution/email version, and reuse `pipeline_command_receipts`. In one transaction they lock in fixed order: email, unknown Send attempt/intent, then its ownership row and current runtime authority. After actor/resource binding, they check the receipt first: same key/same canonical payload returns the stored result without applying current-version checks or doing work, while changed payload conflicts. Only a new command rechecks `send_unknown`, expected version, immutable attempt snapshot, generation/fence and existing resolution. State, one immutable `send_resolution_actions` row, audit and receipt commit together; commit-unknown recovery therefore reads the committed receipt before any retry action.

`confirm_sent` records operator evidence and resolves the aggregate as sent; `cancel` preserves the unknown attempt and prevents further work. `reauthorize` additionally requires `source_deleted_at IS NULL`. It creates a new approval action/send intent/outbox and links immutable supersedes IDs without mutating/resetting the old Send Outbox. A partial unique constraint on non-null `supersedes_send_intent_id` plus the one-resolution-per-unknown CAS prevents concurrent double-send creation. An old generation may be reauthorized only while its ownership row is still `draining` with the exact fence and current authority explicitly permits draining resolution; `retired`, stale-fence or unrelated-generation work is rejected. Concurrent confirm/cancel/reauthorize therefore has one terminal winner, and source deletion racing reauthorization serializes on the email row.

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
- Create: `src/ingestion/durable_adapter.py`
- Create: `tests/unit/outbox/test_runtime.py`
- Create: `tests/integration/approval/test_authority_dual_path.py`
- Create: `tests/architecture/test_phase3_activation_boundary.py`
- Modify: `src/ingestion/processing.py`
- Modify: `src/ingestion/worker.py`
- Modify: `src/ingestion/legacy_adapter.py`
- Modify: `src/init_app.py`
- Modify: `src/main.py`
- Modify: `src/graph/builder.py`
- Modify: `src/nodes/sender.py`
- Modify: `src/utils/lark_app.py`

**Interfaces:**
- Produces: one authority-observing `OutboxRuntime`; `DurableProcessingAdapter`, `DurableLegacyCompatAdapter`; stamped `ProcessingAdapterRouter`; the Durable Graph contains no direct Exchange node and retains its existing no-side-effect human boundary until Phase 4 adds `await_human`

- [ ] **Step 1: Write wiring tests**

```python
def test_durable_graph_contains_no_direct_sender(durable_compiled_graph):
    assert "sender" not in durable_compiled_graph.get_graph().nodes


def test_legacy_graph_keeps_only_guarded_sender(legacy_compiled_graph):
    sender = legacy_compiled_graph.get_graph().nodes["sender"]
    assert sender.requires_legacy_effect_guard is True


@pytest.mark.asyncio
async def test_runtime_stays_standby_before_durable_authority(runtime, authority):
    await authority.seed(mode="shadow", generation=4, fencing_token=40)
    await runtime.start()
    assert runtime.worker_names == set()
    assert runtime.accepting is False


@pytest.mark.asyncio
async def test_runtime_starts_workers_only_after_observing_current_authority(
    runtime, authority
):
    await authority.seed_for_test(
        mode="durable_active", generation=5, fencing_token=50, authority_epoch=8
    )
    await runtime.start()
    assert runtime.worker_names == {"notification", "mailbox", "send"}
    assert runtime.observed_authority == (5, 50, 8)
    await runtime.stop(grace_seconds=1.0)


@pytest.mark.integration
async def test_external_blocked_shadow_keeps_guarded_legacy_business_continuity(
    worker, authority, legacy_effects, legacy_external, outboxes
):
    await authority.seed(
        mode="shadow", pipeline_name="legacy_compat", generation=4, fencing_token=40
    )
    await worker.process_next_stamped_event()
    assert legacy_external.completed_business_effects() != set()
    assert await legacy_effects.completed_exact_identities() != set()
    assert await outboxes.count_for_pipeline("durable_candidate") == 0
    assert await outboxes.claim_count_for_pipeline("durable_candidate") == 0


@pytest.mark.integration
async def test_durable_active_new_inbox_uses_only_durable_adapter(
    worker, authority, durable_facts, outboxes, legacy_external
):
    await authority.seed_for_test(
        mode="durable_active",
        pipeline_name="durable_candidate",
        generation=5,
        fencing_token=50,
        authority_epoch=8,
    )
    await worker.process_next_stamped_event()
    assert await durable_facts.graph_and_draft_count() == 1
    assert await outboxes.created_kinds() <= {"notification", "mailbox", "send"}
    legacy_external.assert_no_calls()


@pytest.mark.integration
async def test_draining_old_stamp_cannot_process_as_current_durable(
    worker, authority, adapters
):
    await authority.seed_old_draining_and_new_current_for_test()
    await worker.process_old_stamped_event()
    assert adapters.selected == ["legacy_compat"]
    assert adapters.legacy_every_effect_was_guarded is True


@pytest.mark.integration
async def test_current_rollback_compat_generation_stays_durable_and_has_no_direct_effect(
    worker, authority, adapters, outboxes, legacy_external
):
    await authority.seed_for_test(
        mode="durable_active", pipeline_name="legacy_compat", generation=6, fencing_token=60
    )
    await worker.process_next_stamped_event()
    assert adapters.selected == ["durable_legacy_compat"]
    assert await outboxes.created_kinds() != set()
    legacy_external.assert_no_calls()


@pytest.mark.integration
async def test_late_legacy_card_callback_after_revoke_or_switch_is_stale(
    card_selector, authority, legacy_card, legacy_external
):
    await legacy_card.local_revoke_and_freeze()
    await authority.seed_for_test(mode="durable_active", generation=5, fencing_token=50)
    result = await card_selector.handle(legacy_card.replayed_callback())
    assert result.code == "stale_card"
    legacy_external.assert_no_calls()


@pytest.mark.integration
@pytest.mark.parametrize("winner", ["freeze", "effect_begin"])
async def test_card_freeze_and_legacy_callback_effect_begin_are_serialized(
    card_selector, card_freezer, legacy_card, legacy_effects, legacy_external, winner
):
    parsed = await card_selector.parse_only(legacy_card.callback())
    legacy_effects.install_card_begin_barrier(winner=winner)
    handling = asyncio.create_task(card_selector.execute(parsed))
    await legacy_effects.card_begin_barrier_reached.wait()
    freezing = asyncio.create_task(card_freezer.freeze_and_revoke(legacy_card.card_key))
    if winner == "freeze":
        await freezing
        legacy_effects.release_card_begin_barrier.set()
    else:
        legacy_effects.release_card_begin_barrier.set()
        await legacy_effects.execute_token_committed.wait()
        await freezing
    result = await handling
    assert (result.code, legacy_external.call_count) == (
        ("stale_card", 0) if winner == "freeze" else ("completed", 1)
    )
```

- [ ] **Step 2: Wire runtime, stamped processing adapters, and compatibility handlers**

AppContext owns repositories/services/runtime. Starting a process creates only clients and a zero-work standby. `OutboxRuntime` starts Notification/Mailbox/Send claims only after reading current PostgreSQL authority as `durable_active` and matching the exact generation, fence, authority epoch, build, protocol and config hash; an environment flag or a stale cached row cannot start it. Loss or change of authority stops new claims before bounded drain. Phase-2 Durable intake/Worker/Sync remain dormant under the same guard until Task 10 consumes the latest `production_ready` successor.

Implement `DurableProcessingAdapter` as the only processor for a current normal Durable stamp. It runs the no-direct-effect Graph, persists immutable draft/workflow facts, and transactionally creates the installed Notification/Mailbox/Send Outboxes; it has no callable Lark, Exchange mutation, send, or Qdrant client. It exposes a typed terminal-projection port but Phase 3 marks that capability unavailable; a requested `QDRANT_OUTBOX_ENABLED` profile cannot activate until Phase 4 binds the real Projection Outbox. `DurableLegacyCompatAdapter` is the forward-only rollback variant: it remains PostgreSQL Inbox/Outbox based and has the same zero-direct-effect ceiling while using compatibility business policy. `DurableInboxWorker` always selects from the lease's immutable `pipeline_name/generation/fencing_token` and a fresh authority read: a current normal Durable stamp selects `DurableProcessingAdapter`; a current `durable_active` rollback stamp named `legacy_compat` selects `DurableLegacyCompatAdapter`; a legacy-authoritative/Shadow or explicitly draining pre-switch stamp selects `LegacyProcessingAdapter`. No stamp may fall back across that boundary. The legacy adapter keeps the existing Graph/direct business behavior for continuity, but every ContentStore/model/Lark/Exchange/Qdrant boundary must win its exact `LegacyEffectGuard` registration. Dormant Durable work creates/claims nothing; after switch, new current work cannot enter the legacy monolith and old draining work cannot enter the new adapter.

Lark handlers use a parallel `CardActionSelector`: current Durable card resources call `ApprovalService`; a legacy callback may continue the guarded old behavior only while authority is exactly `legacy_authoritative` or `shadow`, the trusted resource is not locally revoked, and card-freeze/quiescing has not begun. Parsing/selector checks are not authorization. The actual legacy card effect begin takes the same per-account advisory lock as freeze/quiesce/switch, locks and re-reads trusted card local-revoke/freeze state plus authority, and atomically inserts the exact `pipeline_legacy_effects.started` token before returning permission. Freeze/revoke uses the same lock. If freeze wins, begin is rejected and remote call count is zero; if begin wins, only that one registered in-flight call may finish, then every replay is stale. Local revoke, card freeze, quiescing, draining, retired authority or a later switch otherwise always returns `stale_card`; draining alone never authorizes a callback. Neither selector trusts callback payload generation. `src/nodes/sender.py` remains an authority-selecting compatibility entry: legacy-authoritative/Shadow and exactly draining **already-stamped processing work** may delegate to the guarded legacy sender, while current Durable stamps fail closed and can send only via Send Outbox. It must not become an unconditional retired-path exception until the Phase-6 contraction gate has real v2 activation and production-stability evidence.

`tests/architecture/test_phase3_activation_boundary.py` scans production wiring and allows creation/consumption of `durable_active` authority only in Task 10's activation service. It also proves no module import, startup hook, flag tuple, Outbox runtime or Lark handler can activate a standby process before that transaction commits, and no Durable stamp can resolve to `LegacyProcessingAdapter`. The integration dual-path test is mandatory in the external-blocked state: legacy/Shadow still completes guarded cards/mark-read/send/Qdrant-compatible work, while the Durable candidate has zero business Outbox rows/claims; after test activation, the same new Inbox creates only durable facts/Outboxes and makes zero direct external call.

- [ ] **Step 3: Run the Phase 3 gate**

```bash
.venv/bin/python -m pytest tests/unit/approval tests/unit/outbox tests/architecture/test_phase3_activation_boundary.py -q
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/integration/approval -q
.venv/bin/python -m pytest --cov=src.approval --cov=src.outbox --cov-report=term-missing --cov-fail-under=90
.venv/bin/ruff check src/ tests/
.venv/bin/python -m pytest -q
git diff --check
```

- [ ] **Step 4: Commit**

```bash
git add src/outbox/runtime.py src/ingestion/durable_adapter.py src/ingestion/processing.py src/ingestion/worker.py src/ingestion/legacy_adapter.py src/init_app.py src/main.py src/graph/builder.py src/nodes/sender.py src/utils/lark_app.py tests/unit/outbox/test_runtime.py tests/integration/approval/test_authority_dual_path.py tests/architecture/test_phase3_activation_boundary.py
git commit -m "refactor: keep outbox workers dormant behind database authority"
```

---

### Task 10: Consume the Final Barrier and Activate Durable Runtime

**Files:**
- Create: `src/ingestion/activation.py`
- Create: `tests/unit/ingestion/test_activation_service.py`
- Create: `tests/integration/approval/test_durable_activation.py`
- Modify: `tests/architecture/test_phase3_activation_boundary.py`
- Modify: `src/ingestion/runtime_authority.py`
- Modify: `src/ingestion/cutover_barrier.py`
- Modify: `src/ingestion/runtime.py`
- Modify: `src/ingestion/processing.py`
- Modify: `src/ingestion/durable_adapter.py`
- Modify: `src/outbox/runtime.py`
- Modify: `src/init_app.py`
- Modify: `src/main.py`
- Modify: `src/commands/handlers.py`
- Modify: `scripts/manage_pipeline.py`

**Interfaces:**
- Consumes: Phase-2 immutable readiness predecessor, shared `pipeline_command_receipts`, `pipeline_legacy_effects`, per-FolderScope active-cursor/approved-boundary manifest, Phase-3 adapter/Outbox/action/card evidence and target standby roster
- Produces: `ActivationService.switch(...)`, `ActivationService.rollback(...)`; the only production transition to `durable_active`

- [ ] **Step 1: Write final-barrier, serialization and replay tests**

```python
@pytest.mark.integration
async def test_final_switch_revalidates_every_bound_evidence(activation, ready_barrier):
    await activation.mutate_phase3_manifest_for_test(ready_barrier.id)
    with pytest.raises(ActivationBlocked, match="evidence_drift"):
        await activation.switch(
            ready_barrier.id,
            actor="operator",
            reason="activate",
            idempotency_key="switch-1",
        )


@pytest.mark.integration
async def test_current_extension_contract_blocks_production_switch(
    activation, barrier_with_current_extension
):
    with pytest.raises(
        ActivationBlocked, match="blocked_external_exchange_sync_contract"
    ):
        await activation.switch(
            barrier_with_current_extension.id,
            actor="operator",
            reason="activate",
            idempotency_key="switch-current-extension",
        )
    assert (await activation.authority()).mode != "durable_active"


@pytest.mark.integration
async def test_effect_begin_and_switch_have_one_serialized_winner(
    activation, legacy_effects, ready_barrier
):
    effect, switched = await asyncio.gather(
        legacy_effects.try_begin("exchange_send"),
        activation.try_switch(
            ready_barrier.id,
            actor="operator",
            reason="activate",
            idempotency_key="switch-race-1",
        ),
    )
    assert (effect is not None, switched.activated) in {(True, False), (False, True)}


@pytest.mark.integration
async def test_switch_commit_unknown_replays_one_committed_receipt(
    activation, ready_barrier, fault
):
    fault.lose_commit_ack_once("pipeline.switch")
    first = await activation.switch(
        ready_barrier.id,
        actor="operator",
        reason="activate",
        idempotency_key="switch-commit-unknown",
    )
    replay = await activation.switch(
        ready_barrier.id,
        actor="operator",
        reason="activate",
        idempotency_key="switch-commit-unknown",
    )
    assert replay == first
    assert await activation.authority_epoch_increment_count() == 1
    assert await activation.receipt_count("pipeline.switch", "switch-commit-unknown") == 1
    assert await activation.consumption_count(first.production_ready_id) == 1
    with pytest.raises(IdempotencyConflict):
        await activation.switch(
            ready_barrier.id,
            actor="operator",
            reason="different",
            idempotency_key="switch-commit-unknown",
        )


@pytest.mark.integration
async def test_switch_is_one_atomic_authority_handoff(activation, ready_barrier):
    reserved = await activation.target_reservation(ready_barrier.id)
    result = await activation.switch(
        ready_barrier.id,
        actor="operator",
        reason="activate",
        idempotency_key="switch-atomic-1",
    )
    assert await activation.ownership_state(result.old_generation) == "draining"
    assert await activation.ownership_state(result.new_generation) == "current_ingress"
    assert (result.new_generation, result.new_fencing_token) == (
        reserved.generation,
        reserved.fencing_token,
    )
    assert await activation.authority() == (
        "durable_active",
        result.new_generation,
        result.new_fencing_token,
        result.new_authority_epoch,
    )
    assert await activation.barrier_state(ready_barrier.live_cutover_barrier_id) == "consumed"
    assert await activation.same_transaction_facts(result.transaction_id) == {
        "ownership",
        "authority",
        "target_reservation",
        "live_cutover_barrier",
        "activation_consumption",
        "command_receipt",
        "audit",
    }
    consumption = await activation.consumption_for(result.production_ready_id)
    assert (
        consumption.successor_id,
        consumption.live_cutover_barrier_id,
        consumption.switch_receipt_id,
        consumption.target_generation,
        consumption.target_fencing_token,
        consumption.authority_epoch,
    ) == (
        result.production_ready_id,
        ready_barrier.live_cutover_barrier_id,
        result.switch_receipt_id,
        reserved.generation,
        reserved.fencing_token,
        result.new_authority_epoch,
    )


@pytest.mark.integration
async def test_cutover_order_is_strict_and_switch_is_immediate(cutover):
    required = [
        "fence_verified",
        "target_reserved",
        "folder_boundaries_approved",
        "cards_frozen",
        "quiesced",
        "drained_or_quarantined",
        "legacy_cards_invalidated",
        "backfill_shadow_frozen",
        "legacy_workload_and_lb_removed",
        "credentials_rotated_and_connections_terminated",
        "fresh_roster_and_isolation_proof",
        "ready",
        "switched",
    ]
    assert await cutover.assert_only_sequence_is_accepted(required)
    assert await cutover.ready_to_switch_elapsed_seconds() < cutover.proof_ttl_seconds


@pytest.mark.integration
async def test_prepare_target_is_zero_work_and_all_folder_plans_fk_same_reservation(
    activation, folder_scopes, runtime
):
    live_barrier = await activation.plan_live_cutover()
    reservation = await activation.prepare_target(
        live_barrier.id, folder_scopes=folder_scopes
    )
    assert runtime.claim_count == runtime.effect_count == 0
    plans = await activation.folder_plans(reservation.id)
    assert {plan.target_reservation_id for plan in plans} == {reservation.id}
    assert {(plan.generation, plan.fencing_token) for plan in plans} == {
        (reservation.generation, reservation.fencing_token)
    }


@pytest.mark.integration
@pytest.mark.parametrize("disposition", ["cancelled", "expired"])
async def test_unused_target_reservation_retires_without_reusing_fence(
    activation, disposition
):
    first_barrier = await activation.plan_live_cutover()
    first = await activation.prepare_target(first_barrier.id)
    await activation.close_unused_reservation(first.id, disposition=disposition)
    second_barrier = await activation.plan_live_cutover()
    second = await activation.prepare_target(second_barrier.id)
    assert await activation.ownership_state(first.generation) == "retired"
    assert (second.generation, second.fencing_token) != (
        first.generation,
        first.fencing_token,
    )


@pytest.mark.integration
async def test_target_reservation_drift_blocks_ready_and_switch(activation, reserved_target):
    await activation.mutate_standby_config_for_test(reserved_target.id)
    with pytest.raises(ActivationBlocked, match="target_reservation_drift"):
        await activation.prepare_production_ready(reserved_target.capability_leaf_id)


@pytest.mark.integration
async def test_capability_leaf_without_ready_live_barrier_cannot_be_production_ready(
    activation, phase6_capability_leaf
):
    with pytest.raises(ActivationBlocked, match="live_cutover_barrier"):
        await activation.prepare_production_ready(phase6_capability_leaf.id)


@pytest.mark.integration
@pytest.mark.parametrize(
    "bad_state",
    ["missing", "cold_start_pending", "reset_required", "previewing", "ready", "blocked", "cancelled", "expired"],
)
async def test_each_folder_requires_target_bound_active_cursor_or_approved_boundary(
    activation, ready_barrier, bad_state
):
    await activation.replace_one_folder_boundary_for_test(state=bad_state)
    with pytest.raises(ActivationBlocked, match="folder_sync_boundary"):
        await activation.switch(
            ready_barrier.id,
            actor="operator",
            reason="activate",
            idempotency_key=f"switch-folder-{bad_state}",
        )


@pytest.mark.integration
async def test_mail_after_preview_is_processed_from_boundary_after_switch(
    activation, cold_start, exchange, ready_barrier, repo
):
    await cold_start.preview_and_approve_all_target_scopes(ready_barrier.id)
    exchange.inject_after_preview("mail-during-cutover")
    await activation.switch(
        ready_barrier.id,
        actor="operator",
        reason="activate",
        idempotency_key="switch-with-boundaries",
    )
    await cold_start.controlled_apply_all(ready_barrier.id)
    event = await repo.inbox_for("mail-during-cutover")
    assert event.processing_policy == "full"
    assert event.source == "sync"
```

- [ ] **Step 2: Separate implementation capabilities from the live cutover snapshot**

`phase3_base` is an implementation-capability successor only. It freezes the `0008` schema/digest, approval/action authority contract, adapter routing contract and Notification/Mailbox/Send Outbox fencing hashes. It does **not** require or claim quiescing, old-card invalidation, zero legacy effects, FolderScope cutover boundaries, LB isolation, credential rotation or a real v2 proof. Phase 4, Phase 5 and Phase 6 append only capability successors; therefore completing the whole external-blocked chain leaves legacy-authoritative/Shadow cards, approval, send, mark-read and Qdrant functioning through exact guards. An integration test completes all four capability stages and proves authority/legacy calls are unchanged and no Durable business Outbox is claimed.

Only after the separate extension release and real v2 proof does the workflow create a distinct Phase-2 live cutover barrier in `planned`, then `prepare_target(live_barrier_id)` takes the per-account advisory lock and creates the zero-work reserved ownership generation/fence/build/config plus reservation receipt. Every standby instance and approved FolderScope cursor/plan binds that exact reservation FK. The activation workflow then enters card freeze/quiescing, drains or quarantines legacy work, finishes every old-card invalidation, seals backfill/Shadow and folder manifests, removes old workload/LB routes, rotates credentials/connections and imports fresh isolation proofs before moving that same live barrier to `ready`. None of those live facts is copied into an earlier capability stage.

`prepare-production-ready` may append the complete terminal snapshot only when its `predecessor_successor_id` is the latest `phase6_implementation_complete_external_blocked` leaf and its `phase2_cutover_barrier_id` is that separate live barrier already in `ready`. The snapshot inherits every capability hash and additionally binds the real v2 proof, exact reserved target generation/fence/build/config, zero-work standby roster, complete approved FolderScope manifest, terminal old-card invalidation hash, zero started/unknown legacy effects and all fresh LB/credential/connection proofs. Missing/not-ready live barrier, reservation drift, mock/current v2 evidence or any pending/failed fact blocks both `production_ready` and switch. Tests prove implementation capability completion never touches production, and a capability leaf without a ready live barrier cannot become `production_ready`.

`prepare-base`, stage append, `prepare-target`, live `ready`, `prepare-production-ready`, `switch` and `rollback` use append-only command receipts. State/receipt/audit commit together; same key/same payload replays, changed payload conflicts, and every ready/switch call re-reads bound rows/hashes rather than trusting memory. Phase-3 tests may construct a synthetic live-ready barrier and terminal successor solely to test the atomic consumer; neither is production evidence.

- [ ] **Step 3: Implement the only atomic production switch**

The live cutover order is exact and test-enforced: prove real v2/build compatibility; persist the target reservation and zero-work standbys; approve every FolderScope boundary against its FK; freeze cards and quiesce; drain/quarantine work and complete old-card invalidation; freeze backfill/Shadow; remove old workload/LB routes; rotate effect/DB credentials and terminate old connections; import fresh roster/isolation proofs; mark the distinct live barrier ready; append the full `production_ready` snapshot; immediately switch before proof expiry. No step may be reordered, skipped or inferred from an environment flag.

`switch()` takes the same per-account advisory lock as effect begin/target preparation, then locks current authority, old ownership, reserved target ownership/reservation, latest `production_ready` successor and its live barrier. It revalidates the full capability chain, exact current head/schema/build/config, real v2 proof, adapter manifest, reservation and FolderScope FK equality, card/effect/isolation facts and proof TTL. In one transaction it changes old ownership `current_ingress -> draining`, promotes the **already-reserved ownership row** to `current_ingress` without changing its generation or fencing token, marks the reservation `promoted`, increments `authority_epoch`, sets authority to `durable_active`, moves the live barrier `ready -> consumed`, inserts the exact `pipeline.switch` receipt/audit and append-only `pipeline_activation_consumptions` relation. Any exception rolls back every fact. Lost commit acknowledgement replays from the receipt plus consumption row and never promotes/advances twice. Today's extension, a capability-only leaf, absent/not-ready live barrier, reservation drift or a switch-time fence change fails before mutation.

Only after standby processes observe the committed generation/fence/epoch may they start Durable Webhook 202 persistence and the controlled per-folder cold-start apply from each pre-switch boundary. Ordinary five-minute Sync scheduling starts only after every scope reaches its exact active cursor; mail received between preview and switch is therefore emitted normally from the boundary and is never included in historical suppression. Inbox claims and Notification/Mailbox/Send workers then select the exact target `DurableProcessingAdapter`; the old generation may only drain already-owned fenced work through `LegacyProcessingAdapter` and `LegacyEffectGuard`. Startup, flags, adapter fallback and card callbacks cannot bypass this observation gate.

- [ ] **Step 4: Implement forward-only Durable rollback**

Rollback never resurrects the pre-switch in-memory queue, old credentials or Shadow authority. A separately receipted `pipeline.rollback_quiesce` command first enters quiescing and drains or quarantines the failed Durable generation. The final `pipeline.rollback` command then atomically creates a fresh generation whose `pipeline_name='legacy_compat'`, rotates its fence, increments the authority epoch, returns to `durable_active`, and commits its own receipt plus audit in that same transaction. Despite the compatibility name, a current `durable_active` rollback generation resolves only to `DurableLegacyCompatAdapter`: it still consumes PostgreSQL Inbox, emits the installed four business Outboxes, and has zero direct Lark/Exchange/Qdrant effects. `LegacyProcessingAdapter` remains legal only for pre-switch legacy-authoritative/Shadow or already-draining old stamps. An integration test asserts the rollback generation selects `DurableLegacyCompatAdapter`, creates durable facts/Outboxes and makes no direct external call. The failed Durable generation remains draining/retired with immutable Inbox/Outbox history. Both commands use the same-key/same-payload replay, changed-payload conflict and commit-unknown recovery guarantees as switch; tests inject a lost final commit acknowledgement and prove exactly one new generation/epoch/receipt. The runtime path remains PostgreSQL Inbox/Outbox based.

Add architecture scans proving Phase 2 contains no production Durable transition and Phase 3 contains exactly one such authority mutation site. Add an integration test proving old cards are terminally invalidated before activation, a pending/failed invalidation blocks it, target standby does zero work before commit, and the exact target runtime begins only after reading the committed authority.

- [ ] **Step 5: Run the complete Phase 3 gate and commit**

```bash
.venv/bin/python -m pytest tests/unit/approval tests/unit/outbox tests/unit/ingestion/test_activation_service.py tests/architecture/test_phase3_activation_boundary.py -q
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/integration/approval -q
.venv/bin/python -m pytest --cov=src.approval --cov=src.outbox --cov=src.ingestion.activation --cov-report=term-missing --cov-fail-under=90
.venv/bin/ruff check src/ tests/
.venv/bin/python -m pytest -q
git diff --check
git add src/ingestion/activation.py src/ingestion/runtime_authority.py src/ingestion/cutover_barrier.py src/ingestion/runtime.py src/ingestion/processing.py src/ingestion/durable_adapter.py src/outbox/runtime.py src/init_app.py src/main.py src/commands/handlers.py scripts/manage_pipeline.py tests/unit/ingestion/test_activation_service.py tests/integration/approval/test_durable_activation.py tests/architecture/test_phase3_activation_boundary.py
git commit -m "feat: add gated fenced durable activation capability"
```
