# AI-Exchange Phase 4 Graph, Model, and Projection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 完成轻量 LangGraph、正确的 T1→T2→T3 分类顺序、真正生效的 Reviewer 重写循环、统一模型网关和可重建 Qdrant 投影。

**Architecture:** 新 Durable Graph 只编排确定性状态和不可变 ID，依赖通过 `GraphDependencies` 注入。T1 规则先短路，T2 检索旧邮件并产生经验提示，只有仍不确定时进入 T3 LLM。人工暂停只存在于 `await_human` 节点；Qdrant 写入由 Projection Outbox 异步完成。它在 legacy/Shadow 下保持 dormant，只有 stamped current Durable generation 才由 `ProcessingAdapterRouter` 选中；现网 legacy Graph/副作用在切换前继续经 `LegacyEffectGuard` 提供业务连续性。

**Tech Stack:** Python 3.12、LangGraph 1.x、Pydantic 2、LangChain、Qdrant、OpenAI-compatible providers、psycopg 3、pytest。

## Global Constraints

- Phase 3 已从 **Durable Graph** 移除直接发送；新 Graph 不得重新获得 Exchange/Lark 副作用权限。legacy-authoritative/Shadow 仍选择 guarded legacy Graph，切换后新代次选择新 Graph，旧代次只排已 stamped guarded work。
- Graph State 单次序列化必须小于 16 KiB，正文和检索完整对象不得写入 checkpoint。
- Provider SDK `max_retries=0`；只有 ModelGateway 执行一个重试层。
- 每个角色输入最多 131072 Token，角色可配置更低值。
- 模型或 Schema 失败进入 `manual_review`，不得生成 no-action 默认值。
- 当前邮件在检索时必须通过 `exclude_email_id` 排除；只有终态旧邮件可被检索。
- Qdrant 使用稳定 UUIDv5 Point ID、`wait=True`，失败不回滚主业务状态。
- 当前 extension 仍 external-blocked 时，部署本阶段不得切断 legacy cards/审批/发送/mark-read/Qdrant；Dormant Durable candidate creates/claims zero business Outbox.

---

## File Map

| Area | Files | Responsibility |
|---|---|---|
| Graph ports/build | `src/graph/ports.py`, `src/graph/dependencies.py`, `src/graph/nodes.py`, `src/graph/builder.py` | Explicitly inject all repositories/adapters and keep checkpoint state ID-only |
| Model boundary | `src/llm/budget.py`, `src/llm/gateway.py`, provider and every direct model caller | Enforce one retry layer, budgets, roles, schema and manual-review failure |
| Classification/review | `src/router/models.py`, `src/router/pipeline.py`, `src/graph/review_flow.py`, `src/graph/human.py` | T1→T2→T3 ordering, immutable rewrites and one human interrupt |
| Content lifecycle | `src/storage/backend.py`, `src/storage/repository.py`, `src/storage/migration.py`, `src/storage/rotation.py`, `src/storage/gc.py` | Tenant-scoped dedupe, references/holds, historical migration, key rotation and safe GC |
| Projection | `src/projections/qdrant.py`, `src/outbox/projection.py` | Rebuild Qdrant only from terminal business facts through a fenced Outbox |
| Checkpoints | `src/maintenance/checkpoint_cleanup.py` | Generate guarded cleanup plans and execute only with backup evidence |
| Tests | `tests/unit/graph/`, `tests/unit/llm/`, `tests/unit/router/`, `tests/unit/storage/`, `tests/integration/storage/`, `tests/integration/projections/` | Unit, PostgreSQL, failure and lifecycle evidence for the phase |

### Task 1: Complete Explicit Graph Dependency Injection

**Files:**
- Create: `src/graph/ports.py`
- Modify: `src/graph/dependencies.py`
- Create: `src/graph/nodes.py`
- Create: `tests/unit/graph/test_dependencies.py`
- Create: `tests/unit/graph/conftest.py`
- Modify: `src/graph/builder.py`
- Modify: `src/ingestion/durable_adapter.py`
- Modify: `src/ingestion/processing.py`
- Modify: `src/init_app.py`
- Modify: `src/nodes/categorizer.py`
- Modify: `src/nodes/retriever_node.py`
- Modify: `src/nodes/drafter.py`
- Modify: `src/nodes/reviewer.py`
- Create: `tests/integration/graph/test_authority_graph_selection.py`

**Interfaces:**
- Consumes: Phase 1 minimal `GraphDependencies`; `ContentStore`, `DraftRepositoryPort`, `RoutingPort`, `ModelGatewayPort`, `WorkflowPort`, `RetrieverPort`
- Produces: immutable `GraphDependencies`; callable node objects with no dynamic AppContext imports; authority-selected dormant/current Graph binding for `DurableProcessingAdapter` and `DurableLegacyCompatAdapter`

- [ ] **Step 1: Write dependency tests**

```python
def test_build_graph_requires_dependencies():
    with pytest.raises(TypeError, match="dependencies"):
        build_graph(checkpointer=MemorySaver())


def test_node_modules_do_not_import_app_context():
    for module in (categorizer, retriever_node, drafter, reviewer):
        source = inspect.getsource(module)
        assert "src.init_app" not in source
        assert "get_app_context" not in source


@pytest.mark.integration
async def test_graph_selection_preserves_shadow_and_switch_boundaries(
    selector, authority, legacy_graph, durable_graph
):
    await authority.seed(mode="shadow", pipeline_name="legacy_compat", generation=4)
    assert await selector.graph_for_current_stamp() is legacy_graph
    assert legacy_graph.every_external_effect_is_guarded is True
    await authority.seed_for_test(
        mode="durable_active", pipeline_name="durable_candidate", generation=5
    )
    assert await selector.graph_for_current_stamp() is durable_graph
    assert durable_graph.direct_external_clients == set()
```

- [ ] **Step 2: Define GraphDependencies**

```python
from __future__ import annotations


class ModelGatewayPort(Protocol):
    async def invoke_text(self, role: str, messages: Sequence[BaseMessage], budget: TokenBudget) -> str:
        raise NotImplementedError

    async def invoke_structured(
        self,
        role: str,
        messages: Sequence[BaseMessage],
        schema: type[BaseModel],
        budget: TokenBudget,
    ) -> BaseModel:
        raise NotImplementedError


class DraftRepositoryPort(Protocol):
    async def get(self, draft_version_id: str) -> DraftVersion:
        raise NotImplementedError

    async def create(self, snapshot: DraftSnapshotInput) -> DraftVersion:
        raise NotImplementedError


class RoutingPort(Protocol):
    async def classify(self, email_id: str, metadata: Mapping[str, Any], content: str) -> ClassificationDecision:
        raise NotImplementedError


class WorkflowPort(Protocol):
    async def get_status(self, email_id: str) -> str:
        raise NotImplementedError

    async def record_delta(self, email_id: str, delta: Mapping[str, Any]) -> None:
        raise NotImplementedError


class RetrieverPort(Protocol):
    async def search(
        self,
        query_text: str,
        sender: str | None,
        limit: int,
        exclude_email_id: str | None,
        terminal_only: bool,
    ) -> Sequence[Mapping[str, Any]]:
        raise NotImplementedError


@dataclass(frozen=True)
class GraphDependencies:
    content_store: ContentStore
    drafts: DraftRepositoryPort
    routing: RoutingPort
    models: ModelGatewayPort
    workflow: WorkflowPort
    retriever: RetrieverPort
```

`tests/unit/graph/conftest.py` defines `checkpointer`, `dependencies`, `flow`, `graph_runner` and deterministic fake Ports used in Tasks 1, 5 and 6. Every fake records calls and returns immutable DTOs; none imports AppContext or performs external I/O.

- [ ] **Step 3: Bind node callables**

Create callable node classes that hydrate content locally, invoke one dependency, and return a delta. `build_graph(checkpointer, dependencies)` constructs the Durable candidate. Inject it into `DurableProcessingAdapter` and `DurableLegacyCompatAdapter`; the latter keeps compatibility policy but still has no direct external clients. `ProcessingAdapterRouter` selects it only for the matching stamped current `durable_active` generation. Under legacy-authoritative/Shadow it selects the existing guarded legacy Graph; the new Graph remains dormant, and an old draining stamp cannot cross into it. Delete dynamic imports and global singleton access from new nodes without deleting the legacy Graph before Phase-6 contraction.

- [ ] **Step 4: Verify and commit**

```bash
.venv/bin/python -m pytest tests/unit/graph/test_dependencies.py tests/unit/test_nodes.py tests/unit/test_rag_nodes.py -q
git add src/graph/ports.py src/graph/dependencies.py src/graph/nodes.py src/graph/builder.py src/ingestion/durable_adapter.py src/ingestion/processing.py src/init_app.py src/nodes tests/unit/graph/conftest.py tests/unit/graph/test_dependencies.py tests/unit/test_nodes.py tests/unit/test_rag_nodes.py tests/integration/graph/test_authority_graph_selection.py
git commit -m "refactor: inject graph dependencies explicitly"
```

---

### Task 2: Build the Single-retry Model Gateway

**Files:**
- Create: `src/llm/__init__.py`
- Create: `src/llm/budget.py`
- Create: `src/llm/gateway.py`
- Create: `tests/unit/llm/test_gateway.py`
- Create: `tests/unit/llm/test_budget.py`
- Create: `tests/unit/llm/conftest.py`
- Modify: `src/providers/factory.py`
- Modify: `src/config.py`
- Modify: `src/utils/retry_decorator.py`
- Modify: `src/safety/model_budget.py`
- Modify: `src/nodes/categorizer.py`
- Modify: `src/nodes/drafter.py`
- Modify: `src/nodes/reviewer.py`
- Modify: `src/nodes/retriever_node.py`
- Modify: `src/router/engine.py`
- Modify: `src/scheduler/daily_summary.py`
- Modify: `src/memory/consolidator.py`
- Modify: `src/memory/preference_learner.py`
- Modify: `src/memory/style_profiler.py`
- Modify: `src/skills_discovery/analyzer.py`
- Modify: `src/utils/email_processor.py`
- Modify: `src/utils/image_analyzer.py`
- Modify: `src/utils/llm_factory.py`

**Interfaces:**
- Produces: `invoke_text(role, messages, budget)`, `invoke_structured(role, messages, schema, budget)`

- [ ] **Step 1: Write retry and budget tests**

```python
from langchain_core.messages import AIMessage, HumanMessage


@pytest.mark.asyncio
async def test_gateway_retries_only_transient_errors(gateway, provider):
    budget = TokenBudget(max_input_tokens=32, max_output_tokens=16)
    provider.ainvoke.side_effect = [httpx.ReadTimeout("timeout"), AIMessage(content="ok")]
    result = await gateway.invoke_text("drafter", [HumanMessage(content="hello")], budget=budget)
    assert result == "ok"
    assert provider.ainvoke.await_count == 2


@pytest.mark.asyncio
async def test_schema_error_is_not_retried(gateway, provider):
    messages = [HumanMessage(content="classify")]
    budget = TokenBudget(max_input_tokens=32, max_output_tokens=16)
    provider.ainvoke.return_value = AIMessage(content="not-json")
    with pytest.raises(ModelSchemaError):
        await gateway.invoke_structured("categorizer", messages, EmailClassification, budget)
    assert provider.ainvoke.await_count == 1
```

`tests/unit/llm/conftest.py` defines the `provider` AsyncMock, a gateway with deterministic limiter/circuit/clock and a minimal `EmailClassification` Pydantic model. Define `class ModelSchemaError(ManualReviewRequired)` so schema failure is terminal, not retried, and is caught by Graph as manual review.

- [ ] **Step 2: Disable provider retries and normalize role names**

Set OpenAI-compatible clients to `max_retries=0`. Define one role map containing `categorizer`, `drafter`, `reviewer`, `router`, `summary`, and `consolidator`; replace the current `summarizer` call with `summary`. Startup validates every configured role.

- [ ] **Step 3: Implement gateway error classes**

Gateway acquires per-provider limiter, checks circuit, enforces budget, makes at most two total attempts, retries only timeout/connect/429/502/503/504, validates Pydantic output, records role/outcome/latency/Token metrics, and raises `ManualReviewRequired` after terminal failure.

- [ ] **Step 4: Remove node-local retry decorators**

Delete `@with_llm_retry` usage from categorizer, drafter, reviewer, router and summary paths. Convert `src/safety/model_budget.py` into a compatibility import of `src.llm.budget` so Phase 1 limits have one implementation. Keep `with_simple_retry` only for explicitly classified non-model operations until Phase 6 cleanup.

- [ ] **Step 5: Verify and commit**

```bash
.venv/bin/python -m pytest tests/unit/llm tests/unit/test_llm_factory.py tests/unit/test_retry_logic.py tests/unit/test_retry_circuit_breaker_integration.py -q
git add src/llm src/safety/model_budget.py src/providers/factory.py src/config.py src/utils/retry_decorator.py src/nodes/categorizer.py src/nodes/drafter.py src/nodes/reviewer.py src/nodes/retriever_node.py src/router/engine.py src/scheduler/daily_summary.py src/memory/consolidator.py src/memory/preference_learner.py src/memory/style_profiler.py src/skills_discovery/analyzer.py src/utils/email_processor.py src/utils/image_analyzer.py src/utils/llm_factory.py tests/unit/llm tests/unit/test_llm_factory.py tests/unit/test_retry_logic.py tests/unit/test_retry_circuit_breaker_integration.py
git commit -m "feat: centralize model budgets and retry policy"
```

---

### Task 3: Reorder Classification to T1 then T2 then T3

**Files:**
- Create: `src/router/models.py`
- Create: `src/router/pipeline.py`
- Create: `tests/unit/router/test_classification_pipeline.py`
- Create: `tests/unit/router/conftest.py`
- Modify: `src/router/engine.py`
- Modify: `src/nodes/categorizer.py`
- Modify: `src/nodes/retriever_node.py`
- Modify: `src/graph/builder.py`

**Interfaces:**
- Produces: immutable `ClassificationDecision`, `HistoricalHints`; `ClassificationPipeline.classify(email_id, metadata, content) -> ClassificationDecision`

- [ ] **Step 1: Write order and short-circuit tests**

```python
@pytest.mark.asyncio
async def test_t1_match_skips_retrieval_and_llm(pipeline):
    pipeline.t1.return_value = ClassificationDecision(
        category="approval_required", need_reply=True, confidence=1.0, source="t1"
    )
    await pipeline.classify("m1", {"sender": "a@example.com"}, "content")
    pipeline.retriever.search.assert_not_called()
    pipeline.models.invoke_structured.assert_not_awaited()


@pytest.mark.asyncio
async def test_t2_hints_are_present_before_t3(pipeline):
    pipeline.t1.return_value = None
    pipeline.t2.return_value = HistoricalHints(
        confidence=0.4, labels=("reply",), guidance=("keep concise",)
    )
    await pipeline.classify("m1", {"sender": "a@example.com"}, "content")
    payload = pipeline.models.invoke_structured.await_args.args[1]
    assert "historical_hints" in "\n".join(str(message.content) for message in payload)
```

```python
@dataclass(frozen=True)
class ClassificationDecision:
    category: str
    need_reply: bool
    confidence: float
    source: Literal["t1", "t2", "t3"]


@dataclass(frozen=True)
class HistoricalHints:
    confidence: float
    labels: Sequence[str]
    guidance: Sequence[str]
```

`tests/unit/router/conftest.py` builds `pipeline` from `AsyncMock` T1/T2/retriever/model ports and explicit thresholds; it records order without network access.

- [ ] **Step 2: Implement the deterministic pipeline**

T1 returns only a complete high-confidence decision. T2 retrieves terminal historical records, applies label voting and experience hints, and returns a decision only above configured confidence. T3 receives T2 hints and calls ModelGateway. Final notification policy runs after a valid classification.

- [ ] **Step 3: Rebuild graph entry**

Graph entry becomes `classify`; remove the old categorizer-before-retriever sequence. Retrieval used for drafting still occurs after a valid need-reply decision, but classification retrieval happens inside the pipeline first.

- [ ] **Step 4: Verify and commit**

```bash
.venv/bin/python -m pytest tests/unit/router/test_classification_pipeline.py tests/unit/test_router_tier1.py tests/unit/test_router_tier2.py tests/unit/test_routing_integration.py -q
git add src/router/models.py src/router/pipeline.py src/router/engine.py src/nodes/categorizer.py src/nodes/retriever_node.py src/graph/builder.py tests/unit/router/conftest.py tests/unit/router/test_classification_pipeline.py tests/unit/test_router_tier1.py tests/unit/test_router_tier2.py tests/unit/test_routing_integration.py
git commit -m "fix: classify in T1 T2 T3 order"
```

---

### Task 4: Exclude the Current Mail and Restrict Historical Retrieval

**Files:**
- Create: `tests/unit/test_retriever_exclusion.py`
- Modify: `src/utils/retriever.py`
- Modify: `src/nodes/retriever_node.py`
- Modify: `src/memory/consolidator.py`

**Interfaces:**
- Produces: `EmailRetriever.search(query_text: str, *, sender: str | None = None, limit: int = 5, exclude_email_id: str | None = None, terminal_only: bool = True) -> list[dict[str, Any]]`

- [ ] **Step 1: Write self-retrieval tests**

```python
def test_search_excludes_current_email(retriever, qdrant):
    qdrant.query_points.return_value = SimpleNamespace(
        points=[
            SimpleNamespace(payload={"id": "current", "status": "sent"}),
            SimpleNamespace(payload={"id": "old", "status": "sent"}),
        ]
    )
    result = retriever.search("query", sender=None, limit=5, exclude_email_id="current", terminal_only=True)
    assert [item["id"] for item in result] == ["old"]
```

`tests/unit/test_retriever_exclusion.py` constructs `qdrant` as a `MagicMock` and injects it into `EmailRetriever`; no global client or live collection is used.

- [ ] **Step 2: Add Qdrant filters and defensive post-filtering**

Use a must-not match for current email ID and a must match for terminal projection status. Post-filter by ID/status as a second boundary in case older points lack payload fields.

- [ ] **Step 3: Verify and commit**

```bash
.venv/bin/python -m pytest tests/unit/test_retriever_exclusion.py tests/unit/test_retriever.py tests/unit/test_thread_aware_retrieval.py -q
git add src/utils/retriever.py src/nodes/retriever_node.py src/memory/consolidator.py tests/unit/test_retriever_exclusion.py tests/unit/test_retriever.py tests/unit/test_thread_aware_retrieval.py
git commit -m "fix: exclude current and nonterminal mail from retrieval"
```

---

### Task 5: Make Draft Review Loop Produce New Immutable Versions

**Files:**
- Create: `src/graph/review_flow.py`
- Create: `tests/unit/graph/test_review_flow.py`
- Modify: `src/nodes/drafter.py`
- Modify: `src/nodes/reviewer.py`
- Modify: `src/graph/builder.py`
- Replace: `tests/unit/test_reviewer_node.py`

**Interfaces:**
- Consumes: DraftRepository and ModelGateway
- Produces: `ReviewResult`, `ReviewFlowResult`; one draft version per generation/rewrite and `manual_review` after configured limit

- [ ] **Step 1: Replace the test that currently skips second review**

```python
@pytest.mark.asyncio
async def test_rewrite_is_reviewed_again(flow):
    flow.models.reviewer_results = [ReviewResult(False, "missing point"), ReviewResult(True, "")]
    result = await flow.run("mail-1")
    assert result.review_count == 2
    assert result.approved_draft_version == 2


@pytest.mark.asyncio
async def test_rewrite_limit_enters_manual_review(flow):
    flow.models.reviewer_results = [ReviewResult(False, "bad")] * 4
    result = await flow.run("mail-2")
    assert result.status == "manual_review"
    assert result.review_count == 3
```

```python
@dataclass(frozen=True)
class ReviewResult:
    approved: bool
    issues: str


@dataclass(frozen=True)
class ReviewFlowResult:
    status: Literal["reviewed", "manual_review"]
    review_count: int
    approved_draft_version: int | None
```

The Phase 4 graph conftest provides `flow` with a `FakeModelGateway` whose `reviewer_results` queue is consumed once per review and a fake DraftRepository that assigns monotonically increasing versions.

- [ ] **Step 2: Implement feedback consumption and immutable versions**

Drafter receives Reviewer issues, creates a new DraftVersion and returns only its ID/hash. Reviewer loads that version, checks it, and either loops with issues or marks it reviewed. Do not return the entire draft body to State.

- [ ] **Step 3: Verify and commit**

```bash
.venv/bin/python -m pytest tests/unit/graph/test_review_flow.py tests/unit/test_reviewer_node.py tests/unit/test_drafter_modifier.py -q
git add src/graph/review_flow.py src/nodes/drafter.py src/nodes/reviewer.py src/graph/builder.py tests/unit/graph/test_review_flow.py tests/unit/test_reviewer_node.py tests/unit/test_drafter_modifier.py
git commit -m "fix: review every rewritten draft version"
```

---

### Task 6: Add the Single await_human Interrupt Boundary

**Files:**
- Create: `src/graph/human.py`
- Create: `tests/unit/graph/test_human_interrupt.py`
- Modify: `src/graph/builder.py`

**Interfaces:**
- Produces: `await_human` node; no `interrupt_after=["reviewer"]`

- [ ] **Step 1: Write interrupt-count tests**

```python
@pytest.mark.asyncio
async def test_rewrite_then_pass_interrupts_once(graph_runner):
    events = await graph_runner.run(reviewer_results=["rewrite", "pass"])
    assert [event.node for event in events].count("await_human") == 1
    assert graph_runner.card.draft_version_id == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ["rejected", "no_action", "manual_review"])
async def test_nonapproval_paths_never_interrupt(graph_runner, terminal):
    events = await graph_runner.run(terminal_result=terminal)
    assert [event.node for event in events].count("await_human") == 0
```

- [ ] **Step 2: Implement explicit human boundary**

On the authority-selected Durable Graph, Reviewer pass creates Notification Outbox and enters `await_human`, which calls LangGraph `interrupt()` with only email ID, draft version/hash and approval version. Reject/no-action/manual paths end without an approval interrupt. This does not rewrite the guarded legacy Graph: under legacy-authoritative/Shadow its existing notification/human behavior remains available through `LegacyProcessingAdapter` and `LegacyEffectGuard`, while the Durable candidate creates/claims no row.

- [ ] **Step 3: Verify and commit**

```bash
.venv/bin/python -m pytest tests/unit/graph/test_human_interrupt.py tests/unit/graph/test_review_flow.py -q
git add src/graph/human.py src/graph/builder.py tests/unit/graph/test_human_interrupt.py tests/unit/graph/test_review_flow.py
git commit -m "fix: interrupt only at reviewed human approval"
```

---

### Task 7: Mature ContentStore Lifecycle, Migration, and Key Rotation

**Files:**
- Create: `alembic/versions/20260713_0009_content_lifecycle.py`
- Create: `src/storage/backend.py`
- Create: `src/storage/repository.py`
- Create: `src/storage/migration.py`
- Create: `src/storage/rotation.py`
- Create: `src/storage/gc.py`
- Create: `tests/unit/storage/test_backend_contract.py`
- Create: `tests/unit/storage/test_rotation.py`
- Create: `tests/integration/storage/test_content_lifecycle.py`
- Create: `tests/integration/storage/test_legacy_content_migration.py`
- Modify: `tests/unit/storage/conftest.py`
- Create: `tests/integration/storage/conftest.py`
- Modify: `src/storage/content_store.py`
- Modify: `src/storage/encrypted_files.py`
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
- Create: `tests/integration/migrations/test_0008_to_0009.py`

**Interfaces:**
- Consumes: Phase 1 `ContentStore`, `ContentRef`, encrypted file layout and immutable email facts
- Produces: `ContentBackend` Port; account-scoped content/artifact repository; `LegacyContentMigrator`; `KeyRotator`; hold-aware `ContentGarbageCollector`

Migration revision is exactly `20260713_0009` with linear `down_revision = "20260713_0008"`.

- [ ] **Step 1: Write cross-account, reference, hold, migration, and rotation tests**

```python
from datetime import UTC, datetime, timedelta

import pytest


@pytest.mark.asyncio
async def test_dedupe_never_crosses_account_boundary(store):
    first = await store.put_blob(account_id=8, media_type="text/plain", content=b"same")
    same_account = await store.put_blob(account_id=8, media_type="text/plain", content=b"same")
    other_account = await store.put_blob(account_id=9, media_type="text/plain", content=b"same")
    assert first.object_id == same_account.object_id
    assert first.object_id != other_account.object_id


@pytest.mark.asyncio
async def test_send_hold_and_reference_prevent_garbage_collection(gc, repo):
    expired = datetime.now(UTC) - timedelta(days=1)
    referenced = await repo.seed_content(ref_count=1, expires_at=expired)
    held = await repo.seed_content(ref_count=0, expires_at=expired, hold="send_unknown")
    unreferenced = await repo.seed_content(ref_count=0, expires_at=expired)
    plan = await gc.plan(now=datetime.now(UTC), limit=100)
    assert [item.object_id for item in plan] == [unreferenced.object_id]
    assert referenced.object_id not in {item.object_id for item in plan}
    assert held.object_id not in {item.object_id for item in plan}


@pytest.mark.asyncio
async def test_rotation_reencrypts_without_changing_plaintext(store, rotator):
    ref = await store.put_blob(account_id=8, media_type="text/plain", content=b"secret")
    rotated = await rotator.rotate(ref, target_key_version="v2")
    assert rotated.key_version == "v2"
    assert await store.load_blob(rotated) == b"secret"
```

Extend the Phase 1 storage conftest with in-memory backend, `repo`, `gc` and `rotator`; the integration conftest binds the same repository to the shared migrated PostgreSQL schema and provides legacy source rows, reference/hold seed methods and a fixed clock.

- [ ] **Step 2: Add the exact lifecycle schema**

Migration creates `email_contents(id UUID PRIMARY KEY, account_id BIGINT, content_hash CHAR(64), backend TEXT, object_key TEXT, key_version TEXT, media_type TEXT, byte_size BIGINT, ref_count BIGINT DEFAULT 0 CHECK(ref_count>=0), expires_at TIMESTAMPTZ, created_at TIMESTAMPTZ, UNIQUE(account_id, content_hash))`; `email_artifacts(id UUID PRIMARY KEY, email_id TEXT, content_id UUID REFERENCES email_contents, filename_hash TEXT, detected_type TEXT, byte_size BIGINT, disposition TEXT, created_at TIMESTAMPTZ)`; `content_references(owner_type, owner_id, content_id, created_at, PRIMARY KEY(owner_type, owner_id, content_id))`; `content_holds(content_id, hold_type, owner_id, expires_at, created_at, PRIMARY KEY(content_id, hold_type, owner_id))`; and migration/rotation job tables with idempotency keys and cursors.

`0009` is a complete revision-contract change in this same task: advance the single exact application head and schema digest; update bootstrap pre/post checks, all four ACL manifests, checkpoint revision allowlist and offline SQL; grant runtime/maintenance only their required content/reference/job columns, auditor SELECT-only and DDL only to migration. `tests/integration/migrations/test_0008_to_0009.py` creates real PostgreSQL at `0008`, seeds representative approval/Outbox/content references with all Phase-4 profiles disabled, runs the code-first `0008 -> 0009` bridge, verifies row preservation/roles/schema/startup and a second no-op upgrade, and proves an old `0008` binary rejects a database-first `0009` head. Empty-DB and downgrade-refusal paths remain single-head.

- [ ] **Step 3: Split metadata from backend bytes**

```python
class ContentBackend(Protocol):
    async def put(self, object_key: str, ciphertext: bytes) -> None:
        raise NotImplementedError

    async def get(self, object_key: str) -> bytes:
        raise NotImplementedError

    async def delete(self, object_key: str) -> None:
        raise NotImplementedError

    async def exists(self, object_key: str) -> bool:
        raise NotImplementedError
```

`ContentStore` allocates/deduplicates only inside `(account_id, sha256)`, encrypts with AAD derivable from `ContentRef`, writes bytes atomically, then commits metadata/reference. On metadata failure it deletes the orphan; startup scans only app-owned random temporary names older than 24 hours. Backend selection is configuration-driven; Phase 1 encrypted files remain the production backend and tests run the same contract against an in-memory backend.

- [ ] **Step 4: Migrate history and rotate keys idempotently**

`LegacyContentMigrator --dry-run` counts Graph/legacy bodies and attachments without loading all rows; execute writes new content, verifies hash/decryption, adds references and only then clears the old payload. Re-running skips verified rows by source ID/hash. New writes use the active key; `KeyRotator` claims bounded jobs, decrypts old versions, writes new ciphertext atomically, verifies plaintext hash, switches metadata and retains old ciphertext until the next verified cleanup window.

- [ ] **Step 5: Verify and commit**

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/unit/storage tests/integration/storage tests/integration/migrations/test_0008_to_0009.py tests/integration/ingestion/test_schema.py tests/integration/ingestion/test_access_roles.py -q
.venv/bin/python -m pytest tests/unit/test_database_revision.py tests/unit/test_database_schema_contract.py tests/unit/test_checkpoint_repository_safety.py tests/unit/test_alembic_offline.py -q
git add alembic/versions/20260713_0009_content_lifecycle.py src/storage src/config.py .env.example src/db/access_contract.py src/db/bootstrap.py src/db/schema.py src/db/schema_contract.py src/maintenance/checkpoint_repository.py tests/unit/storage tests/integration/storage tests/unit/test_database_revision.py tests/unit/test_database_schema_contract.py tests/unit/test_checkpoint_repository_safety.py tests/unit/test_alembic_offline.py tests/integration/ingestion/test_schema.py tests/integration/ingestion/test_access_roles.py tests/integration/migrations/test_0008_to_0009.py
git commit -m "feat: complete encrypted content lifecycle"
```

Expected: restart, dedupe, account isolation, migration re-run, rotation, reference/hold and orphan cleanup tests all pass; `send_unknown`/`accepted` content is never eligible for GC.

---

### Task 8: Move Qdrant Writes to Projection Outbox

**Files:**
- Create: `alembic/versions/20260713_0010_projection_outbox.py`
- Create: `src/projections/__init__.py`
- Create: `src/projections/qdrant.py`
- Create: `src/outbox/projection.py`
- Create: `tests/unit/projections/test_qdrant_projection.py`
- Create: `tests/integration/projections/test_projection_outbox.py`
- Create: `tests/integration/projections/test_authority_dual_path.py`
- Create: `tests/unit/projections/conftest.py`
- Modify: `src/exchange_service.py`
- Modify: `src/utils/email_processor.py`
- Modify: `src/outbox/runtime.py`
- Modify: `src/outbox/send.py`
- Modify: `src/outbox/mailbox.py`
- Modify: `src/approval/service.py`
- Modify: `src/approval/send_resolution.py`
- Modify: `src/ingestion/repository.py`
- Modify: `src/ingestion/durable_adapter.py`
- Modify: `src/ingestion/processing.py`
- Modify: `src/ingestion/cutover_barrier.py`
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
- Create: `tests/integration/migrations/test_0009_to_0010.py`

**Interfaces:**
- Produces: `EmailProjection`, stable point ID, idempotent fenced `ProjectionWorker`, no direct pre-classification write

Migration revision is exactly `20260713_0010` with linear `down_revision = "20260713_0009"`.

- [ ] **Step 1: Write ordering and wait tests**

```python
@pytest.mark.asyncio
async def test_current_mail_is_not_projected_before_terminal(worker, repo):
    await repo.seed_email(status="processing")
    assert await worker.run_once() == 0


@pytest.mark.asyncio
async def test_qdrant_upsert_waits_for_commit(projector, qdrant):
    projection = EmailProjection(
        account_id=8,
        email_id="mail-1",
        projection_type="terminal_email",
        status="sent",
        text="redacted summary",
        generation=4,
        fencing_token=40,
    )
    await projector.upsert_email(projection)
    assert qdrant.upsert.call_args.kwargs["wait"] is True


@pytest.mark.asyncio
async def test_projection_completion_rejects_rotated_fence(worker, repo, qdrant):
    job = await repo.seed_projection(generation=4, fencing_token=40)
    qdrant.upsert.side_effect = lambda *args, **kwargs: repo.rotate_fence_sync(
        job.account_id, generation=4, fencing_token=41
    )
    with pytest.raises(StaleFence):
        await worker.deliver(job)
    assert await repo.status(job.id) == "leased"


@pytest.mark.integration
async def test_shadow_legacy_qdrant_continuity_is_exactly_guarded(
    selector, legacy_event, legacy_effects, qdrant
):
    await selector.process(legacy_event, authority="shadow")
    assert qdrant.upsert.call_count == 1
    assert await legacy_effects.completed_kind("qdrant") == 1


@pytest.mark.integration
async def test_durable_projection_uses_outbox_and_never_direct_qdrant(
    selector, durable_terminal_event, repo, qdrant
):
    await selector.process(durable_terminal_event, authority="durable_active")
    assert await repo.projection_outbox_count(durable_terminal_event.email_id) == 1
    qdrant.upsert.assert_not_called()
```

```python
@dataclass(frozen=True)
class EmailProjection:
    account_id: int
    email_id: str
    projection_type: str
    status: str
    text: str
    generation: int
    fencing_token: int
```

`tests/unit/projections/conftest.py` defines `projector`, `qdrant`, `worker` and an in-memory/fake repository that enforces generation/fence; integration tests use the shared migrated PostgreSQL fixture.

- [ ] **Step 2: Implement projection migration and Worker**

Projection rows use unique business key and stable UUIDv5 of account/email/projection type. Every row freezes `generation` and `fencing_token`; claim and completion verify both. Only terminal transitions in the authority-selected Durable ingestion, approval, send/manual resolution and mailbox services create rows. Qdrant errors retry independently and never roll back email state. Remove `_ingest_to_qdrant()` and direct `process_sent_email()` only from `DurableProcessingAdapter`/`DurableLegacyCompatAdapter`; while authority is legacy-authoritative/Shadow, retain the existing direct Qdrant implementation exclusively behind `LegacyProcessingAdapter` plus exact `LegacyEffectGuard`, and allow an old generation only to finish an already-stamped guarded effect. Phase-6 post-activation contraction deletes it after the stability gate.

Bind the real `ProjectionOutboxPort` into both Durable adapters and add `ProjectionWorker` to `OutboxRuntime` only when current authority/generation/fence and the installed capability manifest match. Append an immutable `phase4_graph_projection` activation-barrier successor that references the Phase-3 base and freezes the new Graph hash, adapter routing contract, all four business Outbox fencing contracts and exact `0010` schema/build/config manifest. It remains non-consumable and cannot become `production_ready`.

`0010` is another complete revision-contract change: advance the exact single head/schema digest, bootstrap checks, four ACL manifests, checkpoint allowlist and offline SQL. Runtime receives only projection enqueue/lease/complete privileges; maintenance gets bounded rebuild/inspection rights; auditor is SELECT-only; migration alone owns DDL. `tests/integration/migrations/test_0009_to_0010.py` uses real PostgreSQL with all projection/Durable profiles disabled to prove code-first `0009 -> 0010`, seed preservation, role behavior, startup, second no-op upgrade, old-binary head rejection and no split head.

- [ ] **Step 3: Verify and commit**

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/unit/projections/test_qdrant_projection.py tests/integration/projections/test_projection_outbox.py tests/integration/projections/test_authority_dual_path.py tests/integration/migrations/test_0009_to_0010.py tests/integration/ingestion/test_schema.py tests/integration/ingestion/test_access_roles.py -q
.venv/bin/python -m pytest tests/unit/test_database_revision.py tests/unit/test_database_schema_contract.py tests/unit/test_checkpoint_repository_safety.py tests/unit/test_alembic_offline.py -q
git add alembic/versions/20260713_0010_projection_outbox.py src/projections src/outbox/projection.py src/outbox/runtime.py src/outbox/send.py src/outbox/mailbox.py src/approval/service.py src/approval/send_resolution.py src/ingestion/repository.py src/ingestion/durable_adapter.py src/ingestion/processing.py src/ingestion/cutover_barrier.py src/exchange_service.py src/utils/email_processor.py src/db/access_contract.py src/db/bootstrap.py src/db/schema.py src/db/schema_contract.py src/maintenance/checkpoint_repository.py tests/unit/projections/conftest.py tests/unit/projections/test_qdrant_projection.py tests/integration/projections/test_projection_outbox.py tests/integration/projections/test_authority_dual_path.py tests/unit/test_database_revision.py tests/unit/test_database_schema_contract.py tests/unit/test_checkpoint_repository_safety.py tests/unit/test_alembic_offline.py tests/integration/ingestion/test_schema.py tests/integration/ingestion/test_access_roles.py tests/integration/migrations/test_0009_to_0010.py
git commit -m "feat: project terminal mail through durable outbox"
```

---

### Task 9: Enforce Guarded Checkpoint Retention and Complete Phase Gate

**Files:**
- Modify: `src/maintenance/checkpoint_cleanup.py`
- Modify: `tests/unit/test_checkpoint_cleanup_plan.py`
- Modify: `tests/integration/test_checkpoint_cleanup.py`
- Modify: `src/outbox/runtime.py`
- Modify: `src/main.py`

**Interfaces:**
- Produces: `CheckpointRetentionPolicy`; terminal checkpoint candidates within 24 hours, approval expiry at 7 days, and backup-gated bounded execution

- [ ] **Step 1: Write policy tests**

```python
def test_terminal_checkpoint_expires_after_24_hours():
    policy = CheckpointRetentionPolicy()
    assert policy.should_delete(status="sent", age=timedelta(hours=25)) is True
    assert policy.should_delete(status="waiting_approval", age=timedelta(days=6)) is False


def test_waiting_approval_expires_at_seven_days():
    policy = CheckpointRetentionPolicy()
    decision = policy.expiry_action(status="waiting_approval", age=timedelta(days=8))
    assert decision.status == "expired"
    assert decision.invalidate_cards is True
```

- [ ] **Step 2: Implement scheduled candidate planning and guarded execution**

The scheduler only writes candidate plans. Terminal workflow first writes a compact audit summary; waiting approval reaches expired at seven days and atomically enqueues card invalidation. A checkpoint is eligible only after that Outbox is durably complete. Execution requires a verified backup/snapshot ID, processes at most 500 rows, records cursor/statistics and resumes after failure. Always exclude `waiting_approval`, `accepted`, `send_unknown`, in-flight and unmatched-generation records.

- [ ] **Step 3: Run Phase 4 gate and commit**

```bash
.venv/bin/python -m pytest tests/unit/graph tests/unit/llm tests/unit/router tests/unit/storage tests/unit/projections tests/integration/storage tests/integration/test_checkpoint_cleanup.py -q
.venv/bin/python -m pytest --cov=src.graph --cov=src.llm --cov=src.router --cov=src.projections --cov-report=term-missing --cov-fail-under=90
.venv/bin/ruff check src/ tests/
.venv/bin/python -m pytest -q
git diff --check
git add src/maintenance/checkpoint_cleanup.py src/outbox/runtime.py src/main.py tests/unit/test_checkpoint_cleanup_plan.py tests/integration/test_checkpoint_cleanup.py
git commit -m "feat: enforce bounded workflow checkpoint retention"
```
