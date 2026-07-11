import asyncio
from contextlib import ExitStack
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.runnables import RunnableLambda
from langgraph.checkpoint.memory import InMemorySaver

from src.exchange_service import _dispatch_notification, _run_ai_pipeline
from src.graph.builder import build_graph
from src.graph.dependencies import GraphDependencies
from src.graph.state_factory import (
    MAX_CHECKPOINT_BYTES,
    build_initial_graph_state,
    sanitize_graph_delta,
)
from src.storage import ContentRef
from src.utils import lark_app


class RecordingInMemorySaver(InMemorySaver):
    def __init__(self):
        super().__init__()
        self.records = []

    def _record(self, method, kind, value):
        typed = self.serde.dumps_typed(value)
        self.records.append((method, kind, deepcopy(value), typed))

    def put(self, config, checkpoint, metadata, new_versions):
        self._record("put", "checkpoint", checkpoint)
        self._record("put", "metadata", metadata)
        for channel, value in checkpoint.get("channel_values", {}).items():
            self._record("put", f"channel:{channel}", value)
        return super().put(config, checkpoint, metadata, new_versions)

    async def aput(self, config, checkpoint, metadata, new_versions):
        self._record("aput", "checkpoint", checkpoint)
        self._record("aput", "metadata", metadata)
        for channel, value in checkpoint.get("channel_values", {}).items():
            self._record("aput", f"channel:{channel}", value)
        return await super().aput(config, checkpoint, metadata, new_versions)

    def put_writes(self, config, writes, task_id, task_path=""):
        writes = list(writes)
        self._record("put_writes", "writes", writes)
        for channel, value in writes:
            self._record("put_writes", f"write:{channel}", value)
        return super().put_writes(config, writes, task_id, task_path)

    async def aput_writes(self, config, writes, task_id, task_path=""):
        writes = list(writes)
        self._record("aput_writes", "writes", writes)
        for channel, value in writes:
            self._record("aput_writes", f"write:{channel}", value)
        return await super().aput_writes(config, writes, task_id, task_path)


class FakeContentStore:
    def __init__(self, email):
        self.email = deepcopy(email)
        self.loads = []

    async def load_email(self, ref, *, include_attachments=False):
        self.loads.append((ref, include_attachments))
        return deepcopy(self.email)


class FakeDraftStore:
    def __init__(self):
        self.values = {}
        self.saves = []
        self.loads = []

    async def save_draft(self, email_id, content):
        self.saves.append((email_id, content))
        self.values[email_id] = content
        return email_id

    async def load_draft(self, draft_id):
        self.loads.append(draft_id)
        return self.values[draft_id]


def _ref():
    return ContentRef(
        account_id=8,
        object_id="00000000-0000-4000-8000-000000000127",
        key_version="v1",
        sha256="c" * 64,
    )


def _classification_retry(**_kwargs):
    def decorator(_function):
        async def wrapped(_payload):
            return {
                "priority": "P3",
                "need_reply": False,
                "intent": "通知",
                "summary": "small",
                "reasoning": "small",
                "confidence": 1.0,
            }

        return wrapped

    return decorator


@pytest.mark.asyncio
async def test_recording_saver_observes_real_compiled_flow(monkeypatch):
    monkeypatch.setattr(
        "src.graph.state_factory.get_settings",
        lambda: SimpleNamespace(EXCHANGE_ACCOUNT_ID=8),
    )
    monkeypatch.setattr(
        "src.exchange_service.get_settings",
        lambda: SimpleNamespace(EXCHANGE_ACCOUNT_ID=8),
    )
    email = {
        "id": "checkpoint-mail",
        "subject": "subject",
        "sender": "sender@example.com",
        "body": "local body",
        "attachments": [],
    }
    dependencies = GraphDependencies(
        content_store=FakeContentStore(email),
        drafts=FakeDraftStore(),
    )
    saver = RecordingInMemorySaver()
    graph = build_graph(checkpointer=saver, dependencies=dependencies)
    router = MagicMock()
    router.execute_router = AsyncMock(side_effect=lambda state: state)

    with patch("src.nodes.categorizer.get_routing_engine", return_value=router), patch(
        "src.nodes.categorizer.with_llm_retry", side_effect=_classification_retry
    ), patch(
        "src.providers.factory.get_llm_for_role",
        return_value=RunnableLambda(lambda value: value),
    ):
        await graph.ainvoke(
            build_initial_graph_state(email, _ref()),
            config={"configurable": {"thread_id": "checkpoint-mail"}},
        )

    methods = {record[0] for record in saver.records}
    assert methods == {"put", "aput", "put_writes", "aput_writes"}
    assert saver.records
    for _method, _kind, _decoded, (tag, payload) in saver.records:
        assert len(tag.encode("utf-8")) + len(payload) < MAX_CHECKPOINT_BYTES


def _draft_retry(contents):
    remaining = iter(contents)

    def factory(**_kwargs):
        def decorator(_function):
            async def wrapped(_payload):
                return SimpleNamespace(content=next(remaining))

            return wrapped

        return decorator

    return factory


def _review_retry(**_kwargs):
    def decorator(_function):
        async def wrapped(_payload):
            return SimpleNamespace(
                content='{"pass": false, "issues": "REVIEW-ISSUE-SENTINEL"}'
            )

        return wrapped

    return decorator


def _walk_values(value, seen=None):
    seen = seen or set()
    identity = id(value)
    if identity in seen:
        return
    seen.add(identity)
    if isinstance(value, (str, bytes, bytearray)):
        yield value
        return
    if isinstance(value, dict):
        for key, item in value.items():
            yield from _walk_values(key, seen)
            yield from _walk_values(item, seen)
        return
    if isinstance(value, (list, tuple, set, frozenset)):
        for item in value:
            yield from _walk_values(item, seen)
        return
    attributes = getattr(value, "__dict__", None)
    if isinstance(attributes, dict):
        yield from _walk_values(attributes, seen)


@pytest.mark.asyncio
async def test_compiled_flow_never_checkpoints_complete_content(monkeypatch):
    monkeypatch.setattr(
        "src.graph.state_factory.get_settings",
        lambda: SimpleNamespace(EXCHANGE_ACCOUNT_ID=8),
    )
    monkeypatch.setattr(
        "src.exchange_service.get_settings",
        lambda: SimpleNamespace(EXCHANGE_ACCOUNT_ID=8),
    )
    body_marker = "BODY-10M-SENTINEL"
    body = body_marker + "b" * (10 * 1024 * 1024)
    base64_marker = "QVRUQUNITUVOVC1CQVNFNjQtU0VOVElORUw="
    first_draft = "FIRST-DRAFT-SENTINEL-" + "d" * 20_000
    second_draft = "SECOND-DRAFT-SENTINEL-" + "e" * 20_000
    human_draft = "FULL-FEEDBACK-SENTINEL-" + "f" * 25_000
    complete_hit = "h" * 20_000 + "FULL-CHUNK-END-SENTINEL"
    email = {
        "id": "checkpoint-full-flow",
        "subject": "subject",
        "sender": "sender@example.com",
        "body": body,
        "attachments": [{"name": "secret.bin", "content": base64_marker}],
        "_image_attachments": [{"content": base64_marker}],
        "draft_to": ["recipient@example.com"],
    }
    content_store = FakeContentStore(email)
    drafts = FakeDraftStore()
    dependencies = GraphDependencies(content_store=content_store, drafts=drafts)
    saver = RecordingInMemorySaver()
    graph = build_graph(checkpointer=saver, dependencies=dependencies)
    config = {"configurable": {"thread_id": email["id"]}}

    router = MagicMock()
    router.execute_router = AsyncMock(side_effect=lambda state: state)
    router.apply_tier2_hits = AsyncMock(return_value={})
    retriever = MagicMock()
    retriever.search.return_value = [
        {
            "id": "old-mail",
            "sender": "old@example.com",
            "subject": "old subject",
            "chunk_text": complete_hit,
        }
    ]
    draft_retry = _draft_retry([first_draft, second_draft])
    sender_context = SimpleNamespace(
        exchange_client=SimpleNamespace(reply_email=AsyncMock(return_value=True)),
        db_manager=SimpleNamespace(
            get_content_ref=AsyncMock(return_value=_ref()),
            load_draft=AsyncMock(side_effect=lambda draft_id: drafts.values[draft_id]),
            update_status=AsyncMock(),
        ),
        content_store=content_store,
        graph=graph,
        email_processor=SimpleNamespace(
            process_sent_email=MagicMock(),
            update_email_labels=MagicMock(),
        ),
    )

    async def classification_wrapper(_payload):
        return {
            "priority": "P1",
            "need_reply": True,
            "intent": "咨询",
            "summary": "small",
            "reasoning": "small",
            "confidence": 1.0,
        }

    def classification_factory(**_kwargs):
        return lambda _function: classification_wrapper

    with patch("src.nodes.categorizer.get_routing_engine", return_value=router), patch(
        "src.nodes.retriever_node.get_routing_engine", return_value=router
    ), patch(
        "src.nodes.categorizer.with_llm_retry", side_effect=classification_factory
    ), patch(
        "src.nodes.drafter.with_llm_retry", side_effect=draft_retry
    ), patch(
        "src.nodes.reviewer.with_llm_retry", side_effect=_review_retry
    ), patch(
        "src.nodes.retriever_node.get_retriever", return_value=retriever
    ), patch(
        "src.nodes.retriever_node._retrieve_experience",
        new=AsyncMock(return_value=[]),
    ), patch(
        "src.nodes.retriever_node._retrieve_style_guidance",
        new=AsyncMock(return_value=""),
    ), patch(
        "src.nodes.retriever_node._retrieve_user_preferences",
        new=AsyncMock(return_value=[]),
    ), patch(
        "src.providers.factory.get_llm_for_role",
        return_value=RunnableLambda(lambda value: value),
    ), patch(
        "src.nodes.categorizer.enforce_model_input_budget"
    ), patch(
        "src.nodes.drafter.enforce_model_input_budget"
    ), patch(
        "src.nodes.reviewer.enforce_model_input_budget"
    ), patch(
        "src.init_app.get_app_context", return_value=sender_context
    ):
        with ExitStack() as stack:
            stack.enter_context(
                patch(
                    "src.exchange_service.lark_app.generate_and_upload_pdf",
                    new=AsyncMock(return_value=None),
                )
            )
            send_card = stack.enter_context(
                patch(
                    "src.exchange_service.lark_app.send_approval_card",
                    return_value=True,
                )
            )
            stack.enter_context(patch.object(lark_app, "graph", graph))
            stack.enter_context(
                patch.object(lark_app, "graph_dependencies", dependencies)
            )
            stack.enter_context(
                patch.object(lark_app, "db_manager", sender_context.db_manager)
            )
            stack.enter_context(
                patch.object(
                    lark_app,
                    "worker_loop",
                    asyncio.get_running_loop(),
                )
            )
            stack.enter_context(
                patch.object(
                    lark_app,
                    "get_settings",
                    return_value=SimpleNamespace(DEBUG=False),
                )
            )

            pipeline_result = await _run_ai_pipeline(
                email["id"],
                sender_context,
                config,
            )
            assert pipeline_result is not None
            assert pipeline_result["draft"] == second_draft
            second_interrupt = await graph.aget_state(config)
            assert second_interrupt.values["next_step"] == "approval"
            assert second_interrupt.values["metadata"]["review_count"] == 1

            dispatch = await _dispatch_notification(
                email["id"],
                pipeline_result,
                sender_context,
                config,
            )
            assert dispatch == {"delivered": True, "kind": "approval"}
            assert send_card.call_args.kwargs["draft"] == second_draft

            draft_id = await drafts.save_draft(email["id"], human_draft)
            human_update = sanitize_graph_delta(
                second_interrupt.values,
                {
                    "draft_id": draft_id,
                    "approval_status": "modify",
                    "feedback": human_draft,
                },
            )
            assert set(human_update) == {"draft_id", "approval_status"}
            await graph.aupdate_state(config, human_update)

            loop = asyncio.get_running_loop()
            scheduled = []

            def schedule_on_worker(coro):
                future = asyncio.run_coroutine_threadsafe(coro, loop)
                scheduled.append(future)

            with patch.object(
                lark_app,
                "safe_async_run",
                side_effect=schedule_on_worker,
            ):
                await asyncio.to_thread(
                    lark_app.process_approval,
                    email["id"],
                    "approver-1",
                )

            assert len(scheduled) == 1
            await asyncio.wrap_future(scheduled[0])

    sender_context.exchange_client.reply_email.assert_awaited_once_with(
        email_id=email["id"],
        body=human_draft,
        to=["recipient@example.com"],
        cc=[],
    )
    sender_context.db_manager.update_status.assert_any_await(
        email["id"],
        "waiting_approval",
    )
    sender_context.db_manager.update_status.assert_any_await(
        email["id"],
        "approved",
        approver_user_id="approver-1",
        final_draft=human_draft,
    )
    sender_context.email_processor.process_sent_email.assert_called_once()
    sent_email = sender_context.email_processor.process_sent_email.call_args.kwargs
    assert body_marker in sent_email["original_email_data"]["body"]
    assert sent_email["reply_content"] == human_draft
    assert content_store.loads[-1] == (_ref(), False)
    assert [content for _email_id, content in drafts.saves] == [
        first_draft,
        second_draft,
        human_draft,
    ]

    forbidden = [
        body_marker,
        base64_marker,
        "_image_attachments",
        "FIRST-DRAFT-SENTINEL",
        "SECOND-DRAFT-SENTINEL",
        "FULL-FEEDBACK-SENTINEL",
        "FULL-CHUNK-END-SENTINEL",
    ]
    methods = {record[0] for record in saver.records}
    assert methods == {"put", "aput", "put_writes", "aput_writes"}
    for _method, _kind, decoded, typed in saver.records:
        tag, payload = typed
        assert len(tag.encode("utf-8")) + len(payload) < MAX_CHECKPOINT_BYTES
        serialized = tag.encode("utf-8") + payload
        round_tripped = saver.serde.loads_typed(typed)
        leaves = [*_walk_values(decoded), *_walk_values(round_tripped)]
        for marker in forbidden:
            assert marker.encode("utf-8") not in serialized
            assert all(
                marker not in leaf
                if isinstance(leaf, str)
                else marker.encode("utf-8") not in bytes(leaf)
                for leaf in leaves
            )


@pytest.mark.asyncio
async def test_graph_node_errors_are_redacted_before_pending_write_serialization(
    monkeypatch,
):
    monkeypatch.setattr(
        "src.graph.state_factory.get_settings",
        lambda: SimpleNamespace(EXCHANGE_ACCOUNT_ID=8),
    )
    secret = "EXCEPTION-SECRET-SENTINEL-" + "x" * 20_000

    async def explode(_state, dependencies):
        del dependencies
        raise RuntimeError(secret)

    dependencies = GraphDependencies(
        content_store=FakeContentStore({"id": "error-mail"}),
        drafts=FakeDraftStore(),
    )
    saver = RecordingInMemorySaver()
    with patch("src.graph.builder.categorize_email", new=explode):
        graph = build_graph(checkpointer=saver, dependencies=dependencies)

    with pytest.raises(RuntimeError):
        await graph.ainvoke(
            build_initial_graph_state({"id": "error-mail"}, _ref()),
            config={"configurable": {"thread_id": "error-mail"}},
        )

    assert saver.records
    for _method, _kind, decoded, typed in saver.records:
        tag, payload = typed
        assert len(tag.encode("utf-8")) + len(payload) < MAX_CHECKPOINT_BYTES
        assert secret.encode("utf-8") not in tag.encode("utf-8") + payload
        assert all(secret not in leaf for leaf in _walk_values(decoded) if isinstance(leaf, str))
