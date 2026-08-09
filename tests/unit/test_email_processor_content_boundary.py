import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, StateGraph

from src.domain.email_state import InitialEmailWriteResult, ProcessingOutcome
from src.domain.errors import DatabaseOperationError
from src.exchange_service import (
    _ensure_durable_content_ref,
    _run_ai_pipeline,
    process_and_archive_email,
)
from src.graph.state_factory import build_initial_graph_state
from src.graph.state import AgentState
from src.safety.input_limits import InputLimitExceeded
from src.storage import ContentRef, ContentStoreReferenceError
from src.utils.email_processor import EmailProcessor


def _ref() -> ContentRef:
    return ContentRef(
        account_id=8,
        object_id="00000000-0000-4000-8000-000000000017",
        key_version="v1",
        sha256="1" * 64,
    )


def _ctx(order: list[str], *, initial=InitialEmailWriteResult.CREATED):
    ctx = SimpleNamespace()
    ctx.db_manager = SimpleNamespace(
        log_initial_email=AsyncMock(side_effect=lambda _email: order.append("db-log") or initial),
        get_email_status=AsyncMock(return_value=None),
        get_content_ref=AsyncMock(return_value=None),
        set_content_ref=AsyncMock(side_effect=lambda _id, _ref: order.append("db-ref")),
        set_content_ref_if_absent=AsyncMock(
            side_effect=lambda _id, _ref: order.append("db-ref") or True
        ),
        update_status=AsyncMock(),
        load_draft=AsyncMock(),
    )
    ctx.content_store = SimpleNamespace(
        put_email=AsyncMock(side_effect=lambda *_args: order.append("put") or _ref()),
        load_email=AsyncMock(),
        delete=AsyncMock(side_effect=lambda _ref: order.append("delete")),
    )
    ctx.email_processor = MagicMock()
    ctx.exchange_client = AsyncMock()
    ctx.graph = AsyncMock()
    return ctx


def _settings():
    return SimpleNamespace(EXCHANGE_ACCOUNT_ID=8)


def _compiled_cleanup_graph():
    workflow = StateGraph(AgentState)
    workflow.add_node("categorizer", lambda _state: {})
    workflow.set_entry_point("categorizer")
    workflow.add_edge("categorizer", END)
    return workflow.compile(checkpointer=InMemorySaver())


def _stateful_graph(values: dict | None = None):
    state_values = values if values is not None else {}
    state = SimpleNamespace(values=state_values, next=())

    async def update_state(_config, delta, **kwargs):
        state_values.update(deepcopy(delta))
        if kwargs.get("as_node") == "__start__":
            state.next = ("categorizer",)

    return SimpleNamespace(
        aget_state=AsyncMock(return_value=state),
        aupdate_state=AsyncMock(side_effect=update_state),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("skip_analysis", [False, True], ids=["full", "archive"])
async def test_new_email_persists_content_ref_before_any_downstream_work(skip_analysis):
    order: list[str] = []
    ctx = _ctx(order)

    async def downstream(*_args, **_kwargs):
        order.append("downstream")
        return ProcessingOutcome.PROCESSED

    target = "src.exchange_service._archive_only" if skip_analysis else "src.exchange_service._run_ai_path"
    with patch("src.exchange_service.get_settings", return_value=_settings()), patch(
        target, new=AsyncMock(side_effect=downstream)
    ):
        outcome = await process_and_archive_email(
            {"id": "mail-1", "subject": "s", "sender": "a@example.com"},
            ctx,
            skip_analysis=skip_analysis,
        )

    assert outcome is (
        ProcessingOutcome.ARCHIVED if skip_analysis else ProcessingOutcome.PROCESSED
    )
    assert order == ["db-log", "put", "db-ref", "downstream"]
    ctx.content_store.put_email.assert_awaited_once()
    ctx.db_manager.set_content_ref_if_absent.assert_awaited_once_with(
        "mail-1",
        _ref(),
    )


@pytest.mark.asyncio
async def test_normal_duplicate_returns_without_content_object_or_downstream_work():
    order: list[str] = []
    ctx = _ctx(order, initial=InitialEmailWriteResult.DUPLICATE)

    with patch("src.exchange_service.get_settings", return_value=_settings()), patch(
        "src.exchange_service._run_ai_path", new_callable=AsyncMock
    ) as downstream:
        outcome = await process_and_archive_email({"id": "mail-1"}, ctx)

    assert outcome is ProcessingOutcome.DUPLICATE
    assert order == ["db-log"]
    ctx.db_manager.get_content_ref.assert_not_awaited()
    ctx.content_store.put_email.assert_not_awaited()
    downstream.assert_not_awaited()


@pytest.mark.asyncio
async def test_invalid_base64_is_rejected_before_database_or_content_writes():
    order: list[str] = []
    ctx = _ctx(order)

    with pytest.raises(InputLimitExceeded) as caught:
        await process_and_archive_email(
            {
                "id": "mail-invalid-base64",
                "attachments": [{"name": "bad.bin", "content": "!!!!"}],
            },
            ctx,
        )

    assert caught.value.category == "attachment_format"
    assert order == []
    ctx.db_manager.log_initial_email.assert_not_awaited()
    ctx.content_store.put_email.assert_not_awaited()
    ctx.graph.astream.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("email", "category"),
    [
        ({"id": "x" * 513}, "invalid_email_id"),
        (
            {"id": "mail-oversized-sender", "sender": "s" * 321},
            "invalid_draft_to",
        ),
        (
            {"id": "mail-oversized-cc", "draft_cc": ["c" * 321]},
            "invalid_draft_cc",
        ),
    ],
)
async def test_invalid_graph_identifiers_are_rejected_before_any_write(
    email,
    category,
):
    order: list[str] = []
    ctx = _ctx(order)

    with pytest.raises(InputLimitExceeded) as caught:
        await process_and_archive_email(email, ctx)

    assert caught.value.category == category
    assert order == []
    ctx.db_manager.log_initial_email.assert_not_awaited()
    ctx.content_store.put_email.assert_not_awaited()
    ctx.db_manager.set_content_ref_if_absent.assert_not_awaited()
    ctx.graph.astream.assert_not_called()


@pytest.mark.asyncio
async def test_many_initial_recipients_remain_valid_and_are_capped_at_graph_boundary():
    order: list[str] = []
    ctx = _ctx(order)
    email = {
        "id": "mail-many-recipients",
        "draft_to": [f"user-{index}@example.com" for index in range(11)],
    }

    with patch("src.exchange_service.get_settings", return_value=_settings()), patch(
        "src.exchange_service._run_ai_path",
        new_callable=AsyncMock,
    ) as downstream:
        downstream.return_value = ProcessingOutcome.PROCESSED
        outcome = await process_and_archive_email(email, ctx)

    assert outcome is ProcessingOutcome.PROCESSED
    assert order == ["db-log", "put", "db-ref"]
    downstream.assert_awaited_once()
    assert build_initial_graph_state(email, _ref())["draft_to"] == email["draft_to"][:10]


@pytest.mark.asyncio
async def test_force_retry_reuses_existing_database_ref_without_new_object():
    order: list[str] = []
    ctx = _ctx(order, initial=InitialEmailWriteResult.DUPLICATE)

    async def get_ref(_email_id):
        order.append("get-ref")
        return _ref()

    async def downstream(*_args, **_kwargs):
        order.append("downstream")
        return ProcessingOutcome.PROCESSED

    ctx.db_manager.get_content_ref.side_effect = get_ref
    with patch("src.exchange_service.get_settings", return_value=_settings()), patch(
        "src.exchange_service._run_ai_path",
        new=AsyncMock(side_effect=downstream),
    ):
        outcome = await process_and_archive_email(
            {"id": "mail-1"},
            ctx,
            force_reprocess=True,
        )

    assert outcome is ProcessingOutcome.PROCESSED
    assert order == ["db-log", "get-ref", "downstream"]
    ctx.content_store.put_email.assert_not_awaited()
    ctx.db_manager.set_content_ref_if_absent.assert_not_awaited()


@pytest.mark.asyncio
async def test_ref_write_failure_deletes_new_object_and_propagates_before_downstream():
    order: list[str] = []
    ctx = _ctx(order)
    failure = DatabaseOperationError(
        operation="set_content_ref",
        retryable=True,
        message="safe ref write failure",
    )

    async def fail_ref(_email_id, _ref_value):
        order.append("db-ref")
        raise failure

    ctx.db_manager.set_content_ref_if_absent.side_effect = fail_ref
    with patch("src.exchange_service.get_settings", return_value=_settings()), patch(
        "src.exchange_service._run_ai_path", new_callable=AsyncMock
    ) as downstream, pytest.raises(DatabaseOperationError) as caught:
        await process_and_archive_email({"id": "mail-1"}, ctx)

    assert caught.value is failure
    assert order == ["db-log", "put", "db-ref", "delete"]
    ctx.content_store.delete.assert_awaited_once_with(_ref())
    downstream.assert_not_awaited()


@pytest.mark.asyncio
async def test_ref_write_commit_then_raise_is_confirmed_before_any_delete():
    order: list[str] = []
    ctx = _ctx(order)
    failure = DatabaseOperationError(
        operation="set_content_ref",
        retryable=True,
        message="ambiguous commit",
    )

    async def commit_then_raise(_email_id, _ref_value):
        order.append("db-ref")
        ctx.db_manager.get_content_ref.return_value = _ref()
        raise failure

    ctx.db_manager.set_content_ref_if_absent.side_effect = commit_then_raise
    with patch("src.exchange_service.get_settings", return_value=_settings()):
        result = await _ensure_durable_content_ref(
            "mail-1",
            {"id": "mail-1"},
            ctx,
            reuse_existing=False,
        )

    assert result == _ref()
    ctx.content_store.delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_ref_write_failure_with_different_winner_deletes_only_candidate():
    order: list[str] = []
    ctx = _ctx(order)
    winner = ContentRef(
        account_id=8,
        object_id="00000000-0000-4000-8000-000000000019",
        key_version="v1",
        sha256="3" * 64,
    )
    ctx.db_manager.set_content_ref_if_absent.side_effect = DatabaseOperationError(
        operation="set_content_ref_if_absent",
        retryable=True,
        message="ambiguous claim",
    )
    ctx.db_manager.get_content_ref.return_value = winner

    with patch("src.exchange_service.get_settings", return_value=_settings()):
        result = await _ensure_durable_content_ref(
            "mail-1",
            {"id": "mail-1"},
            ctx,
            reuse_existing=False,
        )

    assert result == winner
    ctx.content_store.delete.assert_awaited_once_with(_ref())


@pytest.mark.parametrize("claim_result", ["false", "error"])
@pytest.mark.asyncio
async def test_foreign_account_ref_winner_still_deletes_unclaimed_candidate(
    claim_result,
):
    order: list[str] = []
    ctx = _ctx(order)
    foreign_winner = ContentRef(
        account_id=9,
        object_id="00000000-0000-4000-8000-000000000039",
        key_version="v1",
        sha256="f" * 64,
    )
    if claim_result == "false":
        ctx.db_manager.set_content_ref_if_absent.side_effect = None
        ctx.db_manager.set_content_ref_if_absent.return_value = False
    else:
        ctx.db_manager.set_content_ref_if_absent.side_effect = DatabaseOperationError(
            operation="set_content_ref_if_absent",
            retryable=True,
            message="ambiguous claim",
        )
    ctx.db_manager.get_content_ref.return_value = foreign_winner

    with patch("src.exchange_service.get_settings", return_value=_settings()), pytest.raises(
        ContentStoreReferenceError,
        match="content_account_mismatch",
    ):
        await _ensure_durable_content_ref(
            "mail-1",
            {"id": "mail-1"},
            ctx,
            reuse_existing=False,
        )

    ctx.content_store.delete.assert_awaited_once_with(_ref())


@pytest.mark.asyncio
async def test_ref_write_failure_with_uncertain_readback_never_deletes_candidate(caplog):
    order: list[str] = []
    ctx = _ctx(order)
    write_failure = RuntimeError("PRIVATE-WRITE-DETAIL")
    ctx.db_manager.set_content_ref_if_absent.side_effect = write_failure
    ctx.db_manager.get_content_ref.side_effect = RuntimeError("PRIVATE-READ-DETAIL")

    with patch("src.exchange_service.get_settings", return_value=_settings()), pytest.raises(
        RuntimeError
    ) as caught:
        await _ensure_durable_content_ref(
            "mail-1",
            {"id": "mail-1"},
            ctx,
            reuse_existing=False,
        )

    assert caught.value is write_failure
    assert caught.value.__suppress_context__ is True
    ctx.content_store.delete.assert_not_awaited()
    assert "PRIVATE-WRITE-DETAIL" not in caplog.text
    assert "PRIVATE-READ-DETAIL" not in caplog.text


@pytest.mark.parametrize(
    ("readback", "expect_delete"),
    [
        ("missing", True),
        ("committed", False),
        ("winner", True),
        ("foreign", True),
        ("unknown", False),
    ],
)
@pytest.mark.asyncio
async def test_cancelled_ref_claim_reconciles_candidate_before_propagating(
    readback,
    expect_delete,
    caplog,
):
    order: list[str] = []
    ctx = _ctx(order)
    cancellation = asyncio.CancelledError()
    ctx.db_manager.set_content_ref_if_absent.side_effect = cancellation
    if readback == "missing":
        ctx.db_manager.get_content_ref.return_value = None
    elif readback == "committed":
        ctx.db_manager.get_content_ref.return_value = _ref()
    elif readback in {"winner", "foreign"}:
        ctx.db_manager.get_content_ref.return_value = ContentRef(
            account_id=9 if readback == "foreign" else 8,
            object_id="00000000-0000-4000-8000-000000000029",
            key_version="v1",
            sha256="9" * 64,
        )
    else:
        ctx.db_manager.get_content_ref.side_effect = RuntimeError(
            "PRIVATE-CANCEL-READBACK-DETAIL"
        )

    with patch("src.exchange_service.get_settings", return_value=_settings()), pytest.raises(
        asyncio.CancelledError
    ) as caught:
        await _ensure_durable_content_ref(
            "mail-1",
            {"id": "mail-1"},
            ctx,
            reuse_existing=False,
        )

    assert caught.value is cancellation
    ctx.db_manager.get_content_ref.assert_awaited_once_with("mail-1")
    if expect_delete:
        ctx.content_store.delete.assert_awaited_once_with(_ref())
    else:
        ctx.content_store.delete.assert_not_awaited()
    assert "PRIVATE-CANCEL-READBACK-DETAIL" not in caplog.text


@pytest.mark.asyncio
async def test_false_ref_claim_cancelled_readback_deletes_known_unclaimed_candidate():
    order: list[str] = []
    ctx = _ctx(order)
    cancellation = asyncio.CancelledError()
    ctx.db_manager.set_content_ref_if_absent.side_effect = None
    ctx.db_manager.set_content_ref_if_absent.return_value = False
    ctx.db_manager.get_content_ref.side_effect = cancellation

    with patch("src.exchange_service.get_settings", return_value=_settings()), pytest.raises(
        asyncio.CancelledError
    ) as caught:
        await _ensure_durable_content_ref(
            "mail-1",
            {"id": "mail-1"},
            ctx,
            reuse_existing=False,
        )

    assert caught.value is cancellation
    ctx.content_store.delete.assert_awaited_once_with(_ref())


@pytest.mark.asyncio
async def test_false_ref_claim_with_no_persisted_winner_deletes_candidate():
    order: list[str] = []
    ctx = _ctx(order)
    ctx.db_manager.set_content_ref_if_absent.side_effect = None
    ctx.db_manager.set_content_ref_if_absent.return_value = False
    ctx.db_manager.get_content_ref.return_value = None

    with patch("src.exchange_service.get_settings", return_value=_settings()), pytest.raises(
        DatabaseOperationError
    ) as caught:
        await _ensure_durable_content_ref(
            "mail-1",
            {"id": "mail-1"},
            ctx,
            reuse_existing=False,
        )

    assert caught.value.operation == "set_content_ref_if_absent"
    ctx.content_store.delete.assert_awaited_once_with(_ref())


@pytest.mark.parametrize(
    "delete_failure",
    [None, RuntimeError("PRIVATE-DELETE-DETAIL")],
)
@pytest.mark.asyncio
async def test_false_ref_claim_readback_failure_deletes_unclaimed_candidate(
    delete_failure,
    caplog,
):
    order: list[str] = []
    ctx = _ctx(order)
    read_failure = RuntimeError("PRIVATE-READ-DETAIL")
    ctx.db_manager.set_content_ref_if_absent.side_effect = None
    ctx.db_manager.set_content_ref_if_absent.return_value = False
    ctx.db_manager.get_content_ref.side_effect = read_failure
    if delete_failure is not None:
        ctx.content_store.delete.side_effect = delete_failure

    with patch("src.exchange_service.get_settings", return_value=_settings()), pytest.raises(
        RuntimeError
    ) as caught:
        await _ensure_durable_content_ref(
            "mail-1",
            {"id": "mail-1"},
            ctx,
            reuse_existing=False,
        )

    assert caught.value is read_failure
    assert caught.value.__suppress_context__ is True
    ctx.content_store.delete.assert_awaited_once_with(_ref())
    assert "PRIVATE-READ-DETAIL" not in caplog.text
    assert "PRIVATE-DELETE-DETAIL" not in caplog.text


@pytest.mark.asyncio
async def test_ref_delete_failure_never_rebinds_an_ambiguous_object():
    order: list[str] = []
    ctx = _ctx(order)
    failure = DatabaseOperationError(
        operation="set_content_ref",
        retryable=True,
        message="write failed",
    )
    ctx.db_manager.set_content_ref_if_absent.side_effect = failure
    ctx.db_manager.get_content_ref.return_value = None
    ctx.content_store.delete.side_effect = RuntimeError("delete failed")

    with patch("src.exchange_service.get_settings", return_value=_settings()), pytest.raises(
        DatabaseOperationError
    ) as caught:
        await _ensure_durable_content_ref(
            "mail-1",
            {"id": "mail-1"},
            ctx,
            reuse_existing=False,
        )

    assert caught.value is failure
    assert ctx.db_manager.set_content_ref_if_absent.await_count == 1
    ctx.content_store.delete.assert_awaited_once_with(_ref())


@pytest.mark.asyncio
async def test_concurrent_force_retries_claim_one_ref_and_delete_loser():
    refs = [
        _ref(),
        ContentRef(
            account_id=8,
            object_id="00000000-0000-4000-8000-000000000018",
            key_version="v1",
            sha256="2" * 64,
        ),
    ]
    winner: ContentRef | None = None
    lock = asyncio.Lock()
    initial_reads = 0
    both_read = asyncio.Event()

    async def get_ref(_email_id):
        nonlocal initial_reads
        if winner is None and initial_reads < 2:
            observed = winner
            initial_reads += 1
            if initial_reads == 2:
                both_read.set()
            await both_read.wait()
            return observed
        return winner

    async def claim(_email_id, candidate):
        nonlocal winner
        async with lock:
            if winner is None:
                winner = candidate
                return True
            return False

    content_store = SimpleNamespace(
        put_email=AsyncMock(side_effect=refs),
        delete=AsyncMock(),
    )
    ctx = SimpleNamespace(
        db_manager=SimpleNamespace(
            get_content_ref=AsyncMock(side_effect=get_ref),
            set_content_ref_if_absent=AsyncMock(side_effect=claim),
        ),
        content_store=content_store,
    )

    with patch("src.exchange_service.get_settings", return_value=_settings()):
        first, second = await asyncio.gather(
            _ensure_durable_content_ref(
                "mail-1",
                {"id": "mail-1"},
                ctx,
                reuse_existing=True,
            ),
            _ensure_durable_content_ref(
                "mail-1",
                {"id": "mail-1"},
                ctx,
                reuse_existing=True,
            ),
        )

    assert winner is not None
    assert first == second == winner
    loser = refs[1] if winner == refs[0] else refs[0]
    content_store.delete.assert_awaited_once_with(loser)


@pytest.mark.asyncio
async def test_pipeline_restart_builds_slim_state_from_database_ref():
    order: list[str] = []
    ctx = _ctx(order)
    ctx.db_manager.get_content_ref.return_value = _ref()
    email = {
        "id": "mail-1",
        "subject": "subject",
        "sender": "a@example.com",
        "body": "BODY-MUST-STAY-LOCAL",
        "attachments": [{"name": "a.txt"}],
    }
    ctx.content_store.load_email.return_value = email
    captured = {}

    async def stream(initial_state, *, config):
        captured["state"] = initial_state
        captured["config"] = config
        if False:
            yield None

    ctx.graph.astream = stream
    final_state = MagicMock()
    final_state.values = {
        **MagicMock(),
        "classification": {"need_reply": False},
        "draft_id": None,
        "context_summaries": [],
        "routing_log": [],
    }
    ctx.graph.aget_state.return_value = final_state

    with patch("src.exchange_service.get_settings", return_value=_settings()):
        result = await _run_ai_pipeline(
            "mail-1",
            ctx,
            {"configurable": {"thread_id": "mail-1"}},
            attachment_tokens=["file-token-1"],
            preserved_attachment_tokens=["old-file-token"],
            preserved_pdf_token="old-pdf-token",
        )

    assert captured["state"]["email_id"] == "mail-1"
    assert "body" not in captured["state"]["email"]
    assert "attachments" not in captured["state"]["email"]
    assert captured["state"]["attachment_tokens"] == [
        "old-file-token",
        "file-token-1",
    ]
    assert captured["state"]["pdf_token"] == "old-pdf-token"
    assert result["email"]["body"] == "BODY-MUST-STAY-LOCAL"
    assert "lark_file_url" not in result["email"]["attachments"][0]
    ctx.db_manager.get_content_ref.assert_awaited_once_with("mail-1")


@pytest.mark.asyncio
@pytest.mark.parametrize("malformed_draft_id", ["", 0, False])
async def test_pipeline_rejects_non_none_malformed_draft_id_without_loading_it(
    malformed_draft_id,
):
    ctx = _ctx([])
    ctx.db_manager.get_content_ref.return_value = _ref()
    ctx.content_store.load_email.return_value = {
        "id": "mail-1",
        "subject": "subject",
        "sender": "a@example.com",
    }

    async def stream(_initial_state, *, config):
        if False:
            yield config

    ctx.graph.astream = stream
    ctx.graph.aget_state.return_value = SimpleNamespace(
        values={
            "email_id": "mail-1",
            "classification": {"need_reply": False},
            "draft_id": malformed_draft_id,
            "context_summaries": [],
            "routing_log": [],
            }
    )

    with patch("src.exchange_service.get_settings", return_value=_settings()):
        result = await _run_ai_pipeline(
            "mail-1",
            ctx,
            {"configurable": {"thread_id": "mail-1"}},
        )

    assert result is None
    ctx.db_manager.load_draft.assert_not_awaited()


def test_email_processor_never_creates_a_second_image_base64_copy():
    processor = EmailProcessor.__new__(EmailProcessor)
    processor.collection_name = "emails"
    processor.init_collection = MagicMock()
    captured = {}

    def split_text(value):
        captured["indexed_text"] = value
        return ["small chunk"]

    processor.text_splitter = SimpleNamespace(split_text=split_text)
    processor.openai_client = SimpleNamespace(
        embeddings=SimpleNamespace(
            create=lambda **_kwargs: SimpleNamespace(
                data=[SimpleNamespace(embedding=[0.1, 0.2])]
            )
        )
    )
    processor.embedding_model = "test"
    processor.qdrant_client = MagicMock()
    email = {
        "id": "mail-image",
        "subject": "image",
        "body": (
            "<p>VISIBLE-BODY-SENTINEL</p>"
            '<img src="data:image/png;base64,BASE64-IMAGE-SENTINEL">'
        ),
        "_image_attachments": [
            {
                "content": "LEGACY-BASE64-IMAGE-SENTINEL",
                "content_type": "image/png",
            }
        ],
        "attachments": [
            {
                "name": "image.png",
                "content_type": "image/png",
                "size": 3,
                "content": "BASE64-IMAGE-SENTINEL",
            }
        ],
    }
    before = deepcopy(email)

    assert processor.process_batch([email], wait=True) == 1

    assert email == before
    point = processor.qdrant_client.upsert.call_args.kwargs["points"][0]
    assert processor.qdrant_client.upsert.call_args.kwargs["wait"] is True
    assert "VISIBLE-BODY-SENTINEL" in captured["indexed_text"]
    assert "[内嵌图片]" in captured["indexed_text"]
    assert "data:image" not in captured["indexed_text"]
    assert point.payload["body"] == "VISIBLE-BODY-SENTINEL\n[内嵌图片]"
    assert "_image_attachments" not in point.payload
    assert "BASE64-IMAGE-SENTINEL" not in str(point.payload)
    assert "LEGACY-BASE64-IMAGE-SENTINEL" not in str(point.payload)


def test_email_processor_indexes_reply_delta_without_reembedding_quoted_history():
    processor = EmailProcessor.__new__(EmailProcessor)
    processor.collection_name = "emails"
    processor.init_collection = MagicMock()
    captured = {}

    def split_text(value):
        captured["indexed_text"] = value
        return ["small chunk"]

    processor.text_splitter = SimpleNamespace(split_text=split_text)
    processor.openai_client = SimpleNamespace(
        embeddings=SimpleNamespace(
            create=lambda **_kwargs: SimpleNamespace(
                data=[SimpleNamespace(embedding=[0.1, 0.2])]
            )
        )
    )
    processor.embedding_model = "test"
    processor.qdrant_client = MagicMock()
    email = {
        "id": "mail-evolution",
        "subject": "答复: 项目材料",
        "conversation_id": "conversation-1",
        "unique_body": "<p>本轮增量：材料已完成，请查收。</p>",
        "uniqueBody": (
            "<img src=\"data:image/png;base64,UNIQUE-BODY-RAW-SENTINEL\">"
        ),
        "body": """
            <p>完整正文中的旧请求：请重新编制预算。</p>
            <div class="gmail_quote">
              <p>旧任务：请在今天前修改材料并回复。</p>
            </div>
        """,
        "attachments": [],
    }
    before = deepcopy(email)

    assert processor.process_batch([email]) == 1

    assert email == before
    assert "本轮增量：材料已完成，请查收。" in captured["indexed_text"]
    assert "完整正文中的旧请求：请重新编制预算。" not in captured["indexed_text"]
    assert "旧任务：请在今天前修改材料并回复。" not in captured["indexed_text"]
    point = processor.qdrant_client.upsert.call_args.kwargs["points"][0]
    assert point.payload["body"] == "本轮增量：材料已完成，请查收。"
    assert point.payload["thread_id"] == "conversation-1"
    assert point.payload["has_quoted_history"] is False
    assert "unique_body" not in point.payload
    assert "uniqueBody" not in point.payload
    assert "UNIQUE-BODY-RAW-SENTINEL" not in str(point.payload)
