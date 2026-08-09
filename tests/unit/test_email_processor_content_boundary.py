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
    CleanupHandleSnapshot,
    _cleanup_graph_drive_files,
    _ensure_durable_content_ref,
    _run_ai_path,
    _run_ai_pipeline,
    _upload_attachments_to_lark,
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
            attachment_links=[
                {
                    "name": "a.txt",
                    "lark_file_url": "https://example.invalid/file",
                }
            ],
        )

    assert captured["state"]["email_id"] == "mail-1"
    assert "body" not in captured["state"]["email"]
    assert "attachments" not in captured["state"]["email"]
    assert captured["state"]["attachment_tokens"] == [
        "old-file-token",
        "file-token-1",
    ]
    assert captured["state"]["pdf_token"] == "old-pdf-token"
    assert "example.invalid" not in str(captured["state"])
    assert result["email"]["body"] == "BODY-MUST-STAY-LOCAL"
    assert result["email"]["attachments"][0]["lark_file_url"] == (
        "https://example.invalid/file"
    )
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


@pytest.mark.asyncio
async def test_attachment_upload_returns_only_tokens_without_mutating_email():
    email = {
        "attachments": [
            {
                "name": "inline-logo.png",
                "content": "aW1hZ2U=",
                "content_type": "image/png",
                "content_id": "inline-logo.png",
                "is_inline": True,
            },
            {"name": "small.txt", "content": "c21hbGw="},
        ]
    }
    before = deepcopy(email)

    with patch(
        "src.exchange_service.lark_app.upload_file_to_drive",
        return_value={"file_token": "file-token-1", "url": "https://example.invalid/file"},
    ) as upload:
        uploads = await _upload_attachments_to_lark(email)

    assert uploads.tokens == ("file-token-1",)
    assert uploads.links == (
        {
            "name": "small.txt",
            "lark_file_url": "https://example.invalid/file",
        },
    )
    upload.assert_called_once_with("small.txt", b"small", 5)
    assert email == before


@pytest.mark.asyncio
async def test_non_inline_pdf_with_content_id_is_uploaded_as_business_attachment():
    email = {
        "body": "Please review the attached reports.",
        "attachments": [
            {
                "name": "report.pdf",
                "content": "JVBERi0xLjcK",
                "content_type": "application/pdf",
                "content_id": "normal-attachment-id",
                "is_inline": False,
            }
        ],
    }

    with patch(
        "src.exchange_service.lark_app.upload_file_to_drive",
        return_value={
            "file_token": "pdf-token",
            "url": "https://example.invalid/report",
        },
    ) as upload:
        uploads = await _upload_attachments_to_lark(email)

    assert uploads.tokens == ("pdf-token",)
    assert uploads.links == (
        {
            "name": "report.pdf",
            "lark_file_url": "https://example.invalid/report",
        },
    )
    upload.assert_called_once_with("report.pdf", b"%PDF-1.7\n", 9)


@pytest.mark.asyncio
async def test_attachment_upload_respects_remaining_cleanup_handle_capacity():
    email = {
        "attachments": [
            {"name": "first.txt", "content": "MQ=="},
            {"name": "second.txt", "content": "Mg=="},
        ]
    }

    with patch(
        "src.exchange_service.lark_app.upload_file_to_drive",
        side_effect=[
            {"file_token": "first-token", "url": "https://example.invalid/first"},
            {"file_token": "second-token", "url": "https://example.invalid/second"},
        ],
    ) as upload:
        uploads = await _upload_attachments_to_lark(email, max_uploads=1)

    assert uploads.tokens == ("first-token",)
    upload.assert_called_once()


@pytest.mark.asyncio
async def test_ai_path_uploads_and_decorates_business_attachments_after_ai():
    ctx = SimpleNamespace(
        db_manager=SimpleNamespace(
            get_content_ref=AsyncMock(return_value=_ref()),
            update_status=AsyncMock(),
        ),
        exchange_client=AsyncMock(),
        graph=_stateful_graph(),
    )
    config = {"configurable": {"thread_id": "mail-1"}}
    events = []
    pipeline_result = {
        "classification": {"need_reply": True},
        "email": {
            "id": "mail-1",
            "attachments": [{"name": "small.txt"}],
        },
    }

    async def run_pipeline(*_args, **_kwargs):
        events.append("pipeline")
        return pipeline_result

    async def upload(*_args, **_kwargs):
        events.append("upload")
        return SimpleNamespace(
            tokens=("file-token-1",),
            links=(
                {
                    "name": "small.txt",
                    "lark_file_url": "https://example.invalid/file",
                },
            ),
        )

    async def dispatch(_email_id, result, *_args, **_kwargs):
        events.append("dispatch")
        assert result["email"]["attachments"][0]["lark_file_url"] == (
            "https://example.invalid/file"
        )
        return {"delivered": True, "kind": "approval"}

    with patch("src.exchange_service.get_settings", return_value=_settings()), patch(
        "src.exchange_service._upload_attachments_to_lark",
        new=AsyncMock(side_effect=upload),
    ) as upload_mock, patch(
        "src.exchange_service._ingest_to_qdrant",
        new_callable=AsyncMock,
    ), patch(
        "src.exchange_service._run_ai_pipeline",
        new=AsyncMock(side_effect=run_pipeline),
    ) as pipeline, patch(
        "src.exchange_service._retain_cleanup_token",
        new=AsyncMock(return_value=True),
    ) as retain, patch(
        "src.exchange_service._dispatch_notification",
        new=AsyncMock(side_effect=dispatch),
    ), patch(
        "src.exchange_service._mark_email_read",
        new_callable=AsyncMock,
    ):
        outcome = await _run_ai_path(
            "mail-1",
            {
                "id": "mail-1",
                "attachments": [{"name": "small.txt", "content": "c21hbGw="}],
            },
            ctx,
            config,
        )

    assert outcome is ProcessingOutcome.PROCESSED
    assert events == ["pipeline", "upload", "dispatch"]
    upload_mock.assert_awaited_once()
    retain.assert_awaited_once_with("mail-1", ctx, "file-token-1")
    pipeline.assert_awaited_once_with(
        "mail-1",
        ctx,
        config,
        attachment_tokens=[],
        preserved_attachment_tokens=[],
        preserved_pdf_token=None,
        attachment_links=[],
    )


@pytest.mark.asyncio
async def test_ai_path_sends_manual_review_card_without_marking_exchange_read():
    """A fail-closed graph result must surface the email without consuming it."""
    ctx = SimpleNamespace(
        db_manager=SimpleNamespace(
            get_content_ref=AsyncMock(return_value=_ref()),
            compare_and_set_manual_review=AsyncMock(return_value=True),
            get_email_status=AsyncMock(return_value="manual_review"),
            update_status=AsyncMock(),
        ),
        exchange_client=AsyncMock(),
        graph=_stateful_graph(),
    )
    pipeline_result = {
        "next_step": "manual_review",
        "approval_status": "manual_review",
        "safe_error_summary": "content_guard_rejected",
        "classification": {"priority": "P1"},
        "email": {
            "id": "mail-manual-review",
            "subject": "需人工处理",
            "sender": "sender@example.test",
            "body": "请在7月30日前完成。",
        },
    }
    manual_card = AsyncMock(return_value={"delivered": True, "pdf_token": None})

    with patch("src.exchange_service.get_settings", return_value=_settings()), patch(
        "src.exchange_service._snapshot_cleanup_handles",
        new=AsyncMock(return_value=CleanupHandleSnapshot()),
    ), patch(
        "src.exchange_service._checkpoint_ai_path_resources",
        new=AsyncMock(return_value=CleanupHandleSnapshot()),
    ), patch(
        "src.exchange_service._ingest_to_qdrant",
        new_callable=AsyncMock,
    ), patch(
        "src.exchange_service._run_ai_pipeline",
        new=AsyncMock(return_value=pipeline_result),
    ), patch(
        "src.exchange_service._dispatch_manual_review_notification",
        new=manual_card,
    ), patch(
        "src.exchange_service._cleanup_graph_drive_files",
        new_callable=AsyncMock,
    ), patch(
        "src.exchange_service._mark_email_read",
        new_callable=AsyncMock,
    ) as mark_read:
        outcome = await _run_ai_path(
            "mail-manual-review",
            {"id": "mail-manual-review"},
            ctx,
            {"configurable": {"thread_id": "mail-manual-review"}},
        )

    assert outcome is ProcessingOutcome.MANUAL_REVIEW
    manual_card.assert_awaited_once_with(
        "mail-manual-review",
        pipeline_result,
        ctx,
        _effect_boundary=None,
    )
    ctx.db_manager.compare_and_set_manual_review.assert_awaited_once()
    assert (
        ctx.db_manager.compare_and_set_manual_review.await_args.kwargs["error_code"]
        == "content_guard_rejected"
    )
    mark_read.assert_not_awaited()


@pytest.mark.asyncio
async def test_ai_path_keeps_manual_review_email_unread_when_card_delivery_fails():
    ctx = SimpleNamespace(
        db_manager=SimpleNamespace(
            get_content_ref=AsyncMock(return_value=_ref()),
            compare_and_set_manual_review=AsyncMock(return_value=True),
            update_status=AsyncMock(),
        ),
        exchange_client=AsyncMock(),
        graph=_stateful_graph(),
    )
    pipeline_result = {
        "next_step": "manual_review",
        "safe_error_summary": "content_guard_rejected",
        "email": {"id": "mail-manual-card-failed", "body": "需要人工处理"},
    }
    manual_card = AsyncMock(return_value={"delivered": False, "pdf_token": None})

    with patch("src.exchange_service.get_settings", return_value=_settings()), patch(
        "src.exchange_service._snapshot_cleanup_handles",
        new=AsyncMock(return_value=CleanupHandleSnapshot()),
    ), patch(
        "src.exchange_service._checkpoint_ai_path_resources",
        new=AsyncMock(return_value=CleanupHandleSnapshot()),
    ), patch(
        "src.exchange_service._ingest_to_qdrant",
        new_callable=AsyncMock,
    ), patch(
        "src.exchange_service._run_ai_pipeline",
        new=AsyncMock(return_value=pipeline_result),
    ), patch(
        "src.exchange_service._dispatch_manual_review_notification",
        new=manual_card,
    ), patch(
        "src.exchange_service._cleanup_graph_drive_files",
        new_callable=AsyncMock,
    ) as cleanup, patch(
        "src.exchange_service._mark_email_read",
        new_callable=AsyncMock,
    ) as mark_read:
        outcome = await _run_ai_path(
            "mail-manual-card-failed",
            {"id": "mail-manual-card-failed"},
            ctx,
            {"configurable": {"thread_id": "mail-manual-card-failed"}},
        )

    assert outcome is ProcessingOutcome.FAILED
    manual_card.assert_awaited_once_with(
        "mail-manual-card-failed",
        pipeline_result,
        ctx,
        _effect_boundary=None,
    )
    ctx.db_manager.compare_and_set_manual_review.assert_not_awaited()
    ctx.db_manager.update_status.assert_awaited_once_with(
        "mail-manual-card-failed",
        "delivery_failed",
        error_message="manual_review_card_delivery_failed",
    )
    cleanup.assert_awaited_once()
    mark_read.assert_not_awaited()


@pytest.mark.asyncio
async def test_ai_path_does_not_upload_when_ingest_fails():
    ctx = SimpleNamespace(
        db_manager=SimpleNamespace(
            get_content_ref=AsyncMock(return_value=_ref()),
            update_status=AsyncMock(),
        ),
        exchange_client=AsyncMock(),
        graph=_stateful_graph(),
    )
    failure = DatabaseOperationError(
        operation="update_status",
        retryable=True,
        message="ingest failed",
    )

    with patch("src.exchange_service.get_settings", return_value=_settings()), patch(
        "src.exchange_service._upload_attachments_to_lark",
        new_callable=AsyncMock,
    ) as upload, patch(
        "src.exchange_service._ingest_to_qdrant",
        new=AsyncMock(side_effect=failure),
    ), patch(
        "src.exchange_service.lark_app.delete_file_from_drive",
        return_value=True,
    ) as delete, pytest.raises(DatabaseOperationError):
        await _run_ai_path(
            "mail-1",
            {"id": "mail-1"},
            ctx,
            {"configurable": {"thread_id": "mail-1"}},
        )

    upload.assert_not_awaited()
    delete.assert_not_called()


@pytest.mark.asyncio
async def test_ai_path_seeds_slim_checkpoint_before_ingest_without_uploading():
    graph = _compiled_cleanup_graph()
    config = {"configurable": {"thread_id": "mail-crash-safe"}}
    snapshots: dict[str, dict] = {}

    async def fail_ingest(*_args, **_kwargs):
        snapshots["before_ingest"] = deepcopy((await graph.aget_state(config)).values)
        raise DatabaseOperationError(
            operation="update_status",
            retryable=True,
            message="ingest failed",
        )

    ctx = SimpleNamespace(
        db_manager=SimpleNamespace(
            get_content_ref=AsyncMock(return_value=_ref()),
            update_status=AsyncMock(),
        ),
        exchange_client=AsyncMock(),
        graph=graph,
    )
    email = {
        "id": "mail-crash-safe",
        "subject": "small",
        "body": "FULL-BODY-MUST-NOT-BE-CHECKPOINTED",
    }

    with patch("src.exchange_service.get_settings", return_value=_settings()), patch(
        "src.exchange_service._upload_attachments_to_lark",
        new_callable=AsyncMock,
    ) as upload, patch(
        "src.exchange_service._ingest_to_qdrant",
        new=AsyncMock(side_effect=fail_ingest),
    ), patch(
        "src.exchange_service.lark_app.delete_file_from_drive",
        return_value=False,
    ), pytest.raises(DatabaseOperationError):
        await _run_ai_path("mail-crash-safe", email, ctx, config)

    final_state = await graph.aget_state(config)
    upload.assert_not_awaited()
    assert snapshots["before_ingest"]["email_id"] == "mail-crash-safe"
    assert snapshots["before_ingest"]["attachment_tokens"] == []
    assert final_state.values["attachment_tokens"] == []
    assert "FULL-BODY-MUST-NOT-BE-CHECKPOINTED" not in str(final_state.values)


@pytest.mark.asyncio
async def test_empty_handle_readback_does_not_confirm_a_missing_initial_checkpoint():
    state = SimpleNamespace(values={}, next=())
    graph = SimpleNamespace(
        aget_state=AsyncMock(return_value=state),
        aupdate_state=AsyncMock(side_effect=RuntimeError("checkpoint unavailable")),
    )
    ctx = SimpleNamespace(
        db_manager=SimpleNamespace(
            get_content_ref=AsyncMock(return_value=_ref()),
            update_status=AsyncMock(),
        ),
        exchange_client=AsyncMock(),
        graph=graph,
    )

    with patch("src.exchange_service.get_settings", return_value=_settings()), patch(
        "src.exchange_service._upload_attachments_to_lark",
        new_callable=AsyncMock,
    ) as upload, patch(
        "src.exchange_service._ingest_to_qdrant",
        new_callable=AsyncMock,
    ) as ingest:
        await _run_ai_path(
            "mail-no-seed",
            {"id": "mail-no-seed", "attachments": []},
            ctx,
            {"configurable": {"thread_id": "mail-no-seed"}},
        )

    upload.assert_not_awaited()
    ingest.assert_not_awaited()


@pytest.mark.asyncio
async def test_each_attachment_token_is_checkpointed_before_next_upload_on_cancellation():
    graph = _compiled_cleanup_graph()
    config = {"configurable": {"thread_id": "mail-mid-batch-cancel"}}
    ctx = SimpleNamespace(
        db_manager=SimpleNamespace(
            get_content_ref=AsyncMock(return_value=_ref()),
            update_status=AsyncMock(),
        ),
        exchange_client=AsyncMock(),
        graph=graph,
    )
    email = {
        "id": "mail-mid-batch-cancel",
        "subject": "small",
        "body": "BODY-MUST-STAY-LOCAL",
        "attachments": [
            {"name": "first.txt", "content": "MQ=="},
            {"name": "second.txt", "content": "Mg=="},
        ],
    }

    async def complete_pipeline(*_args, **_kwargs):
        await graph.ainvoke(None, config=config)
        return {
            "classification": {"need_reply": True},
            "email": {"id": "mail-mid-batch-cancel", "attachments": []},
        }

    with patch("src.exchange_service.get_settings", return_value=_settings()), patch(
        "src.exchange_service._run_ai_pipeline",
        new=AsyncMock(side_effect=complete_pipeline),
    ), patch(
        "src.exchange_service.lark_app.upload_file_to_drive",
        side_effect=[
            {"file_token": "first-token", "url": "https://example.invalid/first"},
            asyncio.CancelledError(),
        ],
    ) as upload, patch(
        "src.exchange_service.lark_app.delete_file_from_drive",
        return_value=False,
    ), patch(
        "src.exchange_service._ingest_to_qdrant",
        new_callable=AsyncMock,
    ) as ingest, pytest.raises(asyncio.CancelledError):
        await _run_ai_path("mail-mid-batch-cancel", email, ctx, config)

    state = await graph.aget_state(config)
    assert upload.call_count == 2
    ingest.assert_awaited_once()
    assert state.values["attachment_tokens"] == ["first-token"]
    assert "BODY-MUST-STAY-LOCAL" not in str(state.values)
    assert "MQ==" not in str(state.values)


@pytest.mark.asyncio
async def test_unconfirmed_token_ack_stops_upload_and_delete_failure_retains_handle():
    compiled = _compiled_cleanup_graph()

    class FailFirstTokenAck:
        def __init__(self):
            self.update_count = 0

        async def aget_state(self, config):
            return await compiled.aget_state(config)

        async def aupdate_state(self, config, values, **kwargs):
            self.update_count += 1
            if self.update_count == 2:
                raise RuntimeError("checkpoint unavailable")
            return await compiled.aupdate_state(config, values, **kwargs)

    graph = FailFirstTokenAck()
    config = {"configurable": {"thread_id": "mail-ack-failure"}}
    ctx = SimpleNamespace(
        db_manager=SimpleNamespace(
            get_content_ref=AsyncMock(return_value=_ref()),
            update_status=AsyncMock(),
        ),
        exchange_client=AsyncMock(),
        graph=graph,
    )
    email = {
        "id": "mail-ack-failure",
        "attachments": [
            {"name": "first.txt", "content": "MQ=="},
            {"name": "second.txt", "content": "Mg=="},
        ],
    }

    async def complete_pipeline(*_args, **_kwargs):
        await compiled.ainvoke(None, config=config)
        return {
            "classification": {"need_reply": True},
            "email": {"id": "mail-ack-failure", "attachments": []},
        }

    with patch("src.exchange_service.get_settings", return_value=_settings()), patch(
        "src.exchange_service._run_ai_pipeline",
        new=AsyncMock(side_effect=complete_pipeline),
    ), patch(
        "src.exchange_service.lark_app.upload_file_to_drive",
        side_effect=[
            {"file_token": "candidate-token", "url": "https://example.invalid/first"},
            {"file_token": "must-not-upload", "url": "https://example.invalid/second"},
        ],
    ) as upload, patch(
        "src.exchange_service.lark_app.delete_file_from_drive",
        return_value=False,
    ), patch(
        "src.exchange_service._ingest_to_qdrant",
        new_callable=AsyncMock,
    ) as ingest, pytest.raises(DatabaseOperationError):
        await _run_ai_path("mail-ack-failure", email, ctx, config)

    state = await compiled.aget_state(config)
    upload.assert_called_once()
    ingest.assert_awaited_once()
    assert graph.update_count == 3
    assert state.values["attachment_tokens"] == ["candidate-token"]


@pytest.mark.asyncio
async def test_token_ack_commit_then_raise_is_confirmed_by_readback():
    compiled = _compiled_cleanup_graph()

    class CommitThenRaiseFirstTokenAck:
        def __init__(self):
            self.update_count = 0

        async def aget_state(self, config):
            return await compiled.aget_state(config)

        async def aupdate_state(self, config, values, **kwargs):
            self.update_count += 1
            result = await compiled.aupdate_state(config, values, **kwargs)
            if self.update_count == 2:
                raise RuntimeError("ambiguous checkpoint acknowledgement")
            return result

    graph = CommitThenRaiseFirstTokenAck()
    config = {"configurable": {"thread_id": "mail-ack-readback"}}
    ctx = SimpleNamespace(
        db_manager=SimpleNamespace(
            get_content_ref=AsyncMock(return_value=_ref()),
            update_status=AsyncMock(),
        ),
        exchange_client=AsyncMock(),
        graph=graph,
    )
    email = {
        "id": "mail-ack-readback",
        "attachments": [
            {"name": "first.txt", "content": "MQ=="},
            {"name": "second.txt", "content": "Mg=="},
        ],
    }

    async def complete_pipeline(*_args, **_kwargs):
        await compiled.ainvoke(None, config=config)
        return {
            "classification": {"need_reply": True},
            "email": {"id": "mail-ack-readback", "attachments": []},
        }

    with patch("src.exchange_service.get_settings", return_value=_settings()), patch(
        "src.exchange_service._run_ai_pipeline",
        new=AsyncMock(side_effect=complete_pipeline),
    ), patch(
        "src.exchange_service.lark_app.upload_file_to_drive",
        side_effect=[
            {"file_token": "first-token", "url": "https://example.invalid/first"},
            {"file_token": "second-token", "url": "https://example.invalid/second"},
        ],
    ) as upload, patch(
        "src.exchange_service.lark_app.delete_file_from_drive",
        return_value=True,
    ), patch(
        "src.exchange_service._ingest_to_qdrant",
        new_callable=AsyncMock,
    ), patch(
        "src.exchange_service._dispatch_notification",
        new=AsyncMock(return_value={"delivered": False, "kind": "approval"}),
    ):
        await _run_ai_path("mail-ack-readback", email, ctx, config)

    assert upload.call_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("preexisting", [False, True], ids=["fresh", "retry"])
async def test_cleanup_checkpoint_seed_does_not_double_run_graph_entry(preexisting):
    entry_calls: list[tuple[str, ...]] = []

    async def categorize(state):
        entry_calls.append(tuple(state.get("attachment_tokens") or ()))
        return {"classification": {"need_reply": False}}

    workflow = StateGraph(AgentState)
    workflow.add_node("categorizer", categorize)
    workflow.set_entry_point("categorizer")
    workflow.add_edge("categorizer", END)
    graph = workflow.compile(checkpointer=InMemorySaver())
    email_id = "mail-retry" if preexisting else "mail-fresh"
    config = {"configurable": {"thread_id": email_id}}
    email = {
        "id": email_id,
        "subject": "small",
        "body": "BODY-MUST-STAY-LOCAL",
        "attachments": [{"name": "one.txt", "content": "MQ=="}],
    }
    if preexisting:
        previous = build_initial_graph_state(email, _ref())
        previous["attachment_tokens"] = ["old-token"]
        previous["pdf_token"] = "old-pdf"
        await graph.ainvoke(previous, config=config)
        entry_calls.clear()

    ctx = SimpleNamespace(
        db_manager=SimpleNamespace(
            get_content_ref=AsyncMock(return_value=_ref()),
            load_draft=AsyncMock(),
            update_status=AsyncMock(),
        ),
        content_store=SimpleNamespace(load_email=AsyncMock(return_value=email)),
        exchange_client=AsyncMock(),
        graph=graph,
    )

    with patch("src.exchange_service.get_settings", return_value=_settings()), patch(
        "src.exchange_service.lark_app.upload_file_to_drive",
        return_value={
            "file_token": "new-token",
            "url": "https://example.invalid/new",
        },
    ), patch(
        "src.exchange_service._ingest_to_qdrant",
        new_callable=AsyncMock,
    ), patch(
        "src.exchange_service._dispatch_notification",
        new=AsyncMock(return_value={"delivered": True, "kind": "approval"}),
    ), patch(
        "src.exchange_service._mark_email_read",
        new_callable=AsyncMock,
    ):
        await _run_ai_path(email_id, email, ctx, config)

    assert entry_calls == [
        ("old-token",) if preexisting else ()
    ]
    final_state = await graph.aget_state(config)
    assert "BODY-MUST-STAY-LOCAL" not in str(final_state.values)
    assert "MQ==" not in str(final_state.values)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("dispatch", "expected_kind"),
    [
        ({"delivered": True, "kind": "skipped"}, "skipped"),
        ({"delivered": False, "kind": "approval"}, "approval"),
    ],
)
async def test_ai_path_cleans_graph_tokens_when_no_actionable_card_survives(
    dispatch,
    expected_kind,
):
    state_values = build_initial_graph_state(
        {"id": "mail-1"},
        _ref(),
    )
    state_values["attachment_tokens"] = ["attachment-token"]
    state_values["pdf_token"] = "pdf-token"
    state = SimpleNamespace(values=state_values)

    async def update_state(_config, delta, **_kwargs):
        state_values.update(delta)

    async def run_pipeline(*_args, **_kwargs):
        state_values["attachment_tokens"] = [
            "attachment-token",
            "new-attachment-token",
        ]
        return {"classification": {}}

    ctx = SimpleNamespace(
        db_manager=SimpleNamespace(
            get_content_ref=AsyncMock(return_value=_ref()),
            update_status=AsyncMock(),
        ),
        exchange_client=AsyncMock(),
        graph=SimpleNamespace(
            aget_state=AsyncMock(return_value=state),
            aupdate_state=AsyncMock(side_effect=update_state),
        ),
    )

    with patch("src.exchange_service.get_settings", return_value=_settings()), patch(
        "src.exchange_service._upload_attachments_to_lark",
        new=AsyncMock(
            return_value=SimpleNamespace(tokens=("new-attachment-token",), links=())
        ),
    ), patch(
        "src.exchange_service._ingest_to_qdrant",
        new_callable=AsyncMock,
    ), patch(
        "src.exchange_service._run_ai_pipeline",
        new=AsyncMock(side_effect=run_pipeline),
    ), patch(
        "src.exchange_service._dispatch_notification",
        new=AsyncMock(return_value=dispatch),
    ), patch(
        "src.exchange_service.lark_app.delete_file_from_drive",
        return_value=True,
    ) as delete, patch(
        "src.exchange_service._mark_email_read",
        new_callable=AsyncMock,
    ):
        await _run_ai_path(
            "mail-1",
            {"id": "mail-1"},
            ctx,
            {"configurable": {"thread_id": "mail-1"}},
        )

    deleted = {call.args[0] for call in delete.call_args_list}
    if dispatch["delivered"]:
        assert deleted == {
            "attachment-token",
            "new-attachment-token",
            "pdf-token",
        }
        assert state_values["attachment_tokens"] == []
        assert state_values["pdf_token"] is None
    else:
        assert deleted == {"new-attachment-token"}
        assert state_values["attachment_tokens"] == ["attachment-token"]
        assert state_values["pdf_token"] == "pdf-token"
    assert dispatch["kind"] == expected_kind


@pytest.mark.asyncio
async def test_failed_cleanup_handles_are_retained_in_graph_for_retry():
    values = build_initial_graph_state({"id": "mail-1"}, _ref())
    state = SimpleNamespace(values=values)

    async def update(_config, delta):
        values.update(delta)

    ctx = SimpleNamespace(
        graph=SimpleNamespace(
            aget_state=AsyncMock(return_value=state),
            aupdate_state=AsyncMock(side_effect=update),
        )
    )

    with patch(
        "src.exchange_service.lark_app.delete_file_from_drive",
        return_value=False,
    ):
        await _cleanup_graph_drive_files(
            "mail-1",
            ctx,
            fallback_attachment_tokens=["untracked-token"],
        )

    assert values["attachment_tokens"] == ["untracked-token"]


@pytest.mark.asyncio
async def test_failed_dispatch_cleanup_preserves_preexisting_resource_handles():
    values = build_initial_graph_state({"id": "mail-1"}, _ref())
    values["attachment_tokens"] = [
        "old-attachment",
        "old-pdf",
        "new-attachment",
    ]
    values["pdf_token"] = "new-pdf"
    state = SimpleNamespace(values=values)

    async def update(_config, delta):
        values.update(delta)

    ctx = SimpleNamespace(
        graph=SimpleNamespace(
            aget_state=AsyncMock(return_value=state),
            aupdate_state=AsyncMock(side_effect=update),
        )
    )

    with patch(
        "src.exchange_service.lark_app.delete_file_from_drive",
        return_value=True,
    ) as delete:
        await _cleanup_graph_drive_files(
            "mail-1",
            ctx,
            fallback_attachment_tokens=["new-attachment"],
            preserve_attachment_tokens=["old-attachment"],
            preserve_pdf_token="old-pdf",
        )

    assert {call.args[0] for call in delete.call_args_list} == {
        "new-attachment",
        "new-pdf",
    }
    assert values["attachment_tokens"] == ["old-attachment"]
    assert values["pdf_token"] == "old-pdf"
