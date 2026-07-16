from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from src.exchange_service import (
    MAX_TOKENS,
    _cleanup_graph_drive_files,
    _delete_drive_token_or_retain,
    _delete_replaced_pdf,
    _delete_unclaimed_content_candidate,
    _effect_boundary_kwargs,
    _ensure_durable_content_ref,
    _ingest_to_qdrant,
    _mark_email_read,
    _upload_attachments_to_lark,
)
from src.ingestion.processing import (
    ExternalEffectBoundary,
    GuardedExternalEffectFailed,
    LegacyEffectScope,
)
from src.storage import ContentRef


@pytest.fixture(autouse=True)
def _stable_guarded_account_setting():
    with patch(
        "src.exchange_service.get_settings",
        return_value=SimpleNamespace(EXCHANGE_ACCOUNT_ID=8),
    ):
        yield


def _scope(email_id: str = "message-1") -> LegacyEffectScope:
    return LegacyEffectScope(
        account_id=8,
        inbox_id=str(uuid4()),
        generation=3,
        fencing_token=7,
        attempts=1,
        email_id=str(uuid4()),
        expected_email_version=4,
        event_dedupe_key="a" * 64,
        external_email_id=email_id,
    )


def _boundary(
    callback: AsyncMock | None = None,
) -> tuple[ExternalEffectBoundary, AsyncMock]:
    port = callback or AsyncMock(return_value=None)
    return ExternalEffectBoundary(_scope(), port), port


def _ref(*, object_id: str = "00000000-0000-4000-8000-000000000088") -> ContentRef:
    return ContentRef(
        account_id=8,
        object_id=object_id,
        key_version="v1",
        sha256="8" * 64,
    )


@pytest.mark.parametrize("capacity", [True, -1, MAX_TOKENS + 1, "1"])
@pytest.mark.asyncio
async def test_attachment_upload_rejects_ambiguous_capacity_before_remote_io(
    capacity: object,
) -> None:
    with patch("src.exchange_service.lark_app.upload_file_to_drive") as upload:
        with pytest.raises(ValueError, match="invalid_attachment_upload_capacity"):
            await _upload_attachments_to_lark(
                {"attachments": [{"content": "YQ=="}]},
                max_uploads=capacity,  # type: ignore[arg-type]
            )

    upload.assert_not_called()


@pytest.mark.asyncio
async def test_attachment_upload_skips_contentless_items_and_stops_on_bad_base64() -> (
    None
):
    with patch("src.exchange_service.lark_app.upload_file_to_drive") as upload:
        projection = await _upload_attachments_to_lark(
            {
                "attachments": [
                    {"name": "empty.txt", "content": ""},
                    {"name": "invalid.txt", "content": "not-base64"},
                    {"name": "never.txt", "content": "YQ=="},
                ]
            }
        )

    assert projection.tokens == ()
    assert projection.links == ()
    upload.assert_not_called()


@pytest.mark.parametrize(
    ("remote_result", "expected_links"),
    [
        ({"file_token": "token-1", "url": "https://example.test/file"}, 1),
        ({"file_token": "token-1"}, 0),
    ],
)
@pytest.mark.asyncio
async def test_guarded_attachment_upload_authorizes_and_acks_only_real_tokens(
    remote_result: dict[str, str],
    expected_links: int,
) -> None:
    boundary, port = _boundary()
    acknowledge = AsyncMock(return_value=None)

    with patch(
        "src.exchange_service.lark_app.upload_file_to_drive",
        return_value=remote_result,
    ) as upload:
        projection = await _upload_attachments_to_lark(
            {"attachments": [{"name": "a.txt", "content": "YQ=="}]},
            acknowledge_token=acknowledge,
            _effect_boundary=boundary,
        )

    assert projection.tokens == ("token-1",)
    assert len(projection.links) == expected_links
    assert port.await_args.args[:2] == ("feishu", 0)
    acknowledge.assert_awaited_once_with("token-1")
    upload.assert_called_once_with("a.txt", b"a", 1)


@pytest.mark.asyncio
async def test_zero_attachment_capacity_is_a_strict_noop() -> None:
    boundary, port = _boundary()
    with patch("src.exchange_service.lark_app.upload_file_to_drive") as upload:
        projection = await _upload_attachments_to_lark(
            {"attachments": [{"content": "YQ=="}]},
            max_uploads=0,
            _effect_boundary=boundary,
        )

    assert projection.tokens == ()
    port.assert_not_awaited()
    upload.assert_not_called()


def test_boundary_keyword_projection_never_invents_an_unguarded_boundary() -> None:
    boundary, _port = _boundary()

    assert _effect_boundary_kwargs(None) == {}
    assert _effect_boundary_kwargs(boundary) == {"_effect_boundary": boundary}


@pytest.mark.parametrize(
    ("processor_result", "processor_error"),
    [
        (False, None),
        (None, RuntimeError("qdrant unavailable")),
    ],
    ids=["false-result", "raises"],
)
@pytest.mark.asyncio
async def test_guarded_qdrant_requires_positive_remote_confirmation(
    processor_result: object,
    processor_error: Exception | None,
) -> None:
    boundary, port = _boundary()
    ctx = SimpleNamespace(
        email_processor=SimpleNamespace(
            process_email=MagicMock(
                return_value=processor_result,
                side_effect=processor_error,
            )
        ),
        db_manager=SimpleNamespace(update_status=AsyncMock()),
    )

    with pytest.raises(GuardedExternalEffectFailed):
        await _ingest_to_qdrant(
            "message-1",
            {"id": "message-1"},
            ctx,
            _effect_boundary=boundary,
        )

    assert port.await_args.args[:2] == ("qdrant", 0)
    ctx.db_manager.update_status.assert_not_awaited()


@pytest.mark.parametrize(
    ("remote_result", "remote_error"),
    [
        (False, None),
        (None, RuntimeError("drive unavailable")),
    ],
    ids=["false-result", "raises"],
)
@pytest.mark.asyncio
async def test_guarded_drive_cleanup_never_converts_uncertainty_into_success(
    remote_result: object,
    remote_error: Exception | None,
) -> None:
    boundary, port = _boundary()
    ctx = SimpleNamespace(graph=SimpleNamespace())

    with patch(
        "src.exchange_service.lark_app.delete_file_from_drive",
        return_value=remote_result,
        side_effect=remote_error,
    ):
        with pytest.raises(GuardedExternalEffectFailed):
            await _delete_drive_token_or_retain(
                "message-1",
                ctx,
                "drive-token",
                _state_lock_held=True,
                _effect_boundary=boundary,
            )

    assert port.await_args.args[:2] == ("feishu", 40)


@pytest.mark.asyncio
async def test_replaced_pdf_noop_cases_do_not_authorize_or_delete() -> None:
    boundary, port = _boundary()
    ctx = SimpleNamespace(graph=SimpleNamespace())
    with patch("src.exchange_service.lark_app.delete_file_from_drive") as delete:
        assert await _delete_replaced_pdf(
            "message-1", ctx, None, "new", _effect_boundary=boundary
        )
        assert await _delete_replaced_pdf(
            "message-1", ctx, "same", "same", _effect_boundary=boundary
        )

    port.assert_not_awaited()
    delete.assert_not_called()


@pytest.mark.parametrize(
    ("remote_result", "remote_error"),
    [
        (False, None),
        (None, RuntimeError("drive unavailable")),
    ],
    ids=["false-result", "raises"],
)
@pytest.mark.asyncio
async def test_guarded_replaced_pdf_cleanup_fails_closed(
    remote_result: object,
    remote_error: Exception | None,
) -> None:
    boundary, port = _boundary()
    ctx = SimpleNamespace(graph=SimpleNamespace())

    with patch(
        "src.exchange_service.lark_app.delete_file_from_drive",
        return_value=remote_result,
        side_effect=remote_error,
    ):
        with pytest.raises(GuardedExternalEffectFailed):
            await _delete_replaced_pdf(
                "message-1",
                ctx,
                "old",
                "new",
                _effect_boundary=boundary,
            )

    assert port.await_args.args[:2] == ("feishu", 34)


@pytest.mark.parametrize(
    ("remote_result", "remote_error"),
    [
        (False, None),
        (None, RuntimeError("exchange unavailable")),
    ],
    ids=["false-result", "raises"],
)
@pytest.mark.asyncio
async def test_guarded_mark_read_requires_exact_true_confirmation(
    remote_result: object,
    remote_error: Exception | None,
) -> None:
    boundary, port = _boundary()
    ctx = SimpleNamespace(
        exchange_client=SimpleNamespace(
            mark_as_read=AsyncMock(
                return_value=remote_result,
                side_effect=remote_error,
            )
        )
    )

    with pytest.raises(GuardedExternalEffectFailed):
        await _mark_email_read("message-1", ctx, _effect_boundary=boundary)

    assert port.await_args.args[:2] == ("exchange_mutation", 0)


@pytest.mark.parametrize(
    "failure",
    [asyncio.CancelledError(), RuntimeError("content store unavailable")],
    ids=["cancelled", "raises"],
)
@pytest.mark.asyncio
async def test_guarded_unclaimed_content_cleanup_propagates_or_types_failure(
    failure: BaseException,
) -> None:
    boundary, port = _boundary()
    ctx = SimpleNamespace(
        content_store=SimpleNamespace(delete=AsyncMock(side_effect=failure))
    )

    expected = (
        asyncio.CancelledError
        if isinstance(failure, asyncio.CancelledError)
        else GuardedExternalEffectFailed
    )
    with pytest.raises(expected):
        await _delete_unclaimed_content_candidate(
            _ref(),
            ctx,
            reason="losing-candidate",
            _effect_boundary=boundary,
        )

    assert port.await_args.args[:2] == ("content", 2)


def _content_ctx(
    *,
    put_result: ContentRef | None = None,
    put_error: BaseException | None = None,
    claim_result: object = True,
    claim_error: BaseException | None = None,
    read_result: object = None,
    read_error: BaseException | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        content_store=SimpleNamespace(
            put_email=AsyncMock(
                return_value=put_result or _ref(),
                side_effect=put_error,
            ),
            delete=AsyncMock(return_value=None),
        ),
        db_manager=SimpleNamespace(
            set_content_ref_if_absent=AsyncMock(
                return_value=claim_result,
                side_effect=claim_error,
            ),
            get_content_ref=AsyncMock(
                return_value=read_result,
                side_effect=read_error,
            ),
        ),
    )


@pytest.mark.parametrize(
    ("put_error", "expected"),
    [
        (RuntimeError("put failed"), GuardedExternalEffectFailed),
        (asyncio.CancelledError(), asyncio.CancelledError),
    ],
    ids=["typed-remote-failure", "cancellation-propagates"],
)
@pytest.mark.asyncio
async def test_guarded_content_put_never_enters_claim_after_remote_failure(
    put_error: BaseException,
    expected: type[BaseException],
) -> None:
    boundary, port = _boundary()
    ctx = _content_ctx(put_error=put_error)

    with pytest.raises(expected):
        await _ensure_durable_content_ref(
            "message-1",
            {"id": "message-1"},
            ctx,
            reuse_existing=False,
            _effect_boundary=boundary,
        )

    assert port.await_args.args[:2] == ("content", 0)
    ctx.db_manager.set_content_ref_if_absent.assert_not_awaited()


@pytest.mark.parametrize(
    "claim_failure",
    [asyncio.CancelledError(), RuntimeError("claim failed")],
    ids=["cancelled", "raises"],
)
@pytest.mark.asyncio
async def test_guarded_content_claim_propagates_without_unfenced_reconciliation(
    claim_failure: BaseException,
) -> None:
    boundary, _port = _boundary()
    ctx = _content_ctx(claim_error=claim_failure)

    with pytest.raises(type(claim_failure)):
        await _ensure_durable_content_ref(
            "message-1",
            {"id": "message-1"},
            ctx,
            reuse_existing=False,
            _effect_boundary=boundary,
        )

    ctx.db_manager.get_content_ref.assert_not_awaited()
    ctx.content_store.delete.assert_not_awaited()


@pytest.mark.parametrize(
    "read_failure",
    [asyncio.CancelledError(), RuntimeError("read failed")],
    ids=["cancelled", "raises"],
)
@pytest.mark.asyncio
async def test_guarded_false_claim_propagates_failed_winner_read_without_cleanup(
    read_failure: BaseException,
) -> None:
    boundary, _port = _boundary()
    ctx = _content_ctx(claim_result=False, read_error=read_failure)

    with pytest.raises(type(read_failure)):
        await _ensure_durable_content_ref(
            "message-1",
            {"id": "message-1"},
            ctx,
            reuse_existing=False,
            _effect_boundary=boundary,
        )

    ctx.content_store.delete.assert_not_awaited()


@pytest.mark.parametrize(
    ("state_values", "remote_result", "remote_error", "expected_ordinal"),
    [
        ({"attachment_tokens": ["attachment-token"]}, False, None, 64),
        (
            {"attachment_tokens": ["attachment-token"]},
            None,
            RuntimeError("drive unavailable"),
            64,
        ),
        ({"pdf_token": "pdf-token"}, False, None, 96),
        (
            {"pdf_token": "pdf-token"},
            None,
            RuntimeError("drive unavailable"),
            96,
        ),
    ],
    ids=[
        "attachment-false",
        "attachment-raises",
        "pdf-false",
        "pdf-raises",
    ],
)
@pytest.mark.asyncio
async def test_guarded_graph_cleanup_stops_on_first_uncertain_remote_delete(
    state_values: dict[str, object],
    remote_result: object,
    remote_error: Exception | None,
    expected_ordinal: int,
) -> None:
    boundary, port = _boundary()
    state = SimpleNamespace(values=state_values, next=())
    ctx = SimpleNamespace(
        graph=SimpleNamespace(
            aget_state=AsyncMock(return_value=state),
            aupdate_state=AsyncMock(),
        )
    )

    with patch(
        "src.exchange_service.lark_app.delete_file_from_drive",
        return_value=remote_result,
        side_effect=remote_error,
    ):
        with pytest.raises(GuardedExternalEffectFailed):
            await _cleanup_graph_drive_files(
                "message-1",
                ctx,
                fallback_attachment_tokens=[],
                _state_lock_held=True,
                _effect_boundary=boundary,
            )

    assert port.await_args.args[:2] == ("feishu", expected_ordinal)
    ctx.graph.aupdate_state.assert_not_awaited()
