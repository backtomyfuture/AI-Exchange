from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, MagicMock

import pytest

from src.email_feishu_delivery import (
    DELIVERY_OUTCOME_UNKNOWN_CODE,
    EmailDeliveryDisposition,
    EmailFeishuDelivery,
    ReadNotificationRequest,
)
from src.exchange_service import _ingest_to_qdrant, _mark_email_read


def _stateful_graph(values: dict[str, object]):
    async def get_state(_config):
        return SimpleNamespace(values=dict(values), next=())

    async def update_state(_config, delta, **_kwargs):
        values.update(delta)

    return SimpleNamespace(
        aget_state=AsyncMock(side_effect=get_state),
        aupdate_state=AsyncMock(side_effect=update_state),
    )


@pytest.mark.asyncio
async def test_unguarded_qdrant_error_remains_best_effort():
    ctx = SimpleNamespace(
        email_processor=SimpleNamespace(process_email=MagicMock(side_effect=RuntimeError())),
        db_manager=SimpleNamespace(update_status=AsyncMock()),
    )

    await _ingest_to_qdrant("mail-1", {"id": "mail-1"}, ctx)

    ctx.db_manager.update_status.assert_not_awaited()


@pytest.mark.asyncio
async def test_unguarded_mark_read_error_remains_best_effort():
    ctx = SimpleNamespace(
        exchange_client=SimpleNamespace(mark_as_read=AsyncMock(side_effect=RuntimeError()))
    )

    await _mark_email_read("mail-1", ctx)


@pytest.mark.asyncio
async def test_card_transport_exception_is_manual_review_not_an_automatic_replay():
    values: dict[str, object] = {"attachment_tokens": [], "pdf_token": None}
    graph = _stateful_graph(values)
    database = AsyncMock()
    database.compare_and_set_manual_review = AsyncMock(return_value=True)
    attempts = 0

    async def generate_pdf(*_args, **_kwargs):
        return {"url": "https://feishu.example/review", "file_token": "review-pdf"}

    def send_card(*_args):
        nonlocal attempts
        attempts += 1
        raise RuntimeError("transport disconnected")

    delivery = EmailFeishuDelivery(
        database=database,
        graph=graph,
        graph_dependencies=MagicMock(),
        generate_pdf=generate_pdf,
        send_card=send_card,
        upload_file=MagicMock(),
        delete_file=MagicMock(return_value=True),
    )

    outcome = await delivery.deliver(
        ReadNotificationRequest(
            email_id="mail-1",
            email_data={"id": "mail-1", "attachments": []},
            classification={"need_reply": False},
            context=(),
            routing_log=(),
        ),
        effect_boundary=None,
    )

    assert outcome.disposition is EmailDeliveryDisposition.UNKNOWN
    assert attempts == 1
    database.compare_and_set_manual_review.assert_awaited_once_with(
        "mail-1",
        expected=ANY,
        error_code=DELIVERY_OUTCOME_UNKNOWN_CODE,
    )
    assert values["pdf_token"] == "review-pdf"


@pytest.mark.asyncio
async def test_pdf_generation_failure_is_known_and_does_not_attempt_a_card():
    graph = _stateful_graph({"attachment_tokens": [], "pdf_token": None})
    database = AsyncMock()
    card = MagicMock()

    async def generate_pdf(*_args, **_kwargs):
        return None

    delivery = EmailFeishuDelivery(
        database=database,
        graph=graph,
        graph_dependencies=MagicMock(),
        generate_pdf=generate_pdf,
        send_card=card,
        upload_file=MagicMock(),
        delete_file=MagicMock(return_value=True),
    )

    outcome = await delivery.deliver(
        ReadNotificationRequest(
            email_id="mail-1",
            email_data={"id": "mail-1", "attachments": []},
            classification={"need_reply": False},
            context=(),
            routing_log=(),
        ),
        effect_boundary=None,
    )

    assert outcome.disposition is EmailDeliveryDisposition.KNOWN_FAILURE
    card.assert_not_called()
