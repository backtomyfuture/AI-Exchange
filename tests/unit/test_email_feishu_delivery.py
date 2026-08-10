from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.email_feishu_delivery import (
    ApprovalRequest,
    EmailDeliveryDisposition,
    EmailFeishuDelivery,
    LarkCardDelivery,
    ManualReviewNotificationRequest,
    ReadNotificationRequest,
)
from src.router.decision import (
    DecisionOutcome,
    RouteDecision,
    RouteProvenance,
    RouteTier,
)
from src.router.tier1.schema import CanonicalRoute
from src.safety.recipients import ResolvedRecipients


@pytest.mark.asyncio
async def test_read_notification_confirms_delivery_and_persists_its_status():
    """The public delivery seam confirms a read card only after PDF and Lark succeed."""

    email_id = "email-read-notification"
    values = {"attachment_tokens": [], "pdf_token": None}

    async def get_state(_config):
        return SimpleNamespace(values=dict(values), next=())

    async def update_state(_config, delta, **_kwargs):
        values.update(delta)

    graph = MagicMock()
    graph.aget_state = AsyncMock(side_effect=get_state)
    graph.aupdate_state = AsyncMock(side_effect=update_state)
    database = AsyncMock()
    published: list[tuple[object, str]] = []

    async def generate_pdf(*_args, **_kwargs):
        return {"url": "https://feishu.example/pdf", "file_token": "pdf-token"}

    def send_card(request, pdf_url):
        published.append((request, pdf_url))
        return LarkCardDelivery(accepted=True, outcome_known=True, message_id="m-1")

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
            email_id=email_id,
            email_data={"id": email_id, "attachments": []},
            classification={"need_reply": False, "priority": "P1"},
            context=(),
            routing_log=(),
        ),
        effect_boundary=None,
    )

    assert outcome.disposition is EmailDeliveryDisposition.CONFIRMED
    assert outcome.pdf_token == "pdf-token"
    assert outcome.message_id == "m-1"
    assert values["pdf_token"] == "pdf-token"
    database.update_status.assert_awaited_once_with(
        email_id,
        "notified_readonly",
    )
    assert published[0][1] == "https://feishu.example/pdf"


@pytest.mark.asyncio
async def test_unknown_read_notification_outcome_is_quarantined_without_retrying():
    """A transport-unknown card outcome becomes manual review and keeps its PDF."""

    email_id = "email-read-unknown"
    values = {"attachment_tokens": [], "pdf_token": None}

    async def get_state(_config):
        return SimpleNamespace(values=dict(values), next=())

    async def update_state(_config, delta, **_kwargs):
        values.update(delta)

    graph = MagicMock()
    graph.aget_state = AsyncMock(side_effect=get_state)
    graph.aupdate_state = AsyncMock(side_effect=update_state)
    database = AsyncMock()
    database.compare_and_set_manual_review = AsyncMock(return_value=True)
    delete_file = MagicMock(return_value=True)

    async def generate_pdf(*_args, **_kwargs):
        return {"url": "https://feishu.example/pdf", "file_token": "pdf-token"}

    def send_card(_request, _pdf_url):
        return LarkCardDelivery(accepted=False, outcome_known=False)

    delivery = EmailFeishuDelivery(
        database=database,
        graph=graph,
        graph_dependencies=MagicMock(),
        generate_pdf=generate_pdf,
        send_card=send_card,
        upload_file=MagicMock(),
        delete_file=delete_file,
    )

    outcome = await delivery.deliver(
        ReadNotificationRequest(
            email_id=email_id,
            email_data={"id": email_id, "attachments": []},
            classification={"need_reply": False},
            context=(),
            routing_log=(),
        ),
        effect_boundary=None,
    )

    assert outcome.disposition is EmailDeliveryDisposition.UNKNOWN
    assert outcome.pdf_token == "pdf-token"
    database.compare_and_set_manual_review.assert_awaited_once_with(
        email_id,
        expected=frozenset(
            {
                "pending",
                "recovering",
                "ingested",
                "analyzed",
                "drafted",
                "error",
                "delivery_failed",
            }
        ),
        error_code="feishu_delivery_outcome_unknown",
    )
    database.update_status.assert_not_awaited()
    delete_file.assert_not_called()


@pytest.mark.asyncio
async def test_read_notification_authorizes_pdf_and_card_with_the_existing_boundary():
    """A delivery reuses the caller's fence instead of creating its own one."""

    email_id = "email-read-fenced"
    values = {"attachment_tokens": [], "pdf_token": None}

    async def get_state(_config):
        return SimpleNamespace(values=dict(values), next=())

    async def update_state(_config, delta, **_kwargs):
        values.update(delta)

    graph = MagicMock()
    graph.aget_state = AsyncMock(side_effect=get_state)
    graph.aupdate_state = AsyncMock(side_effect=update_state)
    database = AsyncMock()
    boundary = MagicMock()
    boundary.before = AsyncMock()

    async def generate_pdf(*_args, **_kwargs):
        return {"url": "https://feishu.example/pdf", "file_token": "pdf-token"}

    delivery = EmailFeishuDelivery(
        database=database,
        graph=graph,
        graph_dependencies=MagicMock(),
        generate_pdf=generate_pdf,
        send_card=lambda _request, _url: LarkCardDelivery(True, True),
        upload_file=MagicMock(),
        delete_file=MagicMock(return_value=True),
    )

    await delivery.deliver(
        ReadNotificationRequest(
            email_id=email_id,
            email_data={"id": email_id, "attachments": []},
            classification={"need_reply": False},
            context=(),
            routing_log=(),
        ),
        effect_boundary=boundary,
    )

    assert [call.args[:2] for call in boundary.before.await_args_list] == [
        ("feishu", 32),
        ("feishu", 33),
    ]


@pytest.mark.asyncio
async def test_approval_confirms_delivery_and_persists_its_status():
    """Approval uses the same delivery seam but retains its draft payload."""

    email_id = "email-approval"
    values = {"attachment_tokens": [], "pdf_token": None}

    async def get_state(_config):
        return SimpleNamespace(values=dict(values), next=())

    async def update_state(_config, delta, **_kwargs):
        values.update(delta)

    graph = MagicMock()
    graph.aget_state = AsyncMock(side_effect=get_state)
    graph.aupdate_state = AsyncMock(side_effect=update_state)
    database = AsyncMock()
    seen_request = None

    async def generate_pdf(*_args, **_kwargs):
        return {"url": "https://feishu.example/pdf", "file_token": "pdf-token"}

    def send_card(request, _pdf_url):
        nonlocal seen_request
        seen_request = request
        return LarkCardDelivery(accepted=True, outcome_known=True)

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
        ApprovalRequest(
            email_id=email_id,
            email_data={"id": email_id, "attachments": []},
            classification={"need_reply": True},
            draft="请审批这份回复",
            context=(),
            routing_log=(),
        ),
        effect_boundary=None,
    )

    assert outcome.disposition is EmailDeliveryDisposition.CONFIRMED
    assert seen_request.draft == "请审批这份回复"
    database.update_status.assert_awaited_once_with(email_id, "waiting_approval")


@pytest.mark.asyncio
async def test_durable_approval_freezes_revision_before_rendering_card():
    email_id = "email-durable-approval"
    inbox_id = "00000000-0000-4000-8000-000000000123"
    plan_digest = "1" * 64
    evidence_digest = "2" * 64
    payload_digest = "3" * 64
    decision = RouteDecision(
        outcome=DecisionOutcome.MATCHED,
        route=CanonicalRoute.REPLY,
        params={},
        provenance=RouteProvenance(
            tier=RouteTier.TIER3,
            source_version="router-model-v1",
        ),
        handoff_profile_id="generic_reply_v1",
    )
    values = {
        "attachment_tokens": [],
        "pdf_token": None,
        "route_decision": decision.model_dump(mode="json"),
        "handoff_plan_digest": plan_digest,
        "evidence_pack_digest": evidence_digest,
        "draft_id": email_id,
        "draft_to": ["recipient@example.com"],
        "draft_cc": [],
    }

    async def get_state(_config):
        return SimpleNamespace(values=dict(values), next=())

    async def update_state(_config, delta, **_kwargs):
        values.update(delta)

    graph = MagicMock()
    graph.aget_state = AsyncMock(side_effect=get_state)
    graph.aupdate_state = AsyncMock(side_effect=update_state)
    database = AsyncMock()
    database.get_handoff_run = AsyncMock(
        return_value={
            "state": "evidence_ready",
            "version": 1,
            "evidence_digest": evidence_digest,
        }
    )
    database.create_payload_revision = AsyncMock(return_value=1)
    database.get_payload_revision_binding = AsyncMock(
        return_value={
            "inbox_id": inbox_id,
            "payload_revision": 1,
            "payload_digest": payload_digest,
        }
    )
    database.advance_handoff_execution = AsyncMock()
    resolver = AsyncMock(
        return_value=ResolvedRecipients(
            to=("recipient@example.com",),
            cc=(),
        )
    )
    seen_request = None

    async def generate_pdf(*_args, **_kwargs):
        return {"url": "https://feishu.example/pdf", "file_token": "pdf-token"}

    def send_card(request, _pdf_url):
        nonlocal seen_request
        seen_request = request
        return LarkCardDelivery(accepted=True, outcome_known=True)

    boundary = SimpleNamespace(
        scope=SimpleNamespace(inbox_id=inbox_id),
        before=AsyncMock(),
    )
    delivery = EmailFeishuDelivery(
        database=database,
        graph=graph,
        graph_dependencies=MagicMock(),
        generate_pdf=generate_pdf,
        send_card=send_card,
        upload_file=MagicMock(),
        delete_file=MagicMock(return_value=True),
        resolve_approval_recipients=resolver,
    )

    outcome = await delivery.deliver(
        ApprovalRequest(
            email_id=email_id,
            email_data={
                "id": email_id,
                "sender": "sender@example.com",
                "attachments": [],
                "draft_to": ["recipient@example.com"],
                "draft_cc": [],
            },
            classification={"need_reply": True},
            draft="frozen approved draft",
            context=(),
            routing_log=(),
            inbox_id=inbox_id,
        ),
        effect_boundary=boundary,
    )

    assert outcome.disposition is EmailDeliveryDisposition.CONFIRMED
    assert seen_request.payload_revision == 1
    assert seen_request.payload_digest == payload_digest
    frozen = database.create_payload_revision.await_args.kwargs["payload"]
    assert frozen["decision_digest"] == decision.canonical_digest()
    assert frozen["plan_digest"] == plan_digest
    assert frozen["evidence_digest"] == evidence_digest
    assert frozen["to"] == ["recipient@example.com"]
    assert values["payload_revision"] == 1
    assert values["payload_digest"] == payload_digest


@pytest.mark.asyncio
async def test_manual_review_confirms_with_a_compare_and_set_not_mark_read_status():
    """Manual review cards use the manual-review transition after card delivery."""

    email_id = "email-manual-review"
    values = {"attachment_tokens": [], "pdf_token": None}

    async def get_state(_config):
        return SimpleNamespace(values=dict(values), next=())

    async def update_state(_config, delta, **_kwargs):
        values.update(delta)

    graph = MagicMock()
    graph.aget_state = AsyncMock(side_effect=get_state)
    graph.aupdate_state = AsyncMock(side_effect=update_state)
    database = AsyncMock()
    database.compare_and_set_manual_review = AsyncMock(return_value=True)
    order: list[str] = []
    upload_file = MagicMock()

    async def generate_pdf(*_args, **_kwargs):
        return {"url": "https://feishu.example/pdf", "file_token": "pdf-token"}

    def send_card(_request, _pdf_url):
        order.append("card")
        return LarkCardDelivery(accepted=True, outcome_known=True)

    delivery = EmailFeishuDelivery(
        database=database,
        graph=graph,
        graph_dependencies=MagicMock(),
        generate_pdf=generate_pdf,
        send_card=send_card,
        upload_file=upload_file,
        delete_file=MagicMock(return_value=True),
    )

    outcome = await delivery.deliver(
        ManualReviewNotificationRequest(
            email_id=email_id,
            email_data={
                "id": email_id,
                "attachments": [{"name": "note.txt", "content": "YQ=="}],
            },
            classification={"priority": "P1"},
            reason="categorizer_model_failed",
            context=(),
            routing_log=(),
        ),
        effect_boundary=None,
    )

    assert outcome.disposition is EmailDeliveryDisposition.CONFIRMED
    assert order == ["card"]
    database.compare_and_set_manual_review.assert_awaited_once_with(
        email_id,
        expected=frozenset(
            {
                "pending",
                "recovering",
                "ingested",
                "analyzed",
                "drafted",
                "error",
                "delivery_failed",
            }
        ),
        error_code="categorizer_model_failed",
    )
    database.update_status.assert_not_awaited()
    upload_file.assert_not_called()


@pytest.mark.asyncio
async def test_known_read_notification_failure_is_recorded_and_cleans_unsent_pdf():
    """A rejected card is known-safe to record as failed and clean up."""

    email_id = "email-read-rejected"
    values = {"attachment_tokens": [], "pdf_token": None}

    async def get_state(_config):
        return SimpleNamespace(values=dict(values), next=())

    async def update_state(_config, delta, **_kwargs):
        values.update(delta)

    graph = MagicMock()
    graph.aget_state = AsyncMock(side_effect=get_state)
    graph.aupdate_state = AsyncMock(side_effect=update_state)
    database = AsyncMock()
    delete_file = MagicMock(return_value=True)

    async def generate_pdf(*_args, **_kwargs):
        return {"url": "https://feishu.example/pdf", "file_token": "pdf-token"}

    delivery = EmailFeishuDelivery(
        database=database,
        graph=graph,
        graph_dependencies=MagicMock(),
        generate_pdf=generate_pdf,
        send_card=lambda _request, _url: LarkCardDelivery(False, True),
        upload_file=MagicMock(),
        delete_file=delete_file,
    )

    outcome = await delivery.deliver(
        ReadNotificationRequest(
            email_id=email_id,
            email_data={"id": email_id, "attachments": []},
            classification={"need_reply": False},
            context=(),
            routing_log=(),
        ),
        effect_boundary=None,
    )

    assert outcome.disposition is EmailDeliveryDisposition.KNOWN_FAILURE
    database.update_status.assert_awaited_once_with(
        email_id,
        "delivery_failed",
        error_message="read_only_card_rejected",
    )
    delete_file.assert_called_once_with("pdf-token")
    assert values["pdf_token"] is None


@pytest.mark.asyncio
async def test_approval_uploads_business_attachment_before_publishing_its_card():
    """Approval card payloads contain the Drive link for an admitted attachment."""

    email_id = "email-approval-attachment"
    values = {"attachment_tokens": [], "pdf_token": None}

    async def get_state(_config):
        return SimpleNamespace(values=dict(values), next=())

    async def update_state(_config, delta, **_kwargs):
        values.update(delta)

    graph = MagicMock()
    graph.aget_state = AsyncMock(side_effect=get_state)
    graph.aupdate_state = AsyncMock(side_effect=update_state)
    database = AsyncMock()
    seen_request = None

    async def generate_pdf(*_args, **_kwargs):
        return {"url": "https://feishu.example/pdf", "file_token": "pdf-token"}

    def send_card(request, _pdf_url):
        nonlocal seen_request
        seen_request = request
        return LarkCardDelivery(accepted=True, outcome_known=True)

    upload_file = MagicMock(
        return_value={
            "file_token": "attachment-token",
            "url": "https://feishu.example/attachment",
        }
    )
    delivery = EmailFeishuDelivery(
        database=database,
        graph=graph,
        graph_dependencies=MagicMock(),
        generate_pdf=generate_pdf,
        send_card=send_card,
        upload_file=upload_file,
        delete_file=MagicMock(return_value=True),
    )

    await delivery.deliver(
        ApprovalRequest(
            email_id=email_id,
            email_data={
                "id": email_id,
                "attachments": [{"name": "note.txt", "content": "YQ=="}],
            },
            classification={"need_reply": True},
            draft="请审批",
            context=(),
            routing_log=(),
        ),
        effect_boundary=None,
    )

    upload_file.assert_called_once_with("note.txt", b"a", 1)
    assert seen_request.email_data["attachments"][0]["lark_file_url"] == (
        "https://feishu.example/attachment"
    )
    assert values["attachment_tokens"] == ["attachment-token"]


@pytest.mark.asyncio
async def test_confirmed_delivery_retires_a_replaced_pdf_only_after_card_send():
    """The old review PDF is never removed before the new card is confirmed."""

    email_id = "email-replaced-pdf"
    values = {"attachment_tokens": [], "pdf_token": "old-pdf"}
    order: list[str] = []

    async def get_state(_config):
        return SimpleNamespace(values=dict(values), next=())

    async def update_state(_config, delta, **_kwargs):
        values.update(delta)

    graph = MagicMock()
    graph.aget_state = AsyncMock(side_effect=get_state)
    graph.aupdate_state = AsyncMock(side_effect=update_state)
    database = AsyncMock()

    async def generate_pdf(*_args, **_kwargs):
        return {"url": "https://feishu.example/new", "file_token": "new-pdf"}

    def send_card(_request, _url):
        order.append("card")
        return LarkCardDelivery(True, True)

    def delete_file(token):
        order.append(f"delete:{token}")
        return True

    delivery = EmailFeishuDelivery(
        database=database,
        graph=graph,
        graph_dependencies=MagicMock(),
        generate_pdf=generate_pdf,
        send_card=send_card,
        upload_file=MagicMock(),
        delete_file=delete_file,
    )

    outcome = await delivery.deliver(
        ReadNotificationRequest(
            email_id=email_id,
            email_data={"id": email_id, "attachments": []},
            classification={"need_reply": False},
            context=(),
            routing_log=(),
        ),
        effect_boundary=None,
    )

    assert outcome.disposition is EmailDeliveryDisposition.CONFIRMED
    assert order == ["card", "delete:old-pdf"]
    assert values["pdf_token"] == "new-pdf"


@pytest.mark.asyncio
async def test_pdf_stage_must_be_read_back_before_a_card_can_be_sent():
    """A lost Graph write is a known failure, never a card with an orphan PDF."""

    email_id = "email-pdf-stage-readback"
    graph = MagicMock()
    graph.aget_state = AsyncMock(
        return_value=SimpleNamespace(values={"attachment_tokens": [], "pdf_token": None})
    )
    graph.aupdate_state = AsyncMock()
    database = AsyncMock()
    card = MagicMock(return_value=LarkCardDelivery(True, True))

    async def generate_pdf(*_args, **_kwargs):
        return {"url": "https://feishu.example/pdf", "file_token": "pdf-token"}

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
            email_id=email_id,
            email_data={"id": email_id, "attachments": []},
            classification={"need_reply": False},
            context=(),
            routing_log=(),
        ),
        effect_boundary=None,
    )

    assert outcome.disposition is EmailDeliveryDisposition.KNOWN_FAILURE
    card.assert_not_called()


@pytest.mark.asyncio
async def test_notification_delivery_survives_a_checkpoint_that_never_ran_a_node():
    """A real LangGraph checkpoint seeded via as_node="__start__" and never
    advanced (a Tier 1/2/3 route, e.g. tier1_conflict, that bypasses
    ``_run_ai_pipeline``) must not raise ``InvalidUpdateError`` when the
    notification PDF's token is recorded afterwards.

    This is a regression test for the production incident where every
    ``manual_review``/``read_only`` email needing a PDF attachment failed
    with ``InvalidUpdateError: Ambiguous update, specify as_node`` right
    after the PDF was already uploaded to Feishu, leaving the graph
    checkpoint stuck at ``next=('categorizer',)`` and the inbox row at
    ``notification_pdf_cleanup_untracked`` / ``effect_outcome_unknown``.
    Uses the real compiled graph and ``MemorySaver`` instead of a mock so a
    future change to ``as_node`` handling cannot silently regress.
    """
    from unittest.mock import MagicMock as _MagicMock

    from langgraph.checkpoint.memory import MemorySaver

    from src.graph.builder import build_graph

    email_id = "email-tier1-conflict-bypass"
    checkpointer = MemorySaver()
    graph = build_graph(checkpointer, dependencies=_MagicMock())
    config = {"configurable": {"thread_id": email_id}}

    # Mirrors src.exchange_service._checkpoint_ai_path_resources: seed the
    # checkpoint before any real node runs, exactly as the manual_review /
    # tier1_conflict bypass path does.
    await graph.aupdate_state(
        config,
        {
            "email_id": email_id,
            "content_ref": "ref-1",
            "attachment_tokens": [],
            "pdf_token": None,
        },
        as_node="__start__",
    )
    assert (await graph.aget_state(config)).next == ("categorizer",)

    database = AsyncMock()

    async def generate_pdf(*_args, **_kwargs):
        return {"url": "https://feishu.example/pdf", "file_token": "pdf-token"}

    def send_card(_request, _url):
        return LarkCardDelivery(True, True, message_id="m-1")

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
            email_id=email_id,
            email_data={"id": email_id, "attachments": []},
            classification={"need_reply": False},
            context=(),
            routing_log=(),
        ),
        effect_boundary=None,
    )

    assert outcome.disposition is EmailDeliveryDisposition.CONFIRMED
    assert outcome.pdf_token == "pdf-token"
    final_state = await graph.aget_state(config)
    assert final_state.values["pdf_token"] == "pdf-token"
    # The bookkeeping write must not resurrect the pipeline: the entry node
    # stays pending, it must not be re-triggered or cleared.
    assert final_state.next == ("categorizer",)



