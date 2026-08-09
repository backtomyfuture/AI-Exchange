"""
Lark messaging functions - extracted from lark_app.py for modularity.
Handles: send_approval_card, send_read_only_card, send_manual_review_card,
send_system_notification
"""
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import List

from lark_oapi.api.im.v1 import (
    CreateMessageRequest,
    CreateMessageRequestBody,
    ListMessageRequest,
)

from src.config import get_settings
from src.security.redaction import fingerprint_identifier

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class LarkTextDelivery:
    """The limited delivery fact needed by a durable text-notification caller.

    ``outcome_known`` is deliberately distinct from ``accepted``.  A transport
    exception can happen after Lark accepted the request, so callers must
    reconcile rather than retry it blindly.
    """

    accepted: bool
    outcome_known: bool
    message_id: str | None = None


class LarkTextReconciliationUnavailable(RuntimeError):
    """The configured chat could not be read safely for delivery recovery."""


def _send_interactive_card(
    *,
    email_id: str,
    card_content: object,
    card_kind: str,
    lark_api_client: object | None,
) -> LarkTextDelivery:
    """Send one completed card without collapsing transport ambiguity into False."""
    if lark_api_client is None:
        logger.error("Lark Client not initialized. Cannot send card.")
        return LarkTextDelivery(accepted=False, outcome_known=True)

    chat_id = str(getattr(get_settings(), "LARK_CHAT_ID", "") or "")
    if not chat_id:
        logger.error("LARK_CHAT_ID not configured.")
        return LarkTextDelivery(accepted=False, outcome_known=True)

    try:
        request = CreateMessageRequest.builder() \
            .receive_id_type("chat_id") \
            .request_body(CreateMessageRequestBody.builder()
                .receive_id(chat_id)
                .msg_type("interactive")
                .content(json.dumps(card_content))
                .build()) \
            .build()
        response = lark_api_client.im.v1.message.create(request)
    except Exception as exc:
        # The SDK can fail after Lark has accepted the request.  Only a later
        # reconciliation may decide whether another card is safe.
        logger.error(
            "Lark %s card transport outcome unknown: error_type=%s",
            card_kind,
            type(exc).__name__,
        )
        return LarkTextDelivery(accepted=False, outcome_known=False)

    if not response.success():
        logger.error("Lark %s card rejected: code=%s", card_kind, response.code)
        return LarkTextDelivery(accepted=False, outcome_known=True)
    message_id = getattr(getattr(response, "data", None), "message_id", None)
    if not isinstance(message_id, str) or not message_id:
        message_id = None
    logger.info(
        "Lark %s card sent: email=%s message=%s",
        card_kind,
        fingerprint_identifier(email_id, namespace="email"),
        fingerprint_identifier(message_id or "unknown", namespace="lark_message"),
    )
    return LarkTextDelivery(
        accepted=True,
        outcome_known=True,
        message_id=message_id,
    )


def deliver_approval_card(
    email_id: str,
    draft: str,
    context: List[dict],
    email_data: dict,
    classification: dict,
    pdf_url: str = None,
    routing_log: List = None,
    *,
    lark_api_client=None,
    card_builder=None,
) -> LarkTextDelivery:
    """Return the complete delivery fact for one interactive approval card."""
    if card_builder is None:
        logger.error("Card builder not initialized. Cannot send card.")
        return LarkTextDelivery(accepted=False, outcome_known=True)
    try:
        card_content = card_builder.build_approval_card(
            email_id,
            draft,
            context,
            email_data,
            classification,
            pdf_url=pdf_url,
            routing_log=routing_log,
        )
    except Exception as exc:
        logger.error(
            "Lark approval card build failed: error_type=%s",
            type(exc).__name__,
        )
        return LarkTextDelivery(accepted=False, outcome_known=True)
    return _send_interactive_card(
        email_id=email_id,
        card_content=card_content,
        card_kind="approval",
        lark_api_client=lark_api_client,
    )


def send_approval_card(
    email_id: str,
    draft: str,
    context: List[dict],
    email_data: dict,
    classification: dict,
    pdf_url: str = None,
    routing_log: List = None,
    *,
    lark_api_client=None,
    card_builder=None,
) -> bool:
    """Compatibility projection for callers that only need acceptance."""
    return deliver_approval_card(
        email_id,
        draft,
        context,
        email_data,
        classification,
        pdf_url=pdf_url,
        routing_log=routing_log,
        lark_api_client=lark_api_client,
        card_builder=card_builder,
    ).accepted


def deliver_read_only_card(
    email_id: str,
    context: List[dict],
    email_data: dict,
    classification: dict,
    pdf_url: str = None,
    routing_log: List = None,
    *,
    lark_api_client=None,
    card_builder=None,
) -> LarkTextDelivery:
    """Return the complete delivery fact for one read-notification card."""
    if card_builder is None:
        logger.error("Card builder not initialized. Cannot send card.")
        return LarkTextDelivery(accepted=False, outcome_known=True)
    try:
        card_content = card_builder.build_read_only_card(
            email_id,
            context,
            email_data,
            classification,
            pdf_url=pdf_url,
            routing_log=routing_log,
        )
    except Exception as exc:
        logger.error(
            "Lark read-only card build failed: error_type=%s",
            type(exc).__name__,
        )
        return LarkTextDelivery(accepted=False, outcome_known=True)
    return _send_interactive_card(
        email_id=email_id,
        card_content=card_content,
        card_kind="read_only",
        lark_api_client=lark_api_client,
    )


def send_read_only_card(
    email_id: str,
    context: List[dict],
    email_data: dict,
    classification: dict,
    pdf_url: str = None,
    routing_log: List = None,
    *,
    lark_api_client=None,
    card_builder=None,
) -> bool:
    """Compatibility projection for callers that only need acceptance."""
    return deliver_read_only_card(
        email_id,
        context,
        email_data,
        classification,
        pdf_url=pdf_url,
        routing_log=routing_log,
        lark_api_client=lark_api_client,
        card_builder=card_builder,
    ).accepted


def deliver_manual_review_card(
    email_id: str,
    email_data: dict,
    reason: str,
    classification: dict | None = None,
    pdf_url=None,
    routing_log: List | None = None,
    *,
    lark_api_client=None,
    card_builder=None,
) -> LarkTextDelivery:
    """Return the complete delivery fact for an immutable manual-review card."""
    if not lark_api_client:
        logger.error("Lark Client not initialized. Cannot send manual-review card.")
        return LarkTextDelivery(accepted=False, outcome_known=True)

    if not card_builder:
        logger.error("Card builder not initialized. Cannot send manual-review card.")
        return LarkTextDelivery(accepted=False, outcome_known=True)

    data = email_data if isinstance(email_data, dict) else {}
    try:
        card_content = card_builder.build_manual_review_card(
            email_id, data, reason, classification=classification, pdf_url=pdf_url,
            routing_log=routing_log,
        )
    except Exception as exc:
        logger.error(
            "Manual-review card build failed: error_type=%s",
            type(exc).__name__,
        )
        return LarkTextDelivery(accepted=False, outcome_known=True)
    return _send_interactive_card(
        email_id=email_id,
        card_content=card_content,
        card_kind="manual_review",
        lark_api_client=lark_api_client,
    )


def send_manual_review_card(
    email_id: str,
    email_data: dict,
    reason: str,
    classification: dict | None = None,
    pdf_url=None,
    routing_log: List | None = None,
    *,
    lark_api_client=None,
    card_builder=None,
) -> bool:
    """Compatibility projection for callers that only need acceptance."""
    return deliver_manual_review_card(
        email_id,
        email_data,
        reason,
        classification=classification,
        pdf_url=pdf_url,
        routing_log=routing_log,
        lark_api_client=lark_api_client,
        card_builder=card_builder,
    ).accepted


def send_system_notification(title: str, content: str, template: str = "red",
                             *, lark_api_client=None) -> bool:
    """Send a system notification card. Returns True on Lark acknowledgement."""
    if lark_api_client is None:
        from src.utils.lark_app import lark_api_client as _client
        lark_api_client = _client

    if not lark_api_client:
        logger.error("Lark Client not initialized. Cannot send system notification.")
        return False

    settings = get_settings()
    chat_id = settings.LARK_CHAT_ID
    if not chat_id:
        logger.error("LARK_CHAT_ID not configured.")
        return False

    card_content = {
        "header": {
            "template": template,
            "title": {"content": title, "tag": "plain_text"}
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": content}}
        ]
    }

    try:
        request = CreateMessageRequest.builder() \
            .receive_id_type("chat_id") \
            .request_body(CreateMessageRequestBody.builder()
                .receive_id(chat_id)
                .msg_type("interactive")
                .content(json.dumps(card_content))
                .build()) \
            .build()

        response = lark_api_client.im.v1.message.create(request)
    except Exception as exc:
        logger.error(
            "Lark system notification failed: error_type=%s",
            type(exc).__name__,
        )
        return False

    if not response.success():
        logger.error("Lark system notification rejected: code=%s", response.code)
        return False
    logger.info("Lark system notification sent")
    return True


def send_text_message(
    text: str,
    *,
    request_uuid: str,
    chat_id: str | None = None,
    lark_api_client=None,
) -> LarkTextDelivery:
    """Send one plain-text Lark message with a stable request UUID.

    This is intentionally separate from the interactive-card helpers above.
    It does not log the message body: callers may include operational email
    metadata in the text, and durable delivery state already records it.
    """

    if lark_api_client is None:
        from src.utils.lark_app import lark_api_client as _client

        lark_api_client = _client
    if not lark_api_client:
        logger.error("Lark Client not initialized. Cannot send text message.")
        return LarkTextDelivery(accepted=False, outcome_known=True)

    resolved_chat_id = chat_id
    if resolved_chat_id is None:
        resolved_chat_id = get_settings().LARK_CHAT_ID
    if not resolved_chat_id:
        logger.error("LARK_CHAT_ID not configured.")
        return LarkTextDelivery(accepted=False, outcome_known=True)
    if not isinstance(text, str) or not text or not isinstance(request_uuid, str):
        logger.error("Lark text message input invalid.")
        return LarkTextDelivery(accepted=False, outcome_known=True)

    try:
        request = (
            CreateMessageRequest.builder()
            .receive_id_type("chat_id")
            .request_body(
                CreateMessageRequestBody.builder()
                .receive_id(resolved_chat_id)
                .msg_type("text")
                .content(json.dumps({"text": text}, ensure_ascii=False))
                .uuid(request_uuid)
                .build()
            )
            .build()
        )
        response = lark_api_client.im.v1.message.create(request)
    except Exception as exc:
        # The request may already have reached Lark.  The caller must inspect
        # the configured chat before attempting another request UUID.
        logger.error("Lark text message send uncertain: error_type=%s", type(exc).__name__)
        return LarkTextDelivery(accepted=False, outcome_known=False)

    if not response.success():
        logger.error("Lark text message rejected: code=%s", getattr(response, "code", None))
        return LarkTextDelivery(accepted=False, outcome_known=True)

    data = getattr(response, "data", None)
    message_id = getattr(data, "message_id", None)
    if not isinstance(message_id, str):
        message_id = None
    logger.info("Lark text message accepted")
    return LarkTextDelivery(
        accepted=True,
        outcome_known=True,
        message_id=message_id,
    )


def send_daily_digest_text(
    text: str,
    *,
    request_uuid: str,
    chat_id: str | None = None,
    lark_api_client=None,
) -> LarkTextDelivery:
    """Named port for the daily digest; it remains an ordinary text message."""

    return send_text_message(
        text,
        request_uuid=request_uuid,
        chat_id=chat_id,
        lark_api_client=lark_api_client,
    )


def find_daily_digest_headers(
    headers: set[str],
    *,
    not_before: datetime,
    not_after: datetime | None = None,
    chat_id: str | None = None,
    lark_api_client=None,
    max_pages: int = 5,
) -> set[str]:
    """Find bot-authored text messages whose first line is one of ``headers``.

    Reconciliation is deliberately narrow: it reads only the configured chat,
    considers only bot/app text messages in a bounded time range, and returns
    only already-known header strings.  No unrelated chat text is persisted or
    emitted to logs.
    """

    expected_headers = {header for header in headers if isinstance(header, str)}
    if not expected_headers:
        return set()
    if not isinstance(not_before, datetime):
        raise LarkTextReconciliationUnavailable("invalid_reconciliation_range")
    if not_before.tzinfo is None:
        raise LarkTextReconciliationUnavailable("invalid_reconciliation_range")
    if not_after is None:
        not_after = datetime.now(UTC)
    if not isinstance(not_after, datetime) or not_after.tzinfo is None:
        raise LarkTextReconciliationUnavailable("invalid_reconciliation_range")
    if not isinstance(max_pages, int) or not 1 <= max_pages <= 20:
        raise LarkTextReconciliationUnavailable("invalid_reconciliation_pages")

    if lark_api_client is None:
        from src.utils.lark_app import lark_api_client as _client

        lark_api_client = _client
    resolved_chat_id = chat_id if chat_id is not None else get_settings().LARK_CHAT_ID
    if not lark_api_client or not resolved_chat_id:
        raise LarkTextReconciliationUnavailable("lark_reconciliation_unavailable")

    found: set[str] = set()
    page_token: str | None = None
    try:
        for _page in range(max_pages):
            builder = (
                ListMessageRequest.builder()
                .container_id_type("chat")
                .container_id(resolved_chat_id)
                .start_time(str(int(not_before.astimezone(UTC).timestamp())))
                .end_time(str(int(not_after.astimezone(UTC).timestamp())))
                .sort_type("ByCreateTimeAsc")
                .page_size(100)
            )
            if page_token:
                builder = builder.page_token(page_token)
            response = lark_api_client.im.v1.message.list(builder.build())
            if not response.success():
                raise LarkTextReconciliationUnavailable(
                    "lark_reconciliation_rejected"
                )
            data = getattr(response, "data", None)
            for item in tuple(getattr(data, "items", None) or ()):
                if getattr(item, "msg_type", None) != "text":
                    continue
                sender = getattr(item, "sender", None)
                if getattr(sender, "sender_type", None) not in {"app", "bot"}:
                    continue
                body = getattr(item, "body", None)
                raw_content = getattr(body, "content", None)
                if not isinstance(raw_content, str):
                    continue
                try:
                    content = json.loads(raw_content)
                except (TypeError, ValueError):
                    continue
                text = content.get("text") if isinstance(content, dict) else None
                if not isinstance(text, str):
                    continue
                header = text.split("\n", 1)[0].strip()
                if header in expected_headers:
                    found.add(header)
            if found == expected_headers:
                return found
            if not bool(getattr(data, "has_more", False)):
                return found
            next_token = getattr(data, "page_token", None)
            if not isinstance(next_token, str) or not next_token:
                raise LarkTextReconciliationUnavailable(
                    "lark_reconciliation_incomplete"
                )
            page_token = next_token
    except LarkTextReconciliationUnavailable:
        raise
    except Exception as exc:
        logger.error(
            "Lark digest reconciliation unavailable: error_type=%s",
            type(exc).__name__,
        )
        raise LarkTextReconciliationUnavailable(
            "lark_reconciliation_unavailable"
        ) from None
    # A bounded scan that still has more pages cannot prove absence.  Preserve
    # the unknown outcome rather than risking a duplicate daily report.
    raise LarkTextReconciliationUnavailable("lark_reconciliation_incomplete")
