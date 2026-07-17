"""
Lark messaging functions - extracted from lark_app.py for modularity.
Handles: send_approval_card, send_read_only_card, send_system_notification
"""
import json
import logging
from typing import List

from lark_oapi.api.im.v1 import CreateMessageRequest, CreateMessageRequestBody

from src.config import get_settings
from src.security.redaction import fingerprint_identifier

logger = logging.getLogger(__name__)


def send_approval_card(email_id: str, draft: str, context: List[dict], email_data: dict,
                       classification: dict, pdf_url: str = None,
                       routing_log: List = None, active_skills: List = None,
                       *, lark_api_client=None, card_builder=None) -> bool:
    """Send an interactive approval card. Returns True iff Lark accepted the message."""
    if lark_api_client is None:
        from src.utils.lark_app import lark_api_client as _client, card_builder as _cb
        lark_api_client = _client
        card_builder = _cb

    if not lark_api_client:
        logger.error("Lark Client not initialized. Cannot send card.")
        return False

    settings = get_settings()
    chat_id = settings.LARK_CHAT_ID
    if not chat_id:
        logger.error("LARK_CHAT_ID not configured.")
        return False

    try:
        card_content = card_builder.build_approval_card(
            email_id, draft, context, email_data, classification, pdf_url=pdf_url,
            routing_log=routing_log, active_skills=active_skills,
        )

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
            "Lark approval card send failed: error_type=%s",
            type(exc).__name__,
        )
        return False

    if not response.success():
        logger.error("Lark approval card rejected: code=%s", response.code)
        return False
    logger.info(
        "Lark approval card sent: email=%s message=%s",
        fingerprint_identifier(email_id, namespace="email"),
        fingerprint_identifier(response.data.message_id, namespace="lark_message"),
    )
    return True


def send_read_only_card(email_id: str, context: List[dict], email_data: dict,
                        classification: dict, pdf_url: str = None,
                        routing_log: List = None, active_skills: List = None,
                        *, lark_api_client=None, card_builder=None) -> bool:
    """Send a read-only Lark card. Returns True iff Lark accepted the message."""
    if lark_api_client is None:
        from src.utils.lark_app import lark_api_client as _client, card_builder as _cb
        lark_api_client = _client
        card_builder = _cb

    if not lark_api_client:
        logger.error("Lark Client not initialized. Cannot send card.")
        return False

    settings = get_settings()
    chat_id = settings.LARK_CHAT_ID
    if not chat_id:
        logger.error("LARK_CHAT_ID not configured.")
        return False

    try:
        card_content = card_builder.build_read_only_card(
            email_id, context, email_data, classification, pdf_url=pdf_url,
            routing_log=routing_log, active_skills=active_skills,
        )

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
            "Lark read-only card send failed: error_type=%s",
            type(exc).__name__,
        )
        return False

    if not response.success():
        logger.error("Lark read-only card rejected: code=%s", response.code)
        return False
    logger.info(
        "Lark read-only card sent: email=%s message=%s",
        fingerprint_identifier(email_id, namespace="email"),
        fingerprint_identifier(response.data.message_id, namespace="lark_message"),
    )
    return True


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
