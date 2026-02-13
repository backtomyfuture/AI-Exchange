"""
Lark messaging functions - extracted from lark_app.py for modularity.
Handles: send_approval_card, send_read_only_card, send_system_notification
"""
import json
import logging
from typing import List

from lark_oapi.api.im.v1 import CreateMessageRequest, CreateMessageRequestBody

from src.config import get_settings

logger = logging.getLogger(__name__)


def send_approval_card(email_id: str, draft: str, context: List[dict], email_data: dict,
                       classification: dict, pdf_url: str = None,
                       *, lark_api_client=None, card_builder=None):
    if lark_api_client is None:
        from src.utils.lark_app import lark_api_client as _client, card_builder as _cb
        lark_api_client = _client
        card_builder = _cb

    if not lark_api_client:
        logger.error("Lark Client not initialized. Cannot send card.")
        return

    settings = get_settings()
    chat_id = settings.LARK_CHAT_ID
    if not chat_id:
        logger.error("LARK_CHAT_ID not configured.")
        return

    card_content = card_builder.build_approval_card(
        email_id, draft, context, email_data, classification, pdf_url=pdf_url
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
    if not response.success():
        logger.error(f"Failed to send Lark card: {response.code} - {response.msg}")
    else:
        logger.info(f"Lark card sent for email {email_id}. Msg ID: {response.data.message_id}")


def send_read_only_card(email_id: str, context: List[dict], email_data: dict,
                        classification: dict, pdf_url: str = None,
                        *, lark_api_client=None, card_builder=None):
    if lark_api_client is None:
        from src.utils.lark_app import lark_api_client as _client, card_builder as _cb
        lark_api_client = _client
        card_builder = _cb

    if not lark_api_client:
        logger.error("Lark Client not initialized. Cannot send card.")
        return

    settings = get_settings()
    chat_id = settings.LARK_CHAT_ID
    if not chat_id:
        logger.error("LARK_CHAT_ID not configured.")
        return

    card_content = card_builder.build_read_only_card(
        email_id, context, email_data, classification, pdf_url=pdf_url
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
    if not response.success():
        logger.error(f"Failed to send read-only Lark card: {response.code} - {response.msg}")
    else:
        logger.info(f"Read-only Lark card sent for email {email_id}. Msg ID: {response.data.message_id}")


def send_system_notification(title: str, content: str, template: str = "red",
                             *, lark_api_client=None):
    if lark_api_client is None:
        from src.utils.lark_app import lark_api_client as _client
        lark_api_client = _client

    if not lark_api_client:
        logger.error("Lark Client not initialized. Cannot send system notification.")
        return

    settings = get_settings()
    chat_id = settings.LARK_CHAT_ID
    if not chat_id:
        logger.error("LARK_CHAT_ID not configured.")
        return

    card_content = {
        "header": {
            "template": template,
            "title": {"content": title, "tag": "plain_text"}
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": content}}
        ]
    }

    request = CreateMessageRequest.builder() \
        .receive_id_type("chat_id") \
        .request_body(CreateMessageRequestBody.builder()
            .receive_id(chat_id)
            .msg_type("interactive")
            .content(json.dumps(card_content))
            .build()) \
        .build()

    response = lark_api_client.im.v1.message.create(request)
    if not response.success():
        logger.error(f"Failed to send system notification: {response.code} - {response.msg}")
    else:
        logger.info(f"System notification sent: {title}")
