import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.utils.card_builder import LarkCardBuilder


@pytest.mark.asyncio
async def test_lookup_lark_users_falls_back_to_exchange_from_worker_thread():
    exchange_client = SimpleNamespace(resolve_contact=AsyncMock(return_value="联系人"))
    builder = LarkCardBuilder(
        lark_api_client=None,
        exchange_client=exchange_client,
        exchange_loop=asyncio.get_running_loop(),
    )

    result = await asyncio.to_thread(
        builder.lookup_lark_users,
        ["recipient@example.com"],
    )

    assert result == {"recipient@example.com": {"name": "联系人"}}
    exchange_client.resolve_contact.assert_awaited_once_with("recipient@example.com")


@pytest.mark.asyncio
async def test_exchange_contact_fallback_runs_on_owner_event_loop():
    owner_loop = asyncio.get_running_loop()
    observed_loops = []

    async def resolve_contact(_query):
        observed_loops.append(asyncio.get_running_loop())
        return "联系人"

    exchange_client = SimpleNamespace(resolve_contact=resolve_contact)
    builder = LarkCardBuilder(
        lark_api_client=None,
        exchange_client=exchange_client,
        exchange_loop=owner_loop,
    )

    result = await asyncio.to_thread(
        builder.lookup_lark_users,
        ["recipient@example.com"],
    )

    assert result == {"recipient@example.com": {"name": "联系人"}}
    assert observed_loops == [owner_loop]


def test_build_approval_card_uses_pdf_url_from_dict():
    builder = LarkCardBuilder(lark_api_client=None, exchange_client=None)

    card = builder.build_approval_card(
        email_id="e1",
        draft="ok",
        context=[],
        email_data={"subject": "s", "sender": "a@b.com", "to": [], "cc": []},
        classification={},
        pdf_url={"url": "https://www.feishu.cn/file/abc", "file_token": "abc"},
    )

    contents = []
    urls = []
    for el in card.get("elements", []):
        text = el.get("text")
        if isinstance(text, dict):
            content = text.get("content")
            if content:
                contents.append(content)
        for action in el.get("actions", []):
            if action.get("url"):
                urls.append(action.get("url"))

    assert any("https://www.feishu.cn/file/abc" in c for c in contents)
    assert urls == []


def test_read_only_card_links_non_inline_pdf_even_when_content_id_is_present():
    builder = LarkCardBuilder(lark_api_client=None, exchange_client=None)

    card = builder.build_read_only_card(
        email_id="e1",
        context=[],
        email_data={
            "subject": "s",
            "sender": "Unknown",
            "to": [],
            "cc": [],
            "body": "Please review the attached report.",
            "attachments": [
                {
                    "name": "report.pdf",
                    "content_id": "normal-attachment-id",
                    "is_inline": False,
                    "lark_file_url": "https://example.invalid/report",
                }
            ],
        },
        classification={"priority": "P1"},
    )

    assert "https://example.invalid/report" in json.dumps(card)


def test_approval_card_links_non_inline_pdf_even_when_content_id_is_present():
    builder = LarkCardBuilder(lark_api_client=None, exchange_client=None)

    card = builder.build_approval_card(
        email_id="e1",
        draft="Approved reply",
        context=[],
        email_data={
            "subject": "s",
            "sender": "Unknown",
            "to": [],
            "cc": [],
            "body": "Please review the attached report.",
            "attachments": [
                {
                    "name": "report.pdf",
                    "content_id": "normal-attachment-id",
                    "is_inline": False,
                    "lark_file_url": "https://example.invalid/report",
                }
            ],
        },
        classification={},
    )

    assert "https://example.invalid/report" in json.dumps(card)
