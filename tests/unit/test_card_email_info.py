"""
Tests for card email info section (Sender/To/Cc display) and forward card improvements.

Covers:
  - _build_email_info_section produces separate Sender, To, Cc rows
  - build_approval_card includes To/Cc in email info for reply and forward
  - build_read_only_card includes To/Cc in email info
  - Forward card has proper summary, confidence, reasoning via categorizer
"""
import json
import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from src.utils.card_builder import LarkCardBuilder


def _make_builder():
    return LarkCardBuilder(lark_api_client=None, exchange_client=None)


# ---------------------------------------------------------------------------
# _build_email_info_section
# ---------------------------------------------------------------------------

class TestBuildEmailInfoSection:

    def test_sender_only_when_no_to_cc(self):
        builder = _make_builder()
        rows = builder._build_email_info_section(
            raw_sender="alice@example.com",
            to_list=[],
            cc_list=[],
            user_map={},
        )
        assert len(rows) == 1
        label_col = rows[0]["columns"][0]
        assert "发件人" in label_col["elements"][0]["text"]["content"]

    def test_sender_to_cc_all_present(self):
        builder = _make_builder()
        rows = builder._build_email_info_section(
            raw_sender="alice@example.com",
            to_list=["bob@example.com"],
            cc_list=["carol@example.com"],
            user_map={},
        )
        assert len(rows) == 3
        labels = [r["columns"][0]["elements"][0]["text"]["content"] for r in rows]
        assert "发件人" in labels[0]
        assert "收件人" in labels[1]
        assert "抄送" in labels[2]

    def test_sender_to_without_cc(self):
        builder = _make_builder()
        rows = builder._build_email_info_section(
            raw_sender="alice@example.com",
            to_list=["bob@example.com"],
            cc_list=[],
            user_map={},
        )
        assert len(rows) == 2

    @patch("src.utils.card_builder.get_settings")
    def test_sender_me_label(self, mock_settings):
        settings = MagicMock()
        settings.EXCHANGE_ACCOUNT_EMAIL = "alice@example.com"
        mock_settings.return_value = settings

        builder = _make_builder()
        rows = builder._build_email_info_section(
            raw_sender="alice@example.com",
            to_list=[],
            cc_list=[],
            user_map={},
        )
        label = rows[0]["columns"][0]["elements"][0]["text"]["content"]
        assert "(我)" in label


# ---------------------------------------------------------------------------
# build_approval_card — reply mode
# ---------------------------------------------------------------------------

class TestApprovalCardReplyWithRecipients:

    def test_card_contains_to_cc_rows(self):
        builder = _make_builder()
        card = builder.build_approval_card(
            email_id="e1",
            draft="Draft reply",
            context=[],
            email_data={
                "subject": "Hello",
                "sender": "alice@example.com",
                "to": ["bob@example.com", "dave@example.com"],
                "cc": ["carol@example.com"],
            },
            classification={"reasoning": "test", "summary": "Test email"},
        )
        elements_json = json.dumps(card["elements"], ensure_ascii=False)
        assert "收件人" in elements_json
        assert "抄送" in elements_json

    def test_card_shows_sender(self):
        builder = _make_builder()
        card = builder.build_approval_card(
            email_id="e1",
            draft="ok",
            context=[],
            email_data={
                "subject": "Test",
                "sender": "alice@example.com",
                "to": [],
                "cc": [],
            },
            classification={},
        )
        elements_json = json.dumps(card["elements"], ensure_ascii=False)
        assert "发件人" in elements_json


# ---------------------------------------------------------------------------
# build_approval_card — forward mode
# ---------------------------------------------------------------------------

class TestApprovalCardForward:

    def test_forward_card_header(self):
        builder = _make_builder()
        card = builder.build_approval_card(
            email_id="e1",
            draft="呈阅",
            context=[],
            email_data={
                "subject": "Report",
                "sender": "alice@example.com",
                "to": ["bob@example.com"],
                "cc": [],
                "draft_to": ["boss@company.com"],
                "draft_cc": [],
            },
            classification={"action": "forward", "reasoning": "系统规则自动触发转发"},
        )
        assert "拟定转发" in card["header"]["title"]["content"]

    def test_forward_card_buttons(self):
        builder = _make_builder()
        card = builder.build_approval_card(
            email_id="e1",
            draft="呈阅",
            context=[],
            email_data={
                "subject": "Report",
                "sender": "alice@example.com",
                "to": [],
                "cc": [],
                "draft_to": ["boss@company.com"],
                "draft_cc": [],
            },
            classification={"action": "forward"},
        )
        actions = [e for e in card["elements"] if e.get("tag") == "action"]
        button_texts = []
        for a in actions:
            for btn in a.get("actions", []):
                button_texts.append(btn["text"]["content"])
        assert any("批准转发" in t for t in button_texts)

    def test_forward_card_shows_original_recipients(self):
        builder = _make_builder()
        card = builder.build_approval_card(
            email_id="e1",
            draft="呈阅",
            context=[],
            email_data={
                "subject": "Report",
                "sender": "alice@example.com",
                "to": ["bob@example.com"],
                "cc": ["carol@example.com"],
                "draft_to": ["boss@company.com"],
                "draft_cc": [],
            },
            classification={"action": "forward", "reasoning": "系统规则自动触发转发"},
        )
        elements_json = json.dumps(card["elements"], ensure_ascii=False)
        assert "收件人" in elements_json
        assert "抄送" in elements_json

    def test_forward_card_draft_label(self):
        builder = _make_builder()
        card = builder.build_approval_card(
            email_id="e1",
            draft="呈阅",
            context=[],
            email_data={
                "subject": "Report",
                "sender": "alice@example.com",
                "to": [],
                "cc": [],
                "draft_to": ["boss@company.com"],
                "draft_cc": [],
            },
            classification={"action": "forward"},
        )
        elements_json = json.dumps(card["elements"], ensure_ascii=False)
        assert "拟定转发语" in elements_json
        assert "转发给" in elements_json


# ---------------------------------------------------------------------------
# build_read_only_card
# ---------------------------------------------------------------------------

class TestReadOnlyCardWithRecipients:

    def test_read_only_card_contains_to_cc(self):
        builder = _make_builder()
        card = builder.build_read_only_card(
            email_id="e1",
            context=[],
            email_data={
                "subject": "FYI",
                "sender": "alice@example.com",
                "to": ["bob@example.com"],
                "cc": ["carol@example.com"],
            },
            classification={"priority": "P1", "reasoning": "通知类", "summary": "FYI content"},
        )
        elements_json = json.dumps(card["elements"], ensure_ascii=False)
        assert "收件人" in elements_json
        assert "抄送" in elements_json

    def test_read_only_card_no_cc_when_empty(self):
        builder = _make_builder()
        card = builder.build_read_only_card(
            email_id="e1",
            context=[],
            email_data={
                "subject": "FYI",
                "sender": "alice@example.com",
                "to": ["bob@example.com"],
                "cc": [],
            },
            classification={"priority": "P1", "reasoning": "通知类"},
        )
        elements_json = json.dumps(card["elements"], ensure_ascii=False)
        assert "收件人" in elements_json
        assert "抄送" not in elements_json

    def test_read_only_card_header_purple(self):
        builder = _make_builder()
        card = builder.build_read_only_card(
            email_id="e1",
            context=[],
            email_data={"subject": "Notice", "sender": "a@b.com", "to": [], "cc": []},
            classification={"priority": "P1"},
        )
        assert card["header"]["template"] == "purple"


# ---------------------------------------------------------------------------
# Forward categorizer enrichment
# ---------------------------------------------------------------------------

class TestForwardCategorizerEnrichment:

    @pytest.mark.asyncio
    async def test_forward_gets_summary_and_confidence(self):
        from src.nodes.categorizer import categorize_email

        state = {
            "email": {"subject": "Urgent Report", "sender": "alice@example.com", "body": "content"},
            "classification": {
                "priority": "P0",
                "need_reply": True,
                "intent": "转发",
                "action": "forward",
                "reasoning": "Triggered by skill Forward to Boss",
            },
        }

        with patch("src.nodes.categorizer.get_routing_engine") as mock_engine:
            engine = MagicMock()
            engine.execute_router = AsyncMock(return_value=state)
            mock_engine.return_value = engine

            result = await categorize_email(state)

        cls = result["classification"]
        assert cls["confidence"] == 1.0
        assert "Urgent Report" in cls["summary"]
        assert cls["reasoning"] == "系统规则自动触发转发"

    @pytest.mark.asyncio
    async def test_forward_preserves_existing_summary(self):
        from src.nodes.categorizer import categorize_email

        state = {
            "email": {"subject": "Report", "sender": "a@b.com", "body": "x"},
            "classification": {
                "action": "forward",
                "reasoning": "Triggered by skill test",
                "summary": "Custom summary from skill",
            },
        }

        with patch("src.nodes.categorizer.get_routing_engine") as mock_engine:
            engine = MagicMock()
            engine.execute_router = AsyncMock(return_value=state)
            mock_engine.return_value = engine

            result = await categorize_email(state)

        assert result["classification"]["summary"] == "Custom summary from skill"
