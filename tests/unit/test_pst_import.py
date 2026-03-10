"""Tests for scripts/import_pst.py — PST/Mbox/EML import pipeline."""

from __future__ import annotations

import email as email_lib
import email.policy
import mailbox
from pathlib import Path
from unittest.mock import MagicMock, patch


from scripts.import_pst import (
    ImportStats,
    ParsedEmail,
    _generate_email_id,
    _infer_folder_type,
    _parse_address_list,
    _parse_date,
    iter_from_eml,
    iter_from_eml_dir,
    iter_from_mbox,
    iter_from_pst,
    parse_email_message,
    run_import,
)


# ---------------------------------------------------------------------------
# Unit tests for helpers
# ---------------------------------------------------------------------------


class TestHelpers:
    def test_generate_email_id_deterministic(self):
        id1 = _generate_email_id("test content")
        id2 = _generate_email_id("test content")
        assert id1 == id2
        assert id1.startswith("pst_")

    def test_generate_email_id_unique(self):
        id1 = _generate_email_id("content a")
        id2 = _generate_email_id("content b")
        assert id1 != id2

    def test_parse_address_list(self):
        assert _parse_address_list("a@b.com, c@d.com") == ["a@b.com", "c@d.com"]
        assert _parse_address_list(None) == []
        assert _parse_address_list("") == []
        assert _parse_address_list("single@domain.com") == ["single@domain.com"]

    def test_parse_date_valid(self):
        result = _parse_date("Mon, 01 Jan 2024 10:00:00 +0000")
        assert "2024-01-01" in result

    def test_parse_date_invalid(self):
        result = _parse_date("not-a-date")
        assert "T" in result  # still returns an ISO datetime

    def test_parse_date_none(self):
        result = _parse_date(None)
        assert "T" in result

    def test_infer_folder_type_sent(self):
        assert _infer_folder_type("Sent Items") == "sent"
        assert _infer_folder_type("已发送邮件") == "sent"

    def test_infer_folder_type_draft(self):
        assert _infer_folder_type("Drafts") == "draft"
        assert _infer_folder_type("草稿") == "draft"

    def test_infer_folder_type_received(self):
        assert _infer_folder_type("Inbox") == "received"
        assert _infer_folder_type("收件箱") == "received"
        assert _infer_folder_type("") == "received"


# ---------------------------------------------------------------------------
# ParsedEmail
# ---------------------------------------------------------------------------


class TestParsedEmail:
    def test_to_dict(self):
        pe = ParsedEmail(
            id="test_001",
            subject="Test Subject",
            sender="sender@example.com",
            to=["recipient@example.com"],
            cc=[],
            body="Hello World",
            received_at="2024-01-01T10:00:00",
            source_folder="Inbox",
            message_type="received",
        )
        d = pe.to_dict()
        assert d["id"] == "test_001"
        assert d["subject"] == "Test Subject"
        assert d["type"] == "received"
        assert d["_import_source"] == "pst_import"
        assert d["attachments"] == []
        assert d["source_folder"] == "Inbox"


# ---------------------------------------------------------------------------
# Email message parsing
# ---------------------------------------------------------------------------


def _make_eml(subject="Test", sender="a@b.com", to="c@d.com", body="Hello"):
    """Create a minimal EML string."""
    return (
        f"From: {sender}\r\n"
        f"To: {to}\r\n"
        f"Subject: {subject}\r\n"
        f"Date: Mon, 01 Jan 2024 10:00:00 +0000\r\n"
        f"Message-ID: <test-{hash(subject)}@example.com>\r\n"
        f"\r\n"
        f"{body}\r\n"
    )


class TestParseEmailMessage:
    def test_basic_parse(self):
        raw = _make_eml().encode()
        msg = email_lib.message_from_bytes(raw, policy=email_lib.policy.default)
        result = parse_email_message(msg, folder="Inbox", raw_bytes=raw)
        assert result is not None
        assert result.subject == "Test"
        assert result.sender == "a@b.com"
        assert result.message_type == "received"

    def test_sent_folder(self):
        raw = _make_eml().encode()
        msg = email_lib.message_from_bytes(raw, policy=email_lib.policy.default)
        result = parse_email_message(msg, folder="Sent Items", raw_bytes=raw)
        assert result.message_type == "sent"

    def test_in_reply_to(self):
        eml = (
            "From: a@b.com\r\n"
            "To: c@d.com\r\n"
            "Subject: Re: Original\r\n"
            "In-Reply-To: <original@example.com>\r\n"
            "\r\n"
            "Reply body\r\n"
        )
        raw = eml.encode()
        msg = email_lib.message_from_bytes(raw, policy=email_lib.policy.default)
        result = parse_email_message(msg, folder="Inbox", raw_bytes=raw)
        assert result.in_reply_to == "<original@example.com>"
        assert result.conversation_id == "<original@example.com>"


# ---------------------------------------------------------------------------
# Source iterators
# ---------------------------------------------------------------------------


class TestEmlIterator:
    def test_iter_from_eml(self, tmp_path: Path):
        eml_content = _make_eml(subject="EML Test")
        eml_file = tmp_path / "test.eml"
        eml_file.write_text(eml_content)

        results = list(iter_from_eml(eml_file, folder="Inbox"))
        assert len(results) == 1
        assert results[0].subject == "EML Test"

    def test_iter_from_eml_dir(self, tmp_path: Path):
        for i in range(3):
            (tmp_path / f"email_{i}.eml").write_text(
                _make_eml(subject=f"Mail {i}")
            )

        results = list(iter_from_eml_dir(tmp_path))
        assert len(results) == 3


class TestMboxIterator:
    def test_iter_from_mbox(self, tmp_path: Path):
        mbox_path = tmp_path / "test.mbox"
        mbox = mailbox.mbox(str(mbox_path))

        for i in range(3):
            msg = mailbox.mboxMessage()
            msg["From"] = f"sender{i}@example.com"
            msg["To"] = "recipient@example.com"
            msg["Subject"] = f"Mbox Test {i}"
            msg["Date"] = "Mon, 01 Jan 2024 10:00:00 +0000"
            msg.set_payload(f"Body {i}")
            mbox.add(msg)
        mbox.close()

        results = list(iter_from_mbox(mbox_path, folder="Inbox"))
        assert len(results) == 3
        assert results[0].subject == "Mbox Test 0"


# ---------------------------------------------------------------------------
# Import engine
# ---------------------------------------------------------------------------


class TestRunImport:
    def test_dry_run_eml_dir(self, tmp_path: Path):
        for i in range(2):
            (tmp_path / f"email_{i}.eml").write_text(
                _make_eml(subject=f"Dry Run {i}", body=f"Body {i}")
            )

        stats = run_import(tmp_path, dry_run=True)
        assert stats.total == 2
        assert stats.imported == 2
        assert stats.points_created == 0

    @patch("src.utils.email_processor.EmailProcessor")
    def test_actual_import_mbox(self, mock_proc_cls, tmp_path: Path):
        mock_proc = MagicMock()
        mock_proc.process_batch.return_value = 5
        mock_proc_cls.return_value = mock_proc

        mbox_path = tmp_path / "test.mbox"
        mbox = mailbox.mbox(str(mbox_path))
        msg = mailbox.mboxMessage()
        msg["From"] = "sender@example.com"
        msg["To"] = "recipient@example.com"
        msg["Subject"] = "Import Test"
        msg["Date"] = "Mon, 01 Jan 2024 10:00:00 +0000"
        msg.set_payload("Hello import")
        mbox.add(msg)
        mbox.close()

        stats = run_import(mbox_path, batch_size=10)
        assert stats.total == 1
        assert stats.imported == 1
        mock_proc.process_batch.assert_called_once()

    def test_empty_body_skipped(self, tmp_path: Path):
        eml = (
            "From: a@b.com\r\n"
            "To: c@d.com\r\n"
            "Subject: Empty\r\n"
            "\r\n"
            "\r\n"
        )
        (tmp_path / "empty.eml").write_text(eml)
        stats = run_import(tmp_path, dry_run=True)
        assert stats.skipped >= 1


class TestPypffParser:
    """Tests for the pypff-based PST parsing path."""

    def test_pypff_message_to_email_basic(self):
        """Test converting a mock pypff message to ParsedEmail."""
        from scripts.import_pst import _pypff_message_to_email

        msg = MagicMock()
        msg.subject = "Test Subject"
        msg.sender_name = "Alice"
        msg.sender_email_address = "alice@corp.com"
        msg.plain_text_body = "Hello world"
        msg.html_body = None
        msg.delivery_time = None
        msg.client_submit_time = None
        msg.transport_headers = (
            "To: bob@corp.com\r\n"
            "Cc: carol@corp.com\r\n"
            "In-Reply-To: <original@corp.com>\r\n"
        )
        msg.number_of_attachments = 0

        result = _pypff_message_to_email(msg, "Inbox", "received")
        assert result is not None
        assert result.subject == "Test Subject"
        assert "alice@corp.com" in result.sender
        assert result.to == ["bob@corp.com"]
        assert result.cc == ["carol@corp.com"]
        assert result.in_reply_to == "<original@corp.com>"
        assert result.message_type == "received"

    def test_pypff_message_to_email_with_attachments(self):
        from scripts.import_pst import _pypff_message_to_email

        att = MagicMock()
        att.name = "report.pdf"
        att.size = 12345

        msg = MagicMock()
        msg.subject = "With Attachment"
        msg.sender_name = "Bob"
        msg.sender_email_address = "bob@corp.com"
        msg.plain_text_body = "See attached"
        msg.html_body = None
        msg.delivery_time = None
        msg.client_submit_time = None
        msg.transport_headers = ""
        msg.number_of_attachments = 1
        msg.get_attachment.return_value = att

        result = _pypff_message_to_email(msg, "Inbox", "received")
        assert result is not None
        assert len(result.attachments_metadata) == 1
        assert result.attachments_metadata[0]["name"] == "report.pdf"

    def test_pypff_message_bytes_body(self):
        from scripts.import_pst import _pypff_message_to_email

        msg = MagicMock()
        msg.subject = "Bytes Body"
        msg.sender_name = ""
        msg.sender_email_address = "test@corp.com"
        msg.plain_text_body = b"\xe4\xbd\xa0\xe5\xa5\xbd"  # "你好" in UTF-8
        msg.html_body = None
        msg.delivery_time = None
        msg.client_submit_time = None
        msg.transport_headers = None
        msg.number_of_attachments = 0

        result = _pypff_message_to_email(msg, "Inbox", "received")
        assert result is not None
        assert "你好" in result.body

    def test_iter_from_pst_uses_pypff_when_available(self):
        """Verify iter_from_pst prefers pypff over readpst."""
        with patch("scripts.import_pst._iter_from_pst_pypff") as mock_pypff:
            mock_pypff.return_value = iter([])
            list(iter_from_pst(Path("/fake.pst")))
            mock_pypff.assert_called_once()


class TestImportStats:
    def test_print_summary(self, capsys):
        stats = ImportStats(total=10, imported=8, skipped=1, failed=1, points_created=24)
        stats.print_summary()
        output = capsys.readouterr().out
        assert "10" in output
        assert "8" in output
        assert "24" in output


# ---------------------------------------------------------------------------
# Exchange import
# ---------------------------------------------------------------------------


class TestExchangeImport:
    """Tests for Exchange server email import."""

    def test_exchange_item_to_parsed_email_basic(self):
        from scripts.import_pst import _exchange_item_to_parsed_email

        item = {
            "id": "AAMkAGQ3YzEwNDM=",
            "subject": "Q4 报表审批",
            "sender": "finance@corp.com",
            "to": ["boss@corp.com"],
            "cc": ["cfo@corp.com"],
            "body": "<html><body>请审批</body></html>",
            "received_at": "2024-06-15T10:30:00+08:00",
            "in_reply_to": "",
        }

        result = _exchange_item_to_parsed_email(item, folder="INBOX")
        assert result is not None
        assert result.subject == "Q4 报表审批"
        assert result.sender == "finance@corp.com"
        assert result.to == ["boss@corp.com"]
        assert result.cc == ["cfo@corp.com"]
        assert result.message_type == "received"
        assert result.id.startswith("exc_")
        assert result.import_source == "exchange_import"

    def test_exchange_item_to_parsed_email_sent_folder(self):
        from scripts.import_pst import _exchange_item_to_parsed_email

        item = {
            "id": "AAMkSent123",
            "subject": "Re: 项目进度",
            "sender": "me@corp.com",
            "to": ["pm@corp.com"],
            "body": "已完成",
            "received_at": "2024-06-15T11:00:00+08:00",
        }

        result = _exchange_item_to_parsed_email(item, folder="Sent Items")
        assert result is not None
        assert result.message_type == "sent"

    def test_exchange_item_to_parsed_email_string_addresses(self):
        from scripts.import_pst import _exchange_item_to_parsed_email

        item = {
            "id": "AAMkStr123",
            "subject": "Test",
            "sender": "a@b.com",
            "to": "x@y.com, z@w.com",
            "cc": "",
            "body": "Hello",
        }

        result = _exchange_item_to_parsed_email(item, folder="INBOX")
        assert result is not None
        assert result.to == ["x@y.com", "z@w.com"]
        assert result.cc == []

    def test_exchange_item_to_parsed_email_with_attachments(self):
        from scripts.import_pst import _exchange_item_to_parsed_email

        item = {
            "id": "AAMkAtt123",
            "subject": "附件测试",
            "sender": "a@b.com",
            "to": ["c@d.com"],
            "body": "请查收",
            "attachments": [
                {"name": "report.xlsx", "content_type": "application/vnd.ms-excel", "size": 54321},
            ],
        }

        result = _exchange_item_to_parsed_email(item, folder="INBOX")
        assert result is not None
        assert len(result.attachments_metadata) == 1
        assert result.attachments_metadata[0]["name"] == "report.xlsx"

    def test_exchange_item_to_parsed_email_missing_fields(self):
        """Minimal item with only id and body should still parse."""
        from scripts.import_pst import _exchange_item_to_parsed_email

        item = {"id": "MinimalID", "body": "Just body"}
        result = _exchange_item_to_parsed_email(item, folder="INBOX")
        assert result is not None
        assert result.subject == "(无主题)"
        assert result.sender == "unknown"

    def test_exchange_item_to_dict_import_source(self):
        from scripts.import_pst import _exchange_item_to_parsed_email

        item = {"id": "TestSource", "subject": "Source Test", "body": "body"}
        result = _exchange_item_to_parsed_email(item, folder="INBOX")
        d = result.to_dict()
        assert d["_import_source"] == "exchange_import"

    def test_exchange_dry_run(self, tmp_path: Path):
        """Verify dry-run with exchange source via mocked iter_from_exchange."""
        from scripts.import_pst import _exchange_item_to_parsed_email

        # Simulate what iter_from_exchange would produce
        items = [
            {"id": f"ID_{i}", "subject": f"Mail {i}", "sender": "s@c.com",
             "to": ["r@c.com"], "body": f"Body {i}"}
            for i in range(3)
        ]
        parsed = [_exchange_item_to_parsed_email(it, folder="INBOX") for it in items]

        with patch("scripts.import_pst.iter_from_exchange", return_value=iter(parsed)):
            stats = run_import(
                source=None,
                dry_run=True,
                source_type="exchange",
                exchange_folder="INBOX",
                exchange_limit=10,
            )
            assert stats.total == 3
            assert stats.imported == 3
            assert stats.points_created == 0

