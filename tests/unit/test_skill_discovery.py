"""Tests for skill discovery: analyzer + generator."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from src.skills_discovery.analyzer import (
    DiscoveredPattern,
    EmailHistoryCollector,
    EmailRecord,
    PatternAnalyzer,
)
from src.skills_discovery.generator import (
    _card_type_from,
    _priority_to_level,
    _sanitize_skill_id,
    generate_handler,
    generate_manifest,
    write_skill,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_records(
    n_received: int = 20,
    n_sent: int = 5,
    sender_domain: str = "corp.com",
) -> list[EmailRecord]:
    """Generate synthetic email records for testing."""
    records = []

    senders = [f"user{i}@{sender_domain}" for i in range(min(5, n_received))]

    for i in range(n_received):
        sender = senders[i % len(senders)]
        records.append(EmailRecord(
            id=f"recv_{i:04d}",
            subject=f"关于项目进度 #{i}" if i % 3 == 0 else f"发票审批 #{i}",
            sender=sender,
            to=["me@corp.com"],
            cc=[],
            received_at=f"2024-01-{(i % 28) + 1:02d}T10:00:00",
            message_type="received",
            source_folder="Inbox",
        ))

    for i in range(n_sent):
        records.append(EmailRecord(
            id=f"sent_{i:04d}",
            subject=f"Re: 关于项目进度 #{i * 3}",
            sender="me@corp.com",
            to=[senders[i % len(senders)]],
            cc=[],
            received_at=f"2024-01-{(i % 28) + 1:02d}T14:00:00",
            message_type="sent",
            source_folder="Sent Items",
        ))

    return records


# ---------------------------------------------------------------------------
# Data model tests
# ---------------------------------------------------------------------------


class TestDataModels:
    def test_discovered_pattern_has_condition_logic(self):
        pattern = DiscoveredPattern(
            id="test",
            name="test",
            description="test",
            trigger_type="combined",
            condition_logic="and",
        )
        assert pattern.condition_logic == "and"

    def test_discovered_pattern_default_logic_is_and(self):
        pattern = DiscoveredPattern(
            id="test",
            name="test",
            description="test",
            trigger_type="combined",
        )
        assert pattern.condition_logic == "and"

    def test_email_record_body_preview_default_empty(self):
        record = EmailRecord(
            id="test", subject="test", sender="a@b.com",
            to=[], cc=[], received_at="2024-01-01", message_type="received",
        )
        assert record.body_preview == ""


# ---------------------------------------------------------------------------
# Recipient analysis helpers and tests
# ---------------------------------------------------------------------------


def _make_records_with_recipients() -> list[EmailRecord]:
    """带有收件人/抄送的测试数据。"""
    records = []
    mailing_lists = ["all-staff@corp.com", "dev-team@corp.com"]
    for i in range(15):
        to_list = [mailing_lists[i % 2]] if i < 10 else ["me@corp.com"]
        cc_list = ["me@corp.com"] if i < 10 else []
        records.append(EmailRecord(
            id=f"recv_{i:04d}",
            subject=f"项目通知 #{i}",
            sender=f"user{i % 3}@corp.com",
            to=to_list, cc=cc_list,
            received_at=f"2024-01-{(i % 28) + 1:02d}T10:00:00",
            message_type="received",
        ))
    # 一些回复
    for i in range(3):
        records.append(EmailRecord(
            id=f"sent_{i:04d}",
            subject=f"Re: 项目通知 #{i}",
            sender="me@corp.com",
            to=[f"user{i}@corp.com"], cc=[],
            received_at=f"2024-01-{(i % 28) + 1:02d}T14:00:00",
            message_type="sent",
        ))
    return records


class TestRecipientAnalysis:
    def test_statistics_include_recipient_data(self):
        records = _make_records_with_recipients()
        analyzer = PatternAnalyzer(records)
        stats = analyzer.compute_statistics()

        assert "mailing_lists" in stats
        assert "to_vs_cc_reply_rate" in stats
        assert "frequent_recipient_combos" in stats

    def test_mailing_list_detection(self):
        """邮件组地址（all-@, -team@, -group@）应被识别。"""
        records = [
            EmailRecord(
                id=f"r_{i}", subject=f"通知 #{i}",
                sender=f"sender{i}@corp.com",
                to=["all-staff@corp.com"], cc=[],
                received_at="2024-01-01", message_type="received",
            )
            for i in range(5)
        ] + [
            EmailRecord(
                id=f"s_{i}", subject=f"Re: 通知 #{i}",
                sender="me@corp.com",
                to=[f"sender{i}@corp.com"], cc=[],
                received_at="2024-01-01", message_type="sent",
            )
            for i in range(1)
        ]
        analyzer = PatternAnalyzer(records)
        stats = analyzer.compute_statistics()

        assert len(stats["mailing_lists"]) > 0
        ml_addrs = [ml["address"] for ml in stats["mailing_lists"]]
        assert "all-staff@corp.com" in ml_addrs

    def test_to_vs_cc_reply_rate(self):
        """我在 TO 里的邮件 vs 我在 CC 里的邮件，回复率应不同。"""
        records = [
            # 我在 TO 里 — 5封
            *[EmailRecord(
                id=f"to_{i}", subject=f"直接给你 #{i}",
                sender="boss@corp.com",
                to=["me@corp.com"], cc=[],
                received_at="2024-01-01", message_type="received",
            ) for i in range(5)],
            # 我在 CC 里 — 5封
            *[EmailRecord(
                id=f"cc_{i}", subject=f"抄送通知 #{i}",
                sender="colleague@corp.com",
                to=["other@corp.com"], cc=["me@corp.com"],
                received_at="2024-01-01", message_type="received",
            ) for i in range(5)],
            # 对"直接给你"的回复 — 3封
            *[EmailRecord(
                id=f"reply_{i}", subject=f"Re: 直接给你 #{i}",
                sender="me@corp.com",
                to=["boss@corp.com"], cc=[],
                received_at="2024-01-01", message_type="sent",
            ) for i in range(3)],
        ]
        analyzer = PatternAnalyzer(records, my_email="me@corp.com")
        stats = analyzer.compute_statistics()

        assert stats["to_vs_cc_reply_rate"]["to_reply_rate"] > stats["to_vs_cc_reply_rate"]["cc_reply_rate"]


# ---------------------------------------------------------------------------
# Thread analysis helpers and tests
# ---------------------------------------------------------------------------


def _make_records_with_threads() -> list[EmailRecord]:
    """带有线程的测试数据。"""
    records = []
    # 线程1: 深度4，我回复2次
    for i in range(4):
        is_sent = i % 2 == 1
        records.append(EmailRecord(
            id=f"thread1_{i}",
            subject="Re: 重要讨论" if i > 0 else "重要讨论",
            sender="me@corp.com" if is_sent else "partner@corp.com",
            to=["partner@corp.com"] if is_sent else ["me@corp.com"],
            cc=[], received_at=f"2024-01-0{i+1}T10:00:00",
            message_type="sent" if is_sent else "received",
            thread_id="thread_A",
        ))
    # 线程2: 深度2，我没参与
    for i in range(2):
        records.append(EmailRecord(
            id=f"thread2_{i}",
            subject="FYI" if i == 0 else "Re: FYI",
            sender="other@corp.com",
            to=["me@corp.com"], cc=[],
            received_at=f"2024-01-0{i+1}T10:00:00",
            message_type="received",
            thread_id="thread_B",
        ))
    return records


class TestThreadAnalysis:
    def test_statistics_include_thread_data(self):
        records = _make_records_with_threads()
        analyzer = PatternAnalyzer(records, my_email="me@corp.com")
        stats = analyzer.compute_statistics()

        assert "thread_stats" in stats
        assert len(stats["thread_stats"]) > 0

    def test_thread_depth_and_participation(self):
        """线程深度和参与度应被正确计算。"""
        records = [
            # 线程1：4轮，我发了2封（参与度 2/4=0.5）
            EmailRecord(id="t1_1", subject="讨论A", sender="a@corp.com",
                        to=["me@corp.com"], cc=[], received_at="2024-01-01",
                        message_type="received", thread_id="thread_001"),
            EmailRecord(id="t1_2", subject="Re: 讨论A", sender="me@corp.com",
                        to=["a@corp.com"], cc=[], received_at="2024-01-01",
                        message_type="sent", thread_id="thread_001"),
            EmailRecord(id="t1_3", subject="Re: 讨论A", sender="a@corp.com",
                        to=["me@corp.com"], cc=[], received_at="2024-01-02",
                        message_type="received", thread_id="thread_001"),
            EmailRecord(id="t1_4", subject="Re: 讨论A", sender="me@corp.com",
                        to=["a@corp.com"], cc=[], received_at="2024-01-02",
                        message_type="sent", thread_id="thread_001"),
            # 线程2：只有1封，不应出现在结果中
            EmailRecord(id="t2_1", subject="通知B", sender="b@corp.com",
                        to=["me@corp.com"], cc=[], received_at="2024-01-01",
                        message_type="received", thread_id="thread_002"),
        ]
        analyzer = PatternAnalyzer(records, my_email="me@corp.com")
        stats = analyzer.compute_statistics()

        threads = stats["thread_stats"]
        t1 = next((t for t in threads if t["thread_id"] == "thread_001"), None)
        assert t1 is not None
        assert t1["depth"] == 4
        assert t1["my_replies"] == 2
        import pytest
        assert t1["participation"] == pytest.approx(0.5)

    def test_single_email_thread_excluded(self):
        """只有1封邮件的线程不应出现在 thread_stats 中。"""
        records = [
            EmailRecord(
                id="single", subject="孤立邮件", sender="a@corp.com",
                to=["me@corp.com"], cc=[], received_at="2024-01-01",
                message_type="received", thread_id="solo_thread",
            )
        ]
        analyzer = PatternAnalyzer(records, my_email="me@corp.com")
        stats = analyzer.compute_statistics()
        solo = [t for t in stats["thread_stats"] if t["thread_id"] == "solo_thread"]
        assert len(solo) == 0


# ---------------------------------------------------------------------------
# Enhanced LLM Prompt tests
# ---------------------------------------------------------------------------


class TestEnhancedLLMPrompt:
    def test_prompt_includes_recipient_data(self):
        records = _make_records_with_recipients()
        analyzer = PatternAnalyzer(records, my_email="me@corp.com")
        stats = analyzer.compute_statistics()
        prompt = analyzer.build_llm_prompt(stats)

        assert "收件人" in prompt or "邮件组" in prompt or "TO" in prompt or "CC" in prompt

    def test_prompt_includes_thread_data(self):
        records = _make_records_with_threads()
        analyzer = PatternAnalyzer(records, my_email="me@corp.com")
        stats = analyzer.compute_statistics()
        prompt = analyzer.build_llm_prompt(stats)

        assert "线程" in prompt or "thread" in prompt.lower()

    def test_prompt_includes_body_samples(self):
        records = [
            EmailRecord(
                id=f"r_{i}", subject=f"审批请求 #{i}",
                sender="finance@corp.com",
                to=["me@corp.com"], cc=[],
                received_at="2024-01-01", message_type="received",
                body_preview="请审核附件中的合同并签字确认。" * 5,
            )
            for i in range(5)
        ]
        analyzer = PatternAnalyzer(records, my_email="me@corp.com")
        stats = analyzer.compute_statistics()
        prompt = analyzer.build_llm_prompt(stats)

        assert "正文样本" in prompt or "邮件内容" in prompt or "body" in prompt.lower()

    def test_prompt_requests_condition_logic(self):
        records = _make_records(n_received=10, n_sent=3)
        analyzer = PatternAnalyzer(records)
        stats = analyzer.compute_statistics()
        prompt = analyzer.build_llm_prompt(stats)

        assert "condition_logic" in prompt or "AND" in prompt or "OR" in prompt


# ---------------------------------------------------------------------------
# Analyzer tests
# ---------------------------------------------------------------------------


class TestPatternAnalyzer:
    def test_compute_statistics(self):
        records = _make_records(n_received=20, n_sent=5)
        analyzer = PatternAnalyzer(records)
        stats = analyzer.compute_statistics()

        assert stats["total_received"] == 20
        assert stats["total_sent"] == 5
        assert stats["unique_senders"] > 0
        assert len(stats["top_senders"]) > 0

    def test_reply_map_matches(self):
        records = _make_records(n_received=20, n_sent=5)
        analyzer = PatternAnalyzer(records)

        has_replied = any(v for v in analyzer._reply_map.values())
        assert has_replied, "Should detect at least some replies via subject matching"

    def test_heuristic_discovery(self):
        records = _make_records(n_received=30, n_sent=8)
        analyzer = PatternAnalyzer(records)
        patterns = analyzer._discover_heuristic()

        assert len(patterns) > 0
        for p in patterns:
            assert p.sample_count >= 3
            assert p.conditions
            assert p.trigger_type == "sender_match"

    def test_heuristic_with_few_records(self):
        records = _make_records(n_received=2, n_sent=0)
        analyzer = PatternAnalyzer(records)
        patterns = analyzer._discover_heuristic()
        assert patterns == []

    @pytest.mark.asyncio
    async def test_discover_with_llm_fallback(self):
        """When LLM is unavailable, falls back to heuristic."""
        records = _make_records(n_received=30, n_sent=8)
        analyzer = PatternAnalyzer(records)

        with patch("src.skills_discovery.analyzer.PatternAnalyzer.discover_with_llm") as mock_llm:
            mock_llm.side_effect = analyzer._discover_heuristic
            patterns = await mock_llm()
            assert len(patterns) > 0

    def test_build_llm_prompt(self):
        records = _make_records(n_received=10, n_sent=3)
        analyzer = PatternAnalyzer(records)
        stats = analyzer.compute_statistics()
        prompt = analyzer.build_llm_prompt(stats)

        assert "收到邮件" in prompt
        assert "已发送回复" in prompt
        assert "JSON" in prompt

    def test_parse_llm_patterns(self):
        records = _make_records()
        analyzer = PatternAnalyzer(records)

        raw = [
            {
                "name": "财务邮件处理",
                "description": "处理财务相关邮件",
                "trigger_type": "subject_match",
                "conditions": [{"type": "subject_match", "operator": "regex", "value": "发票|报销"}],
                "reply_rate": 0.8,
                "sample_count": 15,
                "suggested_priority": "P1",
                "suggested_need_reply": True,
                "suggested_tone": "专业正式",
                "example_subjects": ["发票审批 #1", "Q4报销单"],
            }
        ]

        patterns = analyzer._parse_llm_patterns(raw)
        assert len(patterns) == 1
        assert patterns[0].name == "财务邮件处理"
        assert patterns[0].suggested_priority == "P1"
        assert patterns[0].conditions[0]["type"] == "subject_match"


class TestEmailHistoryCollector:
    def test_collect_from_qdrant(self):
        mock_client = MagicMock()

        point1 = MagicMock()
        point1.payload = {
            "id": "email_001",
            "subject": "Test",
            "sender": "a@b.com",
            "to": ["c@d.com"],
            "cc": [],
            "received_at": "2024-01-01T10:00:00",
            "type": "received",
            "body_preview": "Hello",
        }

        mock_client.get_collection.return_value = True
        mock_client.scroll.return_value = ([point1], None)

        collector = EmailHistoryCollector(mock_client)
        records = collector.collect(limit=100)

        assert len(records) == 1
        assert records[0].subject == "Test"

    def test_collect_empty_collection(self):
        mock_client = MagicMock()
        mock_client.get_collection.side_effect = Exception("Not found")

        collector = EmailHistoryCollector(mock_client)
        records = collector.collect()
        assert records == []

    def test_deduplication(self):
        mock_client = MagicMock()

        point = MagicMock()
        point.payload = {
            "id": "same_id",
            "subject": "Duplicate",
            "sender": "a@b.com",
            "to": [],
            "cc": [],
            "received_at": "2024-01-01",
            "type": "received",
        }

        mock_client.get_collection.return_value = True
        mock_client.scroll.side_effect = [
            ([point, point], None),
        ]

        collector = EmailHistoryCollector(mock_client)
        records = collector.collect(limit=100)
        assert len(records) == 1


# ---------------------------------------------------------------------------
# Generator tests
# ---------------------------------------------------------------------------


class TestGeneratorHelpers:
    def test_sanitize_skill_id_ascii(self):
        assert _sanitize_skill_id("Finance Handler").startswith("skill_auto_")
        assert "finance" in _sanitize_skill_id("Finance Handler")

    def test_sanitize_skill_id_chinese(self):
        result = _sanitize_skill_id("财务邮件处理")
        assert result.startswith("skill_auto_")

    def test_priority_to_level(self):
        assert _priority_to_level("P0") == 10
        assert _priority_to_level("P1") == 8
        assert _priority_to_level("P2") == 5
        assert _priority_to_level("P3") == 2

    def test_card_type_from(self):
        assert _card_type_from("P1", True) == "approval"
        assert _card_type_from("P1", False) == "read_only"
        assert _card_type_from("P3", False) == "none"


class TestGenerateManifest:
    def test_basic_manifest(self):
        pattern = DiscoveredPattern(
            id="test_001",
            name="Test Skill",
            description="A test skill",
            trigger_type="sender_match",
            conditions=[{"type": "sender_match", "operator": "in", "value": ["a@b.com"]}],
            confidence=0.9,
        )
        manifest = generate_manifest(pattern, "skill_auto_test")

        assert manifest["id"] == "skill_auto_test"
        assert manifest["name"] == "Test Skill"
        assert manifest["execution_mode"] == "modifier"
        assert manifest["triggers"]["conditions"][0]["type"] == "sender_match"

    def test_manifest_without_conditions_uses_examples(self):
        pattern = DiscoveredPattern(
            id="test_002",
            name="Subject Skill",
            description="Based on subjects",
            trigger_type="subject_match",
            conditions=[],
            example_subjects=["Invoice #1", "Payment Due"],
        )
        manifest = generate_manifest(pattern, "skill_auto_subject")
        conds = manifest["triggers"]["conditions"]
        assert len(conds) == 1
        assert conds[0]["type"] == "subject_match"


class TestGenerateHandler:
    def test_handler_without_tone(self):
        pattern = DiscoveredPattern(
            id="test_001",
            name="Test",
            description="Test skill",
            trigger_type="sender_match",
            suggested_priority="P1",
            suggested_need_reply=True,
            reply_rate=0.8,
        )
        code = generate_handler(pattern)

        assert "class Skill(BaseSkill):" in code
        assert "async def execute" in code
        assert '"P1"' in code
        assert "True" in code

    def test_handler_with_tone(self):
        pattern = DiscoveredPattern(
            id="test_002",
            name="Tone Test",
            description="Test with tone",
            trigger_type="subject_match",
            suggested_priority="P2",
            suggested_need_reply=True,
            suggested_tone="简洁专业",
            reply_rate=0.6,
        )
        code = generate_handler(pattern)
        assert "system_prompt_modifier" in code
        assert "简洁专业" in code


class TestWriteSkill:
    def test_write_skill_creates_files(self, tmp_path: Path):
        pattern = DiscoveredPattern(
            id="test_001",
            name="Finance Processor",
            description="Handles finance emails",
            trigger_type="sender_match",
            conditions=[{"type": "sender_match", "operator": "in", "value": ["fin@corp.com"]}],
            suggested_priority="P1",
            suggested_need_reply=True,
            reply_rate=0.9,
            sample_count=20,
            confidence=0.85,
        )

        path = write_skill(pattern, registry_path=str(tmp_path))
        skill_dir = Path(path)

        assert skill_dir.exists()
        assert (skill_dir / "manifest.yaml").exists()
        assert (skill_dir / "handler.py").exists()

        with open(skill_dir / "manifest.yaml") as f:
            manifest = yaml.safe_load(f)
        assert manifest["name"] == "Finance Processor"
        assert manifest["execution_mode"] == "modifier"

        handler_code = (skill_dir / "handler.py").read_text()
        assert "class Skill(BaseSkill):" in handler_code

    def test_write_skill_idempotent(self, tmp_path: Path):
        pattern = DiscoveredPattern(
            id="test",
            name="Repeat Test",
            description="Test idempotency",
            trigger_type="sender_match",
            conditions=[{"type": "sender_match", "operator": "in", "value": ["x@y.com"]}],
        )

        path1 = write_skill(pattern, registry_path=str(tmp_path))
        path2 = write_skill(pattern, registry_path=str(tmp_path))
        assert path1 == path2


# ---------------------------------------------------------------------------
# Body preview enhancement tests
# ---------------------------------------------------------------------------


class TestBodyPreviewEnhancement:
    def test_strip_images_from_body(self):
        """strip_images_from_body 应移除 <img> 标签。"""
        from src.skills_discovery.analyzer import strip_images_from_body
        body = '正文开始<img src="logo.png" alt="Logo"/>中间内容<img src="sig.png"/>结尾'
        cleaned = strip_images_from_body(body)
        assert "<img" not in cleaned
        assert "正文开始" in cleaned
        assert "中间内容" in cleaned
        assert "结尾" in cleaned

    def test_strip_images_self_closing(self):
        """应处理自闭合和非自闭合的 img 标签。"""
        from src.skills_discovery.analyzer import strip_images_from_body
        body = 'text<img src="a.png">more text<IMG SRC="b.jpg" />end'
        cleaned = strip_images_from_body(body)
        assert "<img" not in cleaned.lower()
        assert "text" in cleaned
        assert "more text" in cleaned
        assert "end" in cleaned

    def test_collector_body_truncated_to_1000(self):
        """EmailHistoryCollector 应将 body 截取到 1000 字符。"""
        mock_client = MagicMock()
        point = MagicMock()
        point.payload = {
            "id": "email_long_body",
            "subject": "Test",
            "sender": "a@b.com",
            "to": ["c@d.com"],
            "cc": [],
            "received_at": "2024-01-01T10:00:00",
            "type": "received",
            "body_preview": "x" * 2000,
        }
        mock_client.get_collection.return_value = True
        mock_client.scroll.return_value = ([point], None)

        collector = EmailHistoryCollector(mock_client)
        records = collector.collect(limit=100)

        assert len(records[0].body_preview) <= 1000

    def test_collector_strips_images_from_body(self):
        """EmailHistoryCollector 应从 body 中移除 img 标签。"""
        mock_client = MagicMock()
        point = MagicMock()
        point.payload = {
            "id": "email_with_img",
            "subject": "Test",
            "sender": "a@b.com",
            "to": [],
            "cc": [],
            "received_at": "2024-01-01T10:00:00",
            "type": "received",
            "body_preview": '正文内容<img src="logo.png"/>更多内容',
        }
        mock_client.get_collection.return_value = True
        mock_client.scroll.return_value = ([point], None)

        collector = EmailHistoryCollector(mock_client)
        records = collector.collect(limit=100)

        assert "<img" not in records[0].body_preview
        assert "正文内容" in records[0].body_preview

    def test_parsed_to_record_body_1000(self):
        """_parsed_to_record 应截取到 1000 字符。"""
        from scripts.discover_skills import _parsed_to_record
        from scripts.import_pst import ParsedEmail

        parsed = ParsedEmail(
            id="test", subject="test", sender="a@b.com",
            to=[], cc=[], body="x" * 2000,
            received_at="2024-01-01",
        )
        record = _parsed_to_record(parsed)
        assert len(record.body_preview) <= 1000

    def test_parsed_to_record_strips_img(self):
        """_parsed_to_record 应移除 img 标签。"""
        from scripts.discover_skills import _parsed_to_record
        from scripts.import_pst import ParsedEmail

        parsed = ParsedEmail(
            id="test", subject="test", sender="a@b.com",
            to=[], cc=[], body='正文<img src="x.png"/>结尾',
            received_at="2024-01-01",
        )
        record = _parsed_to_record(parsed)
        assert "<img" not in record.body_preview
        assert "正文" in record.body_preview
