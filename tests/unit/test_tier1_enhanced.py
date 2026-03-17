"""Tests for Tier1 Reflex Router enhancements: cc_match and condition_logic OR."""

from __future__ import annotations

from unittest.mock import patch

from src.router.tier1_reflex import Tier1ReflexRouter


class TestTier1CcMatch:
    @patch("src.router.tier1_reflex.get_skill_manager")
    def test_cc_match_contains(self, mock_mgr):
        """cc_match 条件应匹配 CC 列表中的地址。"""
        mock_mgr.return_value.get_tier1_triggers.return_value = [{
            "skill_id": "skill_cc_test",
            "conditions": [{"type": "cc_match", "operator": "contains", "value": "me@corp.com"}],
        }]
        router = Tier1ReflexRouter()
        email = {
            "subject": "FYI", "sender": "other@corp.com", "body": "",
            "to": ["boss@corp.com"], "cc": ["me@corp.com", "team@corp.com"],
        }
        matches = router.route(email)
        assert "skill_cc_test" in matches

    @patch("src.router.tier1_reflex.get_skill_manager")
    def test_cc_match_not_in_cc(self, mock_mgr):
        """地址在 TO 而非 CC 中时，cc_match 不应匹配。"""
        mock_mgr.return_value.get_tier1_triggers.return_value = [{
            "skill_id": "skill_cc_test",
            "conditions": [{"type": "cc_match", "operator": "contains", "value": "me@corp.com"}],
        }]
        router = Tier1ReflexRouter()
        email = {
            "subject": "Direct", "sender": "other@corp.com", "body": "",
            "to": ["me@corp.com"], "cc": [],
        }
        matches = router.route(email)
        assert "skill_cc_test" not in matches

    @patch("src.router.tier1_reflex.get_skill_manager")
    def test_cc_match_in_operator(self, mock_mgr):
        """cc_match 的 in 操作符应检查 value 列表中是否有地址在 CC 中。"""
        mock_mgr.return_value.get_tier1_triggers.return_value = [{
            "skill_id": "skill_cc_list",
            "conditions": [{"type": "cc_match", "operator": "in", "value": ["me@corp.com", "boss@corp.com"]}],
        }]
        router = Tier1ReflexRouter()
        email = {
            "subject": "Test", "sender": "other@corp.com", "body": "",
            "to": ["other@corp.com"], "cc": ["me@corp.com"],
        }
        matches = router.route(email)
        assert "skill_cc_list" in matches


class TestTier1ConditionLogic:
    @patch("src.router.tier1_reflex.get_skill_manager")
    def test_or_logic_matches_first_condition(self, mock_mgr):
        """condition_logic=or 时，第一个条件匹配即可触发。"""
        mock_mgr.return_value.get_tier1_triggers.return_value = [{
            "skill_id": "skill_or_test",
            "condition_logic": "or",
            "conditions": [
                {"type": "sender_match", "operator": "contains", "value": "boss@"},
                {"type": "subject_match", "operator": "contains", "value": "紧急"},
            ],
        }]
        router = Tier1ReflexRouter()
        email = {"subject": "普通邮件", "sender": "boss@corp.com", "body": "", "to": [], "cc": []}
        assert "skill_or_test" in router.route(email)

    @patch("src.router.tier1_reflex.get_skill_manager")
    def test_or_logic_matches_second_condition(self, mock_mgr):
        """condition_logic=or 时，第二个条件匹配也可触发。"""
        mock_mgr.return_value.get_tier1_triggers.return_value = [{
            "skill_id": "skill_or_test",
            "condition_logic": "or",
            "conditions": [
                {"type": "sender_match", "operator": "contains", "value": "boss@"},
                {"type": "subject_match", "operator": "contains", "value": "紧急"},
            ],
        }]
        router = Tier1ReflexRouter()
        email = {"subject": "紧急通知", "sender": "nobody@corp.com", "body": "", "to": [], "cc": []}
        assert "skill_or_test" in router.route(email)

    @patch("src.router.tier1_reflex.get_skill_manager")
    def test_or_logic_no_match(self, mock_mgr):
        """condition_logic=or 时，所有条件都不满足则不触发。"""
        mock_mgr.return_value.get_tier1_triggers.return_value = [{
            "skill_id": "skill_or_test",
            "condition_logic": "or",
            "conditions": [
                {"type": "sender_match", "operator": "contains", "value": "boss@"},
                {"type": "subject_match", "operator": "contains", "value": "紧急"},
            ],
        }]
        router = Tier1ReflexRouter()
        email = {"subject": "普通", "sender": "nobody@corp.com", "body": "", "to": [], "cc": []}
        assert "skill_or_test" not in router.route(email)

    @patch("src.router.tier1_reflex.get_skill_manager")
    def test_and_logic_all_must_match(self, mock_mgr):
        """默认 and 逻辑，所有条件必须满足。"""
        mock_mgr.return_value.get_tier1_triggers.return_value = [{
            "skill_id": "skill_and_test",
            "conditions": [
                {"type": "sender_match", "operator": "contains", "value": "boss@"},
                {"type": "subject_match", "operator": "contains", "value": "审批"},
            ],
        }]
        router = Tier1ReflexRouter()
        # 只满足 sender，不满足 subject — 不触发
        email1 = {"subject": "普通", "sender": "boss@corp.com", "body": "", "to": [], "cc": []}
        assert "skill_and_test" not in router.route(email1)

        # 两个都满足 — 触发
        email2 = {"subject": "审批请求", "sender": "boss@corp.com", "body": "", "to": [], "cc": []}
        assert "skill_and_test" in router.route(email2)
