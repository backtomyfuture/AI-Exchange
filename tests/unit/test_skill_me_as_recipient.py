import pytest
from unittest.mock import Mock, patch
from src.router.tier1_reflex import Tier1ReflexRouter

@pytest.fixture
def mock_settings():
    with patch("src.config.get_settings") as mock:
        mock_obj = Mock()
        mock_obj.EXCHANGE_ACCOUNT_EMAIL = "me@example.com"
        mock.return_value = mock_obj
        yield mock_obj

@pytest.fixture
def router(mock_settings):
    # Ensure skill manager loads our new skill
    return Tier1ReflexRouter()

def test_to_match_with_me_placeholder(router, mock_settings):
    """测试 $ME 占位符在 To 匹配中的应用"""
    email = {
        "to": ["me@example.com"],
        "cc": ["other@example.com"],
        "sender": "sender@example.com",
        "subject": "Hello",
        "body": "Test"
    }
    
    # 查找是否有 skill_me_as_recipient 匹配
    result = router.route(email)
    assert "skill_me_as_recipient" in result

def test_to_match_no_trigger_on_cc(router, mock_settings):
    """测试抄送中包含 $ME 时不触发 (因为规则只检查 To)"""
    email = {
        "to": ["boss@example.com"],
        "cc": ["me@example.com"],
        "sender": "sender@example.com",
        "subject": "Hello",
        "body": "Test"
    }
    
    result = router.route(email)
    assert "skill_me_as_recipient" not in result

def test_to_match_no_trigger_on_other(router, mock_settings):
    """测试收件人不包含 $ME 时不触发"""
    email = {
        "to": ["someone@example.com"],
        "cc": [],
        "sender": "sender@example.com",
        "subject": "Hello",
        "body": "Test"
    }
    
    result = router.route(email)
    assert "skill_me_as_recipient" not in result
