from unittest.mock import patch, MagicMock
from src.utils import notification_policy as np
import pytest


def _settings(me="me@example.com", leaders="boss@corp.com,vp@corp.com"):
    s = MagicMock()
    s.EXCHANGE_ACCOUNT_EMAIL = me
    s.LEADER_SENDERS = leaders
    return s


def test_is_direct_recipient_in_to_list():
    email = {"to": ["me@example.com", "x@x.com"], "cc": []}
    with patch("src.utils.notification_policy.get_settings", return_value=_settings()):
        assert np.is_direct_recipient(email) is True


def test_is_direct_recipient_case_insensitive_string_to():
    email = {"to": "ME@EXAMPLE.COM"}
    with patch("src.utils.notification_policy.get_settings", return_value=_settings()):
        assert np.is_direct_recipient(email) is True


def test_is_direct_recipient_cc_only_is_false():
    email = {"to": ["boss@example.com"], "cc": ["me@example.com"]}
    with patch("src.utils.notification_policy.get_settings", return_value=_settings()):
        assert np.is_direct_recipient(email) is False


def test_is_direct_recipient_empty_me_defaults_true():
    email = {"to": ["someone@example.com"]}
    with patch("src.utils.notification_policy.get_settings", return_value=_settings(me="")):
        assert np.is_direct_recipient(email) is True


def test_is_vip_sender_matches_leader():
    email = {"sender": "VP@corp.com"}
    with patch("src.utils.notification_policy.get_settings", return_value=_settings()):
        assert np.is_vip_sender(email) is True


def test_is_vip_sender_non_leader_false():
    email = {"sender": "random@corp.com"}
    with patch("src.utils.notification_policy.get_settings", return_value=_settings()):
        assert np.is_vip_sender(email) is False


def _cls(need_reply=False, intent="通知", priority="P3"):
    return {"need_reply": need_reply, "intent": intent, "priority": priority}


@pytest.mark.parametrize("classification,email,expected", [
    # need_reply 永远走审批，不进过滤表
    (_cls(need_reply=True, priority="P3"), {"to": [], "sender": "x@x.com"}, "approval"),
    # 垃圾邮件硬排除，即便我是直接收件人
    (_cls(intent="垃圾邮件"), {"to": ["me@example.com"], "sender": "x@x.com"}, "skipped"),
    # 直接收件人（仅抄送 boss，我在 To）→ 推送，不论优先级
    (_cls(priority="P3"), {"to": ["me@example.com"], "sender": "x@x.com"}, "read_only"),
    # 仅抄送 + VIP 发件 → 推送
    (_cls(priority="P3"), {"to": ["boss@example.com"], "sender": "vp@corp.com"}, "read_only"),
    # 仅抄送 + 非 VIP + P0 → 推送
    (_cls(priority="P0"), {"to": ["boss@example.com"], "sender": "random@corp.com"}, "read_only"),
    # 仅抄送 + 非 VIP + P1 → 推送
    (_cls(priority="P1"), {"to": ["boss@example.com"], "sender": "random@corp.com"}, "read_only"),
    # 仅抄送 + 非 VIP + P2 → 静默
    (_cls(priority="P2"), {"to": ["boss@example.com"], "sender": "random@corp.com"}, "skipped"),
    # 仅抄送 + 非 VIP + P3 → 静默
    (_cls(priority="P3"), {"to": ["boss@example.com"], "sender": "random@corp.com"}, "skipped"),
])
def test_decide_notification_kind(classification, email, expected):
    with patch("src.utils.notification_policy.get_settings",
               return_value=_settings(me="me@example.com", leaders="vp@corp.com")):
        assert np.decide_notification_kind(classification, email) == expected


def test_exchange_to_recipients_direct_recipient_gets_read_notification():
    email = {
        "to_recipients": [
            "Mailbox(name='同名用户3', email_address='me@example.com', "
            "routing_type='SMTP', mailbox_type='Mailbox')"
        ],
        "cc_recipients": [],
        "sender": "sender@example.com",
    }
    classification = _cls(need_reply=False, intent="通知", priority="P2")

    with patch(
        "src.utils.notification_policy.get_settings",
        return_value=_settings(me="me@example.com"),
    ):
        assert np.decide_notification_kind(classification, email) == "read_only"
