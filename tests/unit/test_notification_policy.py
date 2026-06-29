from unittest.mock import patch, MagicMock
from src.utils import notification_policy as np


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
