import pytest
import os
import sys
from unittest.mock import MagicMock

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

@pytest.fixture
def mock_settings():
    """Mock settings environment variables."""
    msg = MagicMock()
    msg.EXCHANGE_API_URL = "http://mock-api"
    msg.EXCHANGE_API_KEY = "test-key"
    msg.EXCHANGE_ACCOUNT_ID = "test-account-id"
    msg.EXCHANGE_SSL_VERIFY = False
    return msg

@pytest.fixture
def mock_env(monkeypatch):
    """Set environment variables for testing."""
    monkeypatch.setenv("EXCHANGE_API_URL", "http://mock-api")
    monkeypatch.setenv("EXCHANGE_API_KEY", "test-key")
    monkeypatch.setenv("EXCHANGE_ACCOUNT_ID", "test-account-id")
    monkeypatch.setenv("LARK_APP_ID", "test-lark-id")
    monkeypatch.setenv("LARK_APP_SECRET", "test-lark-secret")
    monkeypatch.setenv("LARK_CHAT_ID", "test-chat-id")
