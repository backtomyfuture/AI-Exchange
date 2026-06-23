import pytest
from unittest.mock import MagicMock, patch, AsyncMock
import json
from src.utils import lark_app

@pytest.fixture
def mock_lark_deps():
    db_mgr = AsyncMock()
    graph = AsyncMock()
    ex_client = AsyncMock()
    return db_mgr, graph, ex_client

def test_init_lark_app(mock_lark_deps, mock_env):
    """Test initialization of Lark app globals."""
    db, graph, ex = mock_lark_deps
    lark_app.init_lark_app(db, graph, ex)
    
    assert lark_app.db_manager == db
    assert lark_app.graph == graph
    assert lark_app.exchange_client == ex
    assert lark_app.lark_api_client is not None
    assert lark_app.card_builder is not None

@patch("lark_oapi.Client")
def test_send_approval_card(mock_client_cls, mock_lark_deps, mock_env):
    """Test sending an approval card."""
    # Mock the builder chain
    mock_instance = MagicMock()
    mock_builder = MagicMock()
    mock_builder.app_id.return_value = mock_builder
    mock_builder.app_secret.return_value = mock_builder
    mock_builder.log_level.return_value = mock_builder
    mock_builder.build.return_value = mock_instance
    mock_client_cls.builder.return_value = mock_builder
    
    # Ensure init is called with minimal Lark config
    db, graph, ex = mock_lark_deps
    fake_settings = MagicMock(
        LARK_APP_ID="app_id",
        LARK_APP_SECRET="app_secret",
        LARK_CHAT_ID="chat_id",
        LARK_DRIVE_FOLDER_TOKEN="",
    )
    with patch("src.utils.lark_app.get_settings", return_value=fake_settings):
        lark_app.init_lark_app(db, graph, ex)
    
    # Verify the global was set to our mock
    assert lark_app.lark_api_client == mock_instance
    
    # Setup response
    mock_resp = MagicMock()
    mock_resp.success.return_value = True
    mock_resp.data.message_id = "msg_123"
    
    mock_instance.im.v1.message.create.return_value = mock_resp
    
    with patch("src.utils.lark_app.get_settings", return_value=fake_settings), \
         patch("src.utils.lark_messaging.get_settings", return_value=fake_settings):
        lark_app.send_approval_card(
            email_id="123",
            draft="Hi",
            context=[],
            email_data={"subject": "Test"},
            classification={},
        )
    
    # Check calls
    mock_instance.im.v1.message.create.assert_called_once()


@patch("lark_oapi.Client")
def test_update_card_ui(mock_client_cls, mock_lark_deps, mock_env):
    """Test patching a card."""
    # Mock the builder chain
    mock_instance = MagicMock()
    mock_builder = MagicMock()
    mock_builder.app_id.return_value = mock_builder
    mock_builder.app_secret.return_value = mock_builder
    mock_builder.log_level.return_value = mock_builder
    mock_builder.build.return_value = mock_instance
    mock_client_cls.builder.return_value = mock_builder

    db, graph, ex = mock_lark_deps
    fake_settings = MagicMock(
        LARK_APP_ID="app_id",
        LARK_APP_SECRET="app_secret",
        LARK_CHAT_ID="chat_id",
        LARK_DRIVE_FOLDER_TOKEN="",
    )
    with patch("src.utils.lark_app.get_settings", return_value=fake_settings):
        lark_app.init_lark_app(db, graph, ex)
    
    mock_resp = MagicMock()
    mock_resp.success.return_value = True
    
    mock_instance.im.v1.message.patch.return_value = mock_resp
    
    lark_app.update_card_ui("msg_123", {"header": "test"})
    
    mock_instance.im.v1.message.patch.assert_called_once()


@pytest.mark.asyncio
async def test_process_pdf_generation_handles_failure(mock_env):
    """Failure in PDF generation must be logged but never re-raise NameError on stale variables."""
    state = MagicMock()
    state.values = {"email": {"id": "fake-id", "subject": "x"}}

    # Simulate generate_and_upload_pdf raising an exception inside the function.
    with patch(
        "src.utils.lark_app.generate_and_upload_pdf",
        new=AsyncMock(side_effect=RuntimeError("boom")),
    ):
        # Should not raise NameError or any other exception (the function swallows and logs).
        await lark_app.process_pdf_generation_and_reply("fake-id", state, "msg_456")
