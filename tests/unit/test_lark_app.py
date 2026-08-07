import json

import pytest
from typing import get_args, get_type_hints
from unittest.mock import MagicMock, patch, AsyncMock
from src.utils import lark_app
from src.utils import lark_messaging

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


def test_init_lark_app_keeps_the_shared_graph_dependencies(mock_lark_deps, mock_env):
    db, graph, ex = mock_lark_deps
    dependencies = MagicMock()

    lark_app.init_lark_app(db, graph, ex, dependencies=dependencies)

    assert lark_app.graph_dependencies is dependencies

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


def test_manual_review_card_surfaces_email_content_without_an_acknowledge_action():
    from src.utils.card_builder import LarkCardBuilder

    client = MagicMock()
    response = MagicMock()
    response.success.return_value = True
    response.data.message_id = "message-1"
    client.im.v1.message.create.return_value = response
    settings = MagicMock(LARK_CHAT_ID="chat-1")
    builder = LarkCardBuilder(lark_api_client=None, exchange_client=None)

    with patch("src.utils.lark_messaging.get_settings", return_value=settings):
        delivered = lark_messaging.send_manual_review_card(
            "mail-1",
            {
                "subject": "需要人工处理",
                "sender": "sender@example.test",
                "to": ["recipient@example.test"],
                "cc": ["cc@example.test"],
                "body": "<p>需要人工处理的正文</p>",
            },
            "content_guard_rejected",
            pdf_url="https://example.invalid/original.pdf",
            lark_api_client=client,
            card_builder=builder,
        )

    request = client.im.v1.message.create.call_args.args[0]
    card = json.loads(request.request_body.content)
    elements_json = json.dumps(card["elements"], ensure_ascii=False)
    assert delivered is True
    assert card["header"]["template"] == "red"
    assert "需要人工处理" in card["header"]["title"]["content"]
    # The raw internal error code must not leak; a translated reason should.
    assert "content_guard_rejected" not in elements_json
    assert "幻觉" in elements_json
    assert "需要人工处理的正文" in elements_json
    assert "收件人" in elements_json
    assert "抄送" in elements_json
    assert "example.invalid/original.pdf" in elements_json
    assert all(element["tag"] != "action" for element in card["elements"])


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
        "src.utils.lark_pdf_flow.generate_and_upload_pdf",
        new=AsyncMock(side_effect=RuntimeError("boom")),
    ), patch(
        "src.utils.lark_app._require_graph_dependencies",
        return_value=MagicMock(),
    ):
        # Should not raise NameError or any other exception (the function swallows and logs).
        await lark_app.process_pdf_generation_and_reply("fake-id", state, "msg_456")


@pytest.mark.asyncio
async def test_process_pdf_shim_returns_explicit_flow_outcome(mock_env):
    from src.utils.lark_pdf_flow import PdfFlowOutcome

    state = MagicMock()
    outcome = PdfFlowOutcome(
        status="reply_sent_cleanup_pending",
        retryable=True,
        reply_sent=True,
        cleanup_tokens=("cleanup-token",),
    )
    implementation = AsyncMock(return_value=outcome)

    with patch(
        "src.utils.lark_pdf_flow.process_pdf_generation_and_reply",
        new=implementation,
    ), patch(
        "src.utils.lark_app._require_graph_dependencies",
        return_value=MagicMock(),
    ):
        result = await lark_app.process_pdf_generation_and_reply(
            "fake-id",
            state,
            "msg_456",
        )

    assert result is outcome
    implementation.assert_awaited_once()


def test_generate_pdf_shim_type_contract_includes_flow_outcome():
    from src.utils.lark_pdf_flow import PdfFlowOutcome

    return_hint = get_type_hints(lark_app.generate_and_upload_pdf)["return"]
    assert PdfFlowOutcome in get_args(return_hint)
