import pytest
from unittest.mock import patch, MagicMock


@pytest.mark.asyncio
async def test_debug_endpoint_blocked_in_production():
    """Debug endpoint should return 403 when DEBUG=False."""
    mock_settings = MagicMock()
    mock_settings.DEBUG = False
    mock_settings.LARK_APP_ID = ""
    mock_settings.LARK_APP_SECRET = ""
    mock_settings.LARK_CHAT_ID = ""
    mock_settings.EXCHANGE_WEBHOOK_SECRET = "test"

    with patch("src.server.get_settings", return_value=mock_settings), \
         patch("src.utils.lark_app.get_settings", return_value=mock_settings):
        from src.server import app
        from httpx import AsyncClient, ASGITransport
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post("/debug/inject_email", json={
                "id": "test", "subject": "x", "sender": "x",
                "to": ["x"], "body": "x", "received_at": "2026-01-01"
            })
            assert resp.status_code == 403


@pytest.mark.asyncio
async def test_debug_endpoint_allowed_in_debug_mode():
    """Debug endpoint should work when DEBUG=True."""
    mock_settings = MagicMock()
    mock_settings.DEBUG = True
    mock_settings.LARK_APP_ID = ""
    mock_settings.LARK_APP_SECRET = ""
    mock_settings.LARK_CHAT_ID = ""
    mock_settings.EXCHANGE_WEBHOOK_SECRET = "test"

    with patch("src.server.get_settings", return_value=mock_settings), \
         patch("src.utils.lark_app.get_settings", return_value=mock_settings), \
         patch("src.utils.lark_app._mock_store", {}):
        from src.server import app
        from httpx import AsyncClient, ASGITransport
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post("/debug/inject_email", json={
                "id": "test_debug", "subject": "x", "sender": "x",
                "to": ["x"], "body": "x", "received_at": "2026-01-01"
            })
            assert resp.status_code == 200
