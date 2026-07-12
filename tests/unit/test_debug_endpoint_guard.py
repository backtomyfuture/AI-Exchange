import pytest
from unittest.mock import patch, MagicMock


@pytest.mark.asyncio
async def test_debug_endpoint_blocked_in_production():
    """Debug endpoint should return 403 when DEBUG=False."""
    mock_settings = MagicMock()
    mock_settings.DEBUG = False
    mock_settings.APP_ENV = "development"
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
    mock_settings.APP_ENV = "development"
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
                "id": "test_push_debug", "subject": "x", "sender": "x",
                "to": ["x"], "body": "x", "received_at": "2026-01-01"
            })
            assert resp.status_code == 200


@pytest.mark.asyncio
async def test_debug_endpoint_is_hidden_if_production_is_misconfigured_debug_true():
    """Defense in depth: production returns 404 even before startup fail-fast."""
    mock_settings = MagicMock()
    mock_settings.DEBUG = True
    mock_settings.APP_ENV = "production"

    with patch("src.server.get_settings", return_value=mock_settings):
        from httpx import ASGITransport, AsyncClient
        from src.server import app

        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/debug/inject_email",
                json={
                    "id": "test_push_hidden",
                    "subject": "x",
                    "sender": "x",
                    "to": ["x"],
                    "body": "x",
                    "received_at": "2026-01-01",
                },
            )

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_debug_injection_rejects_oversized_body_without_store_growth():
    mock_settings = MagicMock(DEBUG=True, APP_ENV="development")
    store = {}

    with patch("src.server.get_settings", return_value=mock_settings), patch(
        "src.utils.lark_app._mock_store",
        store,
    ):
        from httpx import ASGITransport, AsyncClient
        from src.server import app

        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/debug/inject_email",
                json={
                    "id": "test_push_oversized",
                    "subject": "x",
                    "sender": "x",
                    "to": ["x"],
                    "body": "x" * (1_048_576 + 1),
                    "received_at": "2026-01-01",
                },
            )

    assert response.status_code == 422
    assert store == {}


@pytest.mark.asyncio
async def test_production_hidden_debug_route_does_not_consume_request_body():
    from src.server import app

    sent = []

    async def receive():
        raise AssertionError("production_hidden_route_consumed_body")

    async def send(message):
        sent.append(message)

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/debug/inject_email",
        "raw_path": b"/debug/inject_email",
        "query_string": b"",
        "headers": [(b"host", b"test")],
        "client": ("127.0.0.1", 12345),
        "server": ("test", 80),
    }
    settings = MagicMock(DEBUG=True, APP_ENV="production")

    with patch("src.server.get_settings", return_value=settings):
        await app(scope, receive, send)

    start = next(message for message in sent if message["type"] == "http.response.start")
    assert start["status"] == 404
