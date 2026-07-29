"""Polling-only ingress acceptance checks."""

from src.server import app


def test_exchange_webhook_route_is_not_exposed() -> None:
    paths = {getattr(route, "path", None) for route in app.routes}

    assert "/webhooks/exchange" not in paths
