"""
Lark WebSocket lifecycle facade.

This module exists as a dedicated WS entrypoint while keeping backward
compatibility with existing imports from src.utils.lark_app.
"""

from typing import Any

from src.utils import lark_app


def init_lark_app(
    db_mgr,
    graph_instance,
    ex_client,
    worker_loop_arg=None,
    *,
    dependencies=None,
):
    return lark_app.init_lark_app(
        db_mgr,
        graph_instance,
        ex_client,
        worker_loop_arg=worker_loop_arg,
        dependencies=dependencies,
    )


def start_lark_ws():
    return lark_app.start_lark_ws()


def safe_async_run(coro):
    return lark_app.safe_async_run(coro)


def safe_async_wait(coro):
    return lark_app.safe_async_wait(coro)


def handle_card_action(event: Any):
    return lark_app.handle_card_action(event)


def verify_lark_signature(timestamp: str, nonce: str, body: str, signature: str) -> bool:
    return lark_app.verify_lark_signature(timestamp, nonce, body, signature)


def get_lark_client():
    return lark_app.lark_api_client
