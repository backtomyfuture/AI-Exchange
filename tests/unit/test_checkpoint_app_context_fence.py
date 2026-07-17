from __future__ import annotations

import base64
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import SecretStr


pytestmark = pytest.mark.asyncio


async def test_setup_async_rejects_unbound_checkpoint_write_guard_before_io() -> None:
    from src.db.checkpoint_saver import CheckpointWriteFenceConfigurationError
    from src.init_app import AppContext

    context = AppContext()
    context.db_manager = MagicMock()
    context.db_manager.open = AsyncMock()
    context.db_manager.recover_incomplete_approval_states = AsyncMock(
        return_value=0
    )
    context.pool = MagicMock()
    context.pool.open = AsyncMock()
    context.exchange_client = MagicMock()
    context.exchange_client.get_all_folders = AsyncMock(return_value=[])

    with (
        patch("src.init_app.get_settings", return_value=MagicMock()),
        pytest.raises(
            CheckpointWriteFenceConfigurationError,
            match="^checkpoint_write_fence_not_bound$",
        ),
    ):
        await context.setup_async()

    context.db_manager.open.assert_not_awaited()
    context.pool.open.assert_not_awaited()
    context.exchange_client.get_all_folders.assert_not_awaited()


async def test_write_guard_cannot_be_bound_after_graph_creation() -> None:
    from src.db.checkpoint_saver import CheckpointWriteFenceConfigurationError
    from src.init_app import AppContext

    context = AppContext()
    context.graph = object()

    with pytest.raises(
        CheckpointWriteFenceConfigurationError,
        match="^checkpoint_write_fence_binding_closed$",
    ):
        context.bind_checkpoint_write_guard(AsyncMock())

    assert context._checkpoint_write_guard is None


async def test_initialize_configures_every_checkpoint_pool_session_with_shared_lock(
    tmp_path,
) -> None:
    from src.db.checkpoint_saver import configure_checkpoint_pool_connection
    from src.init_app import AppContext

    settings = SimpleNamespace(
        CONTENT_STORE_ROOT=str(tmp_path / "content"),
        CONTENT_STORE_KEY=SecretStr(base64.b64encode(bytes(range(32))).decode("ascii")),
        CONTENT_STORE_KEY_VERSION="v1",
        database_url="postgresql://runtime:PRIVATE@localhost/email_agent",
    )
    context = AppContext()
    pool_factory = MagicMock()

    with (
        patch("src.init_app.get_settings", return_value=settings),
        patch("src.init_app.ExchangeClient"),
        patch("src.init_app.EmailProcessor"),
        patch("src.init_app.AsyncDatabaseManager"),
        patch("src.init_app.AsyncConnectionPool", pool_factory),
    ):
        context.initialize()

    pool_factory.assert_called_once_with(
        conninfo=settings.database_url,
        max_size=20,
        kwargs={"autocommit": True, "prepare_threshold": 0},
        configure=configure_checkpoint_pool_connection,
        open=False,
    )


async def test_setup_async_builds_graph_with_bound_fenced_saver() -> None:
    from src.init_app import AppContext

    context = AppContext()
    context.db_manager = MagicMock()
    context.db_manager.open = AsyncMock()
    context.db_manager.recover_incomplete_approval_states = AsyncMock(
        return_value=0
    )
    context.pool = MagicMock()
    context.pool.open = AsyncMock()
    context.exchange_client = MagicMock()
    context.exchange_client.get_all_folders = AsyncMock(return_value=[])
    context.graph_dependencies = object()
    guard = AsyncMock()
    context.bind_checkpoint_write_guard(guard)
    settings = SimpleNamespace(
        EXCHANGE_FOLDERS_FULL="",
        EXCHANGE_FOLDERS_ARCHIVE="",
    )
    saver = object()
    graph = object()

    with (
        patch("src.init_app.get_settings", return_value=settings),
        patch("src.init_app.FencedAsyncPostgresSaver", return_value=saver) as factory,
        patch("src.init_app.build_graph", return_value=graph) as build,
    ):
        await context.setup_async()

    factory.assert_called_once_with(context.pool, write_guard=guard)
    build.assert_called_once_with(
        checkpointer=saver,
        dependencies=context.graph_dependencies,
    )
    assert context.graph is graph


async def test_setup_async_recovers_ambiguous_sends_before_other_startup_io() -> None:
    from src.init_app import AppContext

    events: list[str] = []
    context = AppContext()
    context.db_manager = MagicMock()
    context.db_manager.open = AsyncMock(
        side_effect=lambda: events.append("db.open")
    )
    context.db_manager.recover_incomplete_approval_states = AsyncMock(
        side_effect=lambda: events.append("send.recover") or 1
    )
    context.pool = MagicMock()
    context.pool.open = AsyncMock(
        side_effect=lambda: events.append("checkpoint.open")
    )
    context.exchange_client = MagicMock()
    context.exchange_client.get_all_folders = AsyncMock(
        side_effect=lambda: events.append("exchange.folders") or []
    )
    context.graph_dependencies = object()
    context.bind_checkpoint_write_guard(AsyncMock())
    settings = SimpleNamespace(
        EXCHANGE_FOLDERS_FULL="",
        EXCHANGE_FOLDERS_ARCHIVE="",
    )

    with (
        patch("src.init_app.get_settings", return_value=settings),
        patch("src.init_app.FencedAsyncPostgresSaver", return_value=object()),
        patch("src.init_app.build_graph", return_value=object()),
    ):
        await context.setup_async()

    assert events == [
        "db.open",
        "send.recover",
        "checkpoint.open",
        "exchange.folders",
    ]


async def test_setup_async_fails_closed_when_send_recovery_fails() -> None:
    from src.domain.errors import DatabaseOperationError
    from src.init_app import AppContext

    context = AppContext()
    context.db_manager = MagicMock()
    context.db_manager.open = AsyncMock()
    context.db_manager.recover_incomplete_approval_states = AsyncMock(
        side_effect=DatabaseOperationError(
            operation="recover_incomplete_approval_states",
            retryable=True,
            message="bounded recovery failure",
        )
    )
    context.pool = MagicMock()
    context.pool.open = AsyncMock()
    context.exchange_client = MagicMock()
    context.exchange_client.get_all_folders = AsyncMock(return_value=[])
    context.bind_checkpoint_write_guard(AsyncMock())

    with pytest.raises(DatabaseOperationError):
        await context.setup_async()

    context.pool.open.assert_not_awaited()
    context.exchange_client.get_all_folders.assert_not_awaited()


async def test_context_close_is_best_effort_in_checkpoint_database_exchange_order() -> (
    None
):
    from src.init_app import AppContext, AppContextCloseError

    events: list[str] = []
    context = AppContext()

    async def fail_database_close() -> None:
        events.append("database.close")
        raise RuntimeError("database_close_failed")

    context.pool = SimpleNamespace(
        close=AsyncMock(side_effect=lambda: events.append("checkpoint.close"))
    )
    context.db_manager = SimpleNamespace(
        close=AsyncMock(side_effect=fail_database_close)
    )
    context.exchange_client = SimpleNamespace(
        close=AsyncMock(side_effect=lambda: events.append("exchange.close"))
    )

    with pytest.raises(AppContextCloseError, match="^app_context_close_failed$"):
        await context.close()

    assert context.pool.close.await_count == 1
    assert context.db_manager.close.await_count == 1
    assert context.exchange_client.close.await_count == 1
    assert events == ["checkpoint.close", "database.close", "exchange.close"]


async def test_context_close_preserves_cancellation_after_attempting_all_resources() -> (
    None
):
    import asyncio

    from src.init_app import AppContext

    events: list[str] = []
    context = AppContext()

    async def cancel_checkpoint() -> None:
        events.append("checkpoint.close")
        raise asyncio.CancelledError

    context.pool = SimpleNamespace(close=AsyncMock(side_effect=cancel_checkpoint))
    context.db_manager = SimpleNamespace(
        close=AsyncMock(side_effect=lambda: events.append("database.close"))
    )
    context.exchange_client = SimpleNamespace(
        close=AsyncMock(side_effect=lambda: events.append("exchange.close"))
    )

    with pytest.raises(asyncio.CancelledError):
        await context.close()

    assert events == ["checkpoint.close", "database.close", "exchange.close"]
