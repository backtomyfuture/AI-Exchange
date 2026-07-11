"""Startup recovery must finish before any side-effecting worker starts."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest


class RecoveryFailed(RuntimeError):
    """Sentinel raised when startup recovery cannot establish safe state."""


class WorkerReached(RuntimeError):
    """Sentinel used to stop ``main()`` after observing worker startup."""


def _install_startup_harness(
    monkeypatch: pytest.MonkeyPatch,
    *,
    recovery_error: Exception | None = None,
    stop_main_at_worker: bool = False,
) -> SimpleNamespace:
    from src import main as main_module
    from src.memory import consolidator as consolidator_module

    events: list[str] = []
    settings = SimpleNamespace(
        LOG_LEVEL="INFO",
        database_url="postgresql://test/test",
        POLLING_INTERVAL=300,
    )

    async def setup_async() -> None:
        events.append("context_setup")

    async def recover_incomplete_approval_states() -> int:
        events.append("approval_recovery")
        if recovery_error is not None:
            raise recovery_error
        return 2

    context = MagicMock()
    context.setup_async = AsyncMock(side_effect=setup_async)
    context.close = AsyncMock()
    context.db_manager = MagicMock()
    context.db_manager.recover_incomplete_approval_states = AsyncMock(
        side_effect=recover_incomplete_approval_states
    )

    lark = MagicMock()
    lark.init_lark_app.side_effect = lambda *args, **kwargs: events.append(
        "lark_init"
    )
    lark.start_lark_ws.side_effect = lambda: events.append("lark_ws")

    async def start_exchange_worker(_context) -> None:
        events.append("exchange_worker")
        if stop_main_at_worker:
            raise WorkerReached

    healer = MagicMock()

    async def start_healer() -> None:
        events.append("self_healer")

    healer.start = AsyncMock(side_effect=start_healer)

    def make_healer(*args, **kwargs):
        events.append("self_healer_init")
        return healer

    async def check_revision(_database_url: str) -> None:
        events.append("revision_check")

    monkeypatch.setattr(main_module, "get_settings", lambda: settings)
    monkeypatch.setattr(
        main_module,
        "require_current_database",
        AsyncMock(side_effect=check_revision),
    )
    monkeypatch.setattr(main_module, "get_app_context", lambda: context)
    monkeypatch.setattr(main_module, "lark_app", lark)
    monkeypatch.setattr(
        main_module,
        "exchange_start_worker",
        AsyncMock(side_effect=start_exchange_worker),
    )
    monkeypatch.setattr(main_module, "exchange_stop_worker", AsyncMock())
    monkeypatch.setattr(main_module, "SelfHealer", MagicMock(side_effect=make_healer))
    monkeypatch.setattr(main_module, "init_scheduler", MagicMock())
    monkeypatch.setattr(main_module, "run_scheduler", AsyncMock())
    monkeypatch.setattr(main_module, "run_polling_loop", AsyncMock())

    consolidator = MagicMock()
    consolidator.consolidate = AsyncMock(return_value={})
    monkeypatch.setattr(
        consolidator_module,
        "MemoryConsolidator",
        MagicMock(return_value=consolidator),
    )

    return SimpleNamespace(
        main_module=main_module,
        events=events,
        context=context,
        lark=lark,
        healer=healer,
        exchange_start_worker=main_module.exchange_start_worker,
    )


@pytest.mark.asyncio
async def test_lifespan_recovers_after_context_setup_before_workers(monkeypatch):
    harness = _install_startup_harness(monkeypatch)

    async with harness.main_module.lifespan(harness.main_module.app):
        await asyncio.sleep(0)

    assert harness.events.index("context_setup") < harness.events.index(
        "approval_recovery"
    )
    for side_effect in ("lark_init", "lark_ws", "exchange_worker", "self_healer"):
        assert harness.events.index("approval_recovery") < harness.events.index(
            side_effect
        )
    harness.context.db_manager.recover_incomplete_approval_states.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_main_recovers_after_context_setup_before_lark_and_worker(monkeypatch):
    harness = _install_startup_harness(monkeypatch, stop_main_at_worker=True)

    with pytest.raises(WorkerReached):
        await harness.main_module.main()

    assert harness.events == [
        "context_setup",
        "approval_recovery",
        "lark_init",
        "lark_ws",
        "exchange_worker",
    ]
    harness.context.db_manager.recover_incomplete_approval_states.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_lifespan_recovery_failure_prevents_all_worker_startup(monkeypatch):
    harness = _install_startup_harness(
        monkeypatch,
        recovery_error=RecoveryFailed("cannot establish safe startup state"),
    )

    with pytest.raises(RecoveryFailed, match="safe startup state"):
        async with harness.main_module.lifespan(harness.main_module.app):
            pass

    harness.lark.init_lark_app.assert_not_called()
    harness.lark.start_lark_ws.assert_not_called()
    harness.exchange_start_worker.assert_not_awaited()
    harness.main_module.SelfHealer.assert_not_called()


@pytest.mark.asyncio
async def test_main_recovery_failure_prevents_lark_and_worker_startup(monkeypatch):
    harness = _install_startup_harness(
        monkeypatch,
        recovery_error=RecoveryFailed("cannot establish safe startup state"),
        stop_main_at_worker=True,
    )

    with pytest.raises(RecoveryFailed, match="safe startup state"):
        await harness.main_module.main()

    harness.lark.init_lark_app.assert_not_called()
    harness.lark.start_lark_ws.assert_not_called()
    harness.exchange_start_worker.assert_not_awaited()
