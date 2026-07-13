"""Startup recovery must finish before any side-effecting worker starts."""

import asyncio
import logging
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest


class RecoveryFailed(RuntimeError):
    """Sentinel raised when startup recovery cannot establish safe state."""


class WorkerReached(RuntimeError):
    """Sentinel used to stop ``main()`` after observing worker startup."""


def test_fence_loss_closes_lark_intake_before_process_fail_stop(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from src import main as main_module

    events: list[object] = []
    monkeypatch.setattr(
        main_module.lark_app,
        "disable_lark_intake",
        lambda: events.append("lark_intake_disabled"),
    )
    monkeypatch.setattr(
        main_module.os,
        "_exit",
        lambda code: events.append(("hard_exit", code)),
    )

    with caplog.at_level(logging.CRITICAL, logger="MainService"):
        main_module._fail_stop_after_maintenance_fence_loss("private-connection-detail")

    assert events == ["lark_intake_disabled", ("hard_exit", 70)]
    assert "private-connection-detail" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failing_stage",
    ["self_healer", "exchange", "lark_actions", "lark_ws", "context"],
)
async def test_shutdown_attempts_every_stage_then_hard_exits_before_fence_release(
    monkeypatch: pytest.MonkeyPatch,
    failing_stage: str,
) -> None:
    from src import main as main_module

    events: list[object] = []

    def sync_stage(name: str) -> None:
        events.append(name)
        if failing_stage == name:
            raise RuntimeError("private-shutdown-detail")

    async def async_stage(name: str) -> None:
        events.append(name)
        if failing_stage == name:
            raise RuntimeError("private-shutdown-detail")

    async def stop_lark_intake(**_kwargs) -> None:
        await async_stage("lark_actions")

    async def stop_exchange() -> None:
        await async_stage("exchange")

    async def close_context() -> None:
        await async_stage("context")

    lark = MagicMock()
    lark.disable_lark_intake.side_effect = lambda: sync_stage("lark_disable")
    lark.stop_lark_intake = AsyncMock(side_effect=stop_lark_intake)
    lark.stop_lark_ws.side_effect = lambda **_kwargs: sync_stage("lark_ws")
    monkeypatch.setattr(main_module, "lark_app", lark)
    monkeypatch.setattr(
        main_module,
        "exchange_stop_worker",
        AsyncMock(side_effect=stop_exchange),
    )
    monkeypatch.setattr(
        main_module.os,
        "_exit",
        lambda code: events.append(("hard_exit", code)),
    )
    healer = MagicMock()
    healer.stop.side_effect = lambda: sync_stage("self_healer")
    ctx = MagicMock()
    ctx.close = AsyncMock(side_effect=close_context)

    with pytest.raises(RuntimeError, match="runtime_shutdown_incomplete"):
        await main_module._shutdown_runtime_components(
            ctx=ctx,
            lark_initialized=True,
            exchange_worker_start_attempted=True,
            self_healer=healer,
            background_tasks=[],
        )

    assert events == [
        "lark_disable",
        "self_healer",
        "exchange",
        "lark_actions",
        "lark_ws",
        "context",
        ("hard_exit", 70),
    ]


@pytest.mark.asyncio
async def test_shutdown_times_out_cancellation_resistant_background_then_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src import main as main_module

    monkeypatch.setattr(
        main_module,
        "_BACKGROUND_TASK_SHUTDOWN_SECONDS",
        0.01,
        raising=False,
    )
    events: list[str] = []
    background_started = asyncio.Event()
    release_background = asyncio.Event()
    hard_exit_called = asyncio.Event()

    async def cancellation_resistant_background() -> None:
        background_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await release_background.wait()

    async def stage(name: str) -> None:
        events.append(name)

    async def stop_exchange() -> None:
        await stage("exchange")

    async def stop_lark_actions(**_kwargs) -> None:
        await stage("lark_actions")

    async def close_context() -> None:
        await stage("context")

    lark = MagicMock()
    lark.disable_lark_intake.side_effect = lambda: events.append("lark_disable")
    lark.stop_lark_intake = AsyncMock(side_effect=stop_lark_actions)
    lark.stop_lark_ws.side_effect = lambda **_kwargs: events.append("lark_ws")
    monkeypatch.setattr(main_module, "lark_app", lark)
    monkeypatch.setattr(
        main_module,
        "exchange_stop_worker",
        AsyncMock(side_effect=stop_exchange),
    )

    def hard_exit(_code: int) -> None:
        events.append("hard_exit")
        hard_exit_called.set()

    monkeypatch.setattr(main_module.os, "_exit", hard_exit)
    ctx = MagicMock()
    ctx.close = AsyncMock(side_effect=close_context)
    background_task = asyncio.create_task(cancellation_resistant_background())
    await background_started.wait()
    shutdown = asyncio.create_task(
        main_module._shutdown_runtime_components(
            ctx=ctx,
            lark_initialized=True,
            exchange_worker_start_attempted=True,
            self_healer=None,
            background_tasks=[background_task],
        )
    )

    try:
        await asyncio.wait_for(hard_exit_called.wait(), timeout=0.2)
        with pytest.raises(RuntimeError, match="runtime_shutdown_incomplete"):
            await shutdown
        assert events == [
            "lark_disable",
            "exchange",
            "lark_actions",
            "lark_ws",
            "context",
            "hard_exit",
        ]
    finally:
        release_background.set()
        await asyncio.gather(background_task, return_exceptions=True)
        if not shutdown.done():
            await asyncio.gather(shutdown, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("cancel_stage", "expected_events"),
    [
        ("background", ["lark_disable", "background", ("hard_exit", 70)]),
        ("exchange", ["lark_disable", "exchange", ("hard_exit", 70)]),
        (
            "lark_actions",
            ["lark_disable", "exchange", "lark_actions", ("hard_exit", 70)],
        ),
        (
            "lark_ws",
            [
                "lark_disable",
                "exchange",
                "lark_actions",
                "lark_ws",
                ("hard_exit", 70),
            ],
        ),
        (
            "context",
            [
                "lark_disable",
                "exchange",
                "lark_actions",
                "lark_ws",
                "context",
                ("hard_exit", 70),
            ],
        ),
    ],
)
async def test_shutdown_external_cancellation_hard_exits_before_later_stages(
    monkeypatch: pytest.MonkeyPatch,
    cancel_stage: str,
    expected_events: list[object],
) -> None:
    from src import main as main_module

    loop = asyncio.get_running_loop()
    events: list[object] = []
    selected_stage_entered = asyncio.Event()
    release_async_stage = asyncio.Event()
    release_ws_stage = threading.Event()

    async def cancellation_resistant_background() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            events.append("background")
            selected_stage_entered.set()
            await release_async_stage.wait()

    async def async_stage(name: str) -> None:
        events.append(name)
        if cancel_stage == name:
            selected_stage_entered.set()
            await release_async_stage.wait()

    async def stop_exchange() -> None:
        await async_stage("exchange")

    async def stop_lark_actions(**_kwargs) -> None:
        await async_stage("lark_actions")

    def stop_lark_ws(**_kwargs) -> None:
        events.append("lark_ws")
        if cancel_stage == "lark_ws":
            loop.call_soon_threadsafe(selected_stage_entered.set)
            release_ws_stage.wait()

    async def close_context() -> None:
        await async_stage("context")

    lark = MagicMock()
    lark.disable_lark_intake.side_effect = lambda: events.append("lark_disable")
    lark.stop_lark_intake = AsyncMock(side_effect=stop_lark_actions)
    lark.stop_lark_ws.side_effect = stop_lark_ws
    monkeypatch.setattr(main_module, "lark_app", lark)
    monkeypatch.setattr(
        main_module,
        "exchange_stop_worker",
        AsyncMock(side_effect=stop_exchange),
    )
    monkeypatch.setattr(
        main_module.os,
        "_exit",
        lambda code: events.append(("hard_exit", code)),
    )
    ctx = MagicMock()
    ctx.close = AsyncMock(side_effect=close_context)
    background_tasks = []
    if cancel_stage == "background":
        background_tasks.append(
            asyncio.create_task(cancellation_resistant_background())
        )

    shutdown = asyncio.create_task(
        main_module._shutdown_runtime_components(
            ctx=ctx,
            lark_initialized=True,
            exchange_worker_start_attempted=True,
            self_healer=None,
            background_tasks=background_tasks,
        )
    )

    try:
        await asyncio.wait_for(selected_stage_entered.wait(), timeout=1)
        shutdown.cancel()
        with pytest.raises(RuntimeError, match="runtime_shutdown_incomplete"):
            await shutdown
        assert events == expected_events
    finally:
        release_async_stage.set()
        release_ws_stage.set()
        cleanup_tasks = [
            task
            for task in asyncio.all_tasks()
            if task is not asyncio.current_task()
            and (
                task in background_tasks
                or task.get_name().startswith("runtime-shutdown-")
            )
        ]
        if cleanup_tasks:
            await asyncio.wait_for(
                asyncio.gather(*cleanup_tasks, return_exceptions=True),
                timeout=1,
            )
        if not shutdown.done():
            await asyncio.gather(shutdown, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "hanging_stage",
    ["exchange", "lark_actions", "lark_ws", "context"],
)
async def test_shutdown_times_out_each_hanging_stage_and_attempts_later_stages(
    monkeypatch: pytest.MonkeyPatch,
    hanging_stage: str,
) -> None:
    from src import main as main_module

    for timeout_name in (
        "_EXCHANGE_WORKER_SHUTDOWN_SECONDS",
        "_LARK_ACTION_SHUTDOWN_SECONDS",
        "_LARK_WS_SHUTDOWN_SECONDS",
        "_CONTEXT_SHUTDOWN_SECONDS",
    ):
        monkeypatch.setattr(main_module, timeout_name, 0.01, raising=False)

    events: list[str] = []
    release_hanging_stage = asyncio.Event()
    release_ws_stage = threading.Event()
    hard_exit_called = asyncio.Event()

    async def async_stage(name: str) -> None:
        events.append(name)
        if hanging_stage == name:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await release_hanging_stage.wait()

    def ws_stage(**_kwargs) -> None:
        events.append("lark_ws")
        if hanging_stage == "lark_ws":
            release_ws_stage.wait()

    async def stop_exchange() -> None:
        await async_stage("exchange")

    async def stop_lark_actions(**_kwargs) -> None:
        await async_stage("lark_actions")

    async def close_context() -> None:
        await async_stage("context")

    lark = MagicMock()
    lark.disable_lark_intake.side_effect = lambda: events.append("lark_disable")
    lark.stop_lark_intake = AsyncMock(side_effect=stop_lark_actions)
    lark.stop_lark_ws.side_effect = ws_stage
    monkeypatch.setattr(main_module, "lark_app", lark)
    monkeypatch.setattr(
        main_module,
        "exchange_stop_worker",
        AsyncMock(side_effect=stop_exchange),
    )

    def hard_exit(_code: int) -> None:
        events.append("hard_exit")
        hard_exit_called.set()

    monkeypatch.setattr(main_module.os, "_exit", hard_exit)
    ctx = MagicMock()
    ctx.close = AsyncMock(side_effect=close_context)
    shutdown = asyncio.create_task(
        main_module._shutdown_runtime_components(
            ctx=ctx,
            lark_initialized=True,
            exchange_worker_start_attempted=True,
            self_healer=None,
            background_tasks=[],
        )
    )

    try:
        await asyncio.wait_for(hard_exit_called.wait(), timeout=0.3)
        with pytest.raises(RuntimeError, match="runtime_shutdown_incomplete"):
            await shutdown
        assert "hard_exit" in events
        if hanging_stage != "context":
            assert "context" in events
    finally:
        release_hanging_stage.set()
        release_ws_stage.set()
        if not shutdown.done():
            await asyncio.gather(shutdown, return_exceptions=True)
        await asyncio.sleep(0)


def _install_startup_harness(
    monkeypatch: pytest.MonkeyPatch,
    *,
    recovery_error: Exception | None = None,
    database_error: Exception | None = None,
    stop_main_at_worker: bool = False,
) -> SimpleNamespace:
    from src import main as main_module
    from src.memory import consolidator as consolidator_module

    events: list[str] = []
    settings = SimpleNamespace(
        LOG_LEVEL="INFO",
        database_url="postgresql://test/test",
        POLLING_INTERVAL=300,
        DURABLE_INBOX_ENABLED=False,
        INGESTION_SHADOW_ENABLED=False,
        SYNC_RECONCILIATION_ENABLED=False,
        DATABASE_ROLE_SEPARATION_REQUIRED=True,
        POSTGRES_USER="runtime_user",
        POSTGRES_MIGRATION_OWNER_ROLE="migration_owner",
        POSTGRES_SCHEMA="public",
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

    def bind_checkpoint_write_guard(guard) -> None:
        assert callable(guard)
        events.append("checkpoint_write_guard_bound")

    context.bind_checkpoint_write_guard = MagicMock(
        side_effect=bind_checkpoint_write_guard
    )

    async def close_context() -> None:
        events.append("context_close")

    context.close = AsyncMock(side_effect=close_context)
    context.db_manager = MagicMock()
    context.db_manager.recover_incomplete_approval_states = AsyncMock(
        side_effect=recover_incomplete_approval_states
    )

    lark = MagicMock()
    lark.init_lark_app.side_effect = lambda *args, **kwargs: events.append("lark_init")
    lark.start_lark_ws.side_effect = lambda: events.append("lark_ws")

    async def stop_lark_intake(*, timeout_seconds: float) -> None:
        assert timeout_seconds > 0
        events.append("lark_intake_stop")

    lark.stop_lark_intake = AsyncMock(side_effect=stop_lark_intake)

    async def start_exchange_worker(_context) -> None:
        events.append("exchange_worker")
        if stop_main_at_worker:
            raise WorkerReached

    async def stop_exchange_worker() -> None:
        events.append("exchange_worker_stop")

    healer = MagicMock()

    async def start_healer() -> None:
        events.append("self_healer")

    healer.start = AsyncMock(side_effect=start_healer)

    def make_healer(*args, **kwargs):
        events.append("self_healer_init")
        return healer

    async def check_revision(_database_url: str, **_flags: bool) -> None:
        events.append("revision_check")
        if database_error is not None:
            raise database_error

    revision_check = AsyncMock(side_effect=check_revision)

    class RuntimeFence:
        def __init__(self, dsn: str, *, fail_stop) -> None:
            assert dsn == settings.database_url
            assert callable(fail_stop)

        async def __aenter__(self):
            events.append("maintenance_fence_acquired")
            return self

        async def __aexit__(self, exc_type, exc, traceback) -> None:
            events.append("maintenance_fence_released")

        async def assert_held(self) -> None:
            raise AssertionError("startup must bind, not eagerly invoke, the guard")

    monkeypatch.setattr(main_module, "get_settings", lambda: settings)
    monkeypatch.setattr(
        main_module,
        "require_runtime_database",
        revision_check,
        raising=False,
    )
    monkeypatch.setattr(
        main_module,
        "require_current_database",
        revision_check,
        raising=False,
    )
    monkeypatch.setattr(main_module, "get_app_context", lambda: context)
    monkeypatch.setattr(main_module, "lark_app", lark)
    monkeypatch.setattr(
        main_module,
        "RuntimeCheckpointMaintenanceFence",
        RuntimeFence,
        raising=False,
    )
    monkeypatch.setattr(
        main_module,
        "exchange_start_worker",
        AsyncMock(side_effect=start_exchange_worker),
    )
    monkeypatch.setattr(
        main_module,
        "exchange_stop_worker",
        AsyncMock(side_effect=stop_exchange_worker),
    )
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
    assert harness.events.index("maintenance_fence_acquired") < harness.events.index(
        "checkpoint_write_guard_bound"
    )
    assert harness.events.index("checkpoint_write_guard_bound") < harness.events.index(
        "context_setup"
    )
    for side_effect in ("lark_init", "lark_ws", "exchange_worker", "self_healer"):
        assert harness.events.index("approval_recovery") < harness.events.index(
            side_effect
        )
    harness.context.db_manager.recover_incomplete_approval_states.assert_awaited_once_with()
    assert harness.events.index("lark_intake_stop") < harness.events.index(
        "context_close"
    )
    assert harness.events.index("context_close") < harness.events.index(
        "maintenance_fence_released"
    )


@pytest.mark.asyncio
async def test_main_recovers_after_context_setup_before_lark_and_worker(monkeypatch):
    harness = _install_startup_harness(monkeypatch, stop_main_at_worker=True)

    with pytest.raises(WorkerReached):
        await harness.main_module.main()

    assert harness.events == [
        "revision_check",
        "maintenance_fence_acquired",
        "checkpoint_write_guard_bound",
        "context_setup",
        "approval_recovery",
        "lark_init",
        "lark_ws",
        "exchange_worker",
        "exchange_worker_stop",
        "lark_intake_stop",
        "context_close",
        "maintenance_fence_released",
    ]
    harness.context.db_manager.recover_incomplete_approval_states.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_main_database_preflight_failure_prevents_all_runtime_setup(monkeypatch):
    failure = RuntimeError("database_role_preflight_failed")
    harness = _install_startup_harness(
        monkeypatch,
        database_error=failure,
    )

    with pytest.raises(RuntimeError, match="database_role_preflight_failed"):
        await harness.main_module.main()

    assert harness.events == ["revision_check"]
    harness.context.setup_async.assert_not_awaited()
    harness.context.db_manager.recover_incomplete_approval_states.assert_not_awaited()
    harness.lark.init_lark_app.assert_not_called()
    harness.lark.start_lark_ws.assert_not_called()
    harness.exchange_start_worker.assert_not_awaited()


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
    assert harness.events == [
        "revision_check",
        "maintenance_fence_acquired",
        "checkpoint_write_guard_bound",
        "context_setup",
        "approval_recovery",
        "context_close",
        "maintenance_fence_released",
    ]


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
    assert harness.events == [
        "revision_check",
        "maintenance_fence_acquired",
        "checkpoint_write_guard_bound",
        "context_setup",
        "approval_recovery",
        "context_close",
        "maintenance_fence_released",
    ]
