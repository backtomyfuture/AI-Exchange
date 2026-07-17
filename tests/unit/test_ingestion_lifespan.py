from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src import server as server_module


@pytest.mark.parametrize(
    ("shutdown_seconds", "expected_timeout"),
    ((1, 4.0), (30, 62.0)),
)
def test_runtime_stop_timeout_covers_both_declared_drain_phases(
    shutdown_seconds: int,
    expected_timeout: float,
) -> None:
    settings = SimpleNamespace(INGESTION_SHUTDOWN_SECONDS=shutdown_seconds)

    assert server_module._runtime_stop_timeout_seconds(settings) == expected_timeout


@pytest.mark.parametrize("value", (True, 0, 31, 1.5, "30"))
def test_runtime_stop_timeout_rejects_unbounded_or_ambiguous_values(value) -> None:
    with pytest.raises(RuntimeError, match="^ingestion_shutdown_budget_invalid$"):
        server_module._runtime_stop_timeout_seconds(
            SimpleNamespace(INGESTION_SHUTDOWN_SECONDS=value)
        )


def test_processing_control_loss_disables_intake_and_exits_process() -> None:
    with (
        patch.object(server_module.lark_app, "disable_lark_intake") as disable,
        patch.object(server_module.os, "_exit", side_effect=SystemExit(1)) as exit_,
        pytest.raises(SystemExit),
    ):
        server_module._fail_stop_after_processing_control_loss(
            "ingestion_runtime_heartbeat_lost"
        )

    disable.assert_called_once_with()
    exit_.assert_called_once_with(1)


@pytest.mark.asyncio
async def test_lark_ws_startup_wait_is_hard_bounded() -> None:
    with (
        patch.object(server_module.lark_app, "lark_ws_ready", return_value=False),
        pytest.raises(RuntimeError, match="^lark_ws_startup_timeout$"),
    ):
        await server_module._wait_for_lark_ws_connection(
            timeout_seconds=0.01,
            poll_seconds=0.001,
        )


@pytest.mark.asyncio
async def test_sustained_lark_ws_disconnect_invokes_fail_stop_once() -> None:
    reasons: list[str] = []
    with patch.object(server_module.lark_app, "lark_ws_ready", return_value=False):
        await asyncio.wait_for(
            server_module._monitor_lark_ws_connection(
                fail_stop=reasons.append,
                grace_seconds=0.01,
                poll_seconds=0.001,
            ),
            timeout=0.1,
        )

    assert reasons == ["lark_ws_disconnected"]


@pytest.mark.asyncio
async def test_transient_lark_ws_disconnect_is_tolerated_and_monitor_cancels_cleanly(
) -> None:
    connected = False
    reasons: list[str] = []

    def ready() -> bool:
        return connected

    with patch.object(server_module.lark_app, "lark_ws_ready", side_effect=ready):
        monitor = asyncio.create_task(
            server_module._monitor_lark_ws_connection(
                fail_stop=reasons.append,
                grace_seconds=0.05,
                poll_seconds=0.001,
            )
        )
        await asyncio.sleep(0.01)
        connected = True
        await asyncio.sleep(0.01)
        await server_module._cancel_lark_ws_monitor(monitor)

    assert reasons == []
    assert monitor.cancelled()


class _Runtime:
    def __init__(self, events: list[str], *, start_error: Exception | None = None):
        self.events = events
        self.start_error = start_error
        self.webhook_ingress_service = object()

    async def start(self) -> None:
        self.events.append("runtime.start")
        if self.start_error is not None:
            raise self.start_error

    async def stop(self) -> None:
        self.events.append("runtime.stop")


class _Context:
    def __init__(self, events: list[str], runtime: _Runtime):
        self.events = events
        self.runtime = runtime

    def create_ingestion_runtime(self, settings, *, fail_stop=None) -> _Runtime:
        assert fail_stop is server_module._fail_stop_after_processing_control_loss
        self.events.append("context.create")
        return self.runtime

    def release_ingestion_runtime(self, runtime: _Runtime) -> None:
        assert runtime is self.runtime
        self.events.append("context.release")


@pytest.mark.asyncio
async def test_single_lifespan_publishes_after_start_and_unpublishes_before_stop():
    events: list[str] = []
    settings = SimpleNamespace()
    runtime = _Runtime(events)
    context = _Context(events, runtime)
    application = SimpleNamespace(state=SimpleNamespace())

    def security_gate(received) -> None:
        assert received is settings
        events.append("security")

    async def database_gate(received) -> None:
        assert received is settings
        events.append("database.preflight")

    with (
        patch.object(server_module, "get_settings", return_value=settings),
        patch.object(server_module, "validate_runtime_security", security_gate),
        patch.object(
            server_module,
            "require_runtime_database_boundary",
            database_gate,
        ),
        patch.object(server_module, "get_runtime_app_context", return_value=context),
    ):
        async with server_module.application_lifespan(application):
            assert application.state.ingestion_runtime is runtime
            assert (
                application.state.webhook_ingress_service
                is runtime.webhook_ingress_service
            )
            events.append("serving")

    assert application.state.webhook_ingress_service is None
    assert application.state.ingestion_runtime is None
    assert events == [
        "security",
        "database.preflight",
        "context.create",
        "runtime.start",
        "serving",
        "runtime.stop",
        "context.release",
    ]


@pytest.mark.asyncio
async def test_preflight_failure_constructs_no_runtime_resource():
    application = SimpleNamespace(state=SimpleNamespace())
    settings = SimpleNamespace()
    context = MagicMock()

    with (
        patch.object(server_module, "get_settings", return_value=settings),
        patch.object(server_module, "validate_runtime_security"),
        patch.object(
            server_module,
            "require_runtime_database_boundary",
            new=AsyncMock(side_effect=RuntimeError("schema_invalid")),
        ),
        patch.object(server_module, "get_runtime_app_context", return_value=context),
    ):
        with pytest.raises(RuntimeError, match="schema_invalid"):
            async with server_module.application_lifespan(application):
                raise AssertionError("lifespan must not yield")

    context.create_ingestion_runtime.assert_not_called()


@pytest.mark.asyncio
async def test_runtime_start_failure_is_stopped_released_and_never_published():
    events: list[str] = []
    settings = SimpleNamespace()
    runtime = _Runtime(events, start_error=RuntimeError("registration_failed"))
    context = _Context(events, runtime)
    application = SimpleNamespace(state=SimpleNamespace())

    with (
        patch.object(server_module, "get_settings", return_value=settings),
        patch.object(server_module, "validate_runtime_security"),
        patch.object(
            server_module,
            "require_runtime_database_boundary",
            new=AsyncMock(),
        ),
        patch.object(server_module, "get_runtime_app_context", return_value=context),
    ):
        with pytest.raises(RuntimeError, match="registration_failed"):
            async with server_module.application_lifespan(application):
                raise AssertionError("lifespan must not yield")

    assert application.state.webhook_ingress_service is None
    assert application.state.ingestion_runtime is None
    assert events == [
        "context.create",
        "runtime.start",
        "runtime.stop",
        "context.release",
    ]


def test_runtime_context_builds_exactly_one_runtime_without_legacy_initialize():
    from src import init_app

    settings = SimpleNamespace()
    runtime = MagicMock()
    context = init_app.AppContext()

    with (
        patch.object(
            init_app, "build_ingestion_runtime", return_value=runtime
        ) as build,
        patch.object(
            context,
            "initialize",
            side_effect=AssertionError("legacy_initialize_reached"),
        ) as legacy_initialize,
    ):
        assert context.create_ingestion_runtime(settings) is runtime
        with pytest.raises(RuntimeError, match="already_created"):
            context.create_ingestion_runtime(settings)
        context.release_ingestion_runtime(runtime)

    build.assert_called_once_with(
        settings,
        processing_context=context,
        fail_stop=None,
    )
    legacy_initialize.assert_not_called()
    assert context.ingestion_runtime is None


def test_runtime_context_accessor_has_no_legacy_initialization_side_effect():
    from src import init_app

    context = init_app.AppContext()
    with (
        patch.object(init_app, "app_context", context),
        patch.object(
            context,
            "initialize",
            side_effect=AssertionError("legacy_initialize_reached"),
        ) as legacy_initialize,
    ):
        assert init_app.get_runtime_app_context() is context

    legacy_initialize.assert_not_called()


class _ProcessingContext(_Context):
    def __init__(self, events: list[str], runtime: _Runtime):
        super().__init__(events, runtime)
        self.db_manager = object()
        self.graph = object()
        self.exchange_client = object()
        self.graph_dependencies = object()

    def bind_checkpoint_write_guard(self, guard) -> None:
        assert callable(guard)
        self.events.append("context.bind")

    async def setup_async(self) -> None:
        self.events.append("context.setup")

    async def close(self) -> None:
        self.events.append("context.close")


class _Fence:
    def __init__(self, events: list[str], *, close_error: Exception | None = None):
        self.events = events
        self.close_error = close_error

    async def start(self) -> None:
        self.events.append("fence.start")

    async def assert_held(self) -> None:
        return None

    async def close(self) -> None:
        self.events.append("fence.close")
        if self.close_error is not None:
            raise self.close_error


@pytest.mark.asyncio
async def test_processing_lifespan_starts_before_publish_and_closes_fence_last():
    events: list[str] = []
    settings = SimpleNamespace(
        DURABLE_INBOX_ENABLED=True,
        database_url="postgresql://runtime@example.invalid/database",
    )
    runtime = _Runtime(events)
    context = _ProcessingContext(events, runtime)
    fence = _Fence(events)
    application = SimpleNamespace(state=SimpleNamespace())

    def init_lark(db_manager, graph, exchange_client, **kwargs) -> None:
        assert db_manager is context.db_manager
        assert graph is context.graph
        assert exchange_client is context.exchange_client
        assert kwargs["worker_loop_arg"] is not None
        assert kwargs["dependencies"] is context.graph_dependencies
        events.append("lark.init")

    def start_lark_ws(**kwargs) -> None:
        assert kwargs["fail_stop"] is server_module._fail_stop_after_processing_control_loss
        events.append("lark.ws.start")

    with (
        patch.object(server_module, "get_settings", return_value=settings),
        patch.object(
            server_module,
            "validate_runtime_security",
            side_effect=lambda _settings: events.append("security"),
        ),
        patch.object(
            server_module,
            "require_runtime_database_boundary",
            new=AsyncMock(side_effect=lambda _settings: events.append("database.preflight")),
        ),
        patch.object(server_module, "get_runtime_app_context", return_value=context),
        patch.object(server_module, "initialize_app_context", return_value=context),
        patch.object(
            server_module,
            "RuntimeCheckpointMaintenanceFence",
            return_value=fence,
        ) as fence_factory,
        patch.object(server_module.lark_app, "init_lark_app", side_effect=init_lark),
        patch.object(
            server_module.lark_app,
            "start_lark_ws",
            side_effect=start_lark_ws,
        ),
        patch.object(
            server_module,
            "_wait_for_lark_ws_connection",
            new=AsyncMock(side_effect=lambda: events.append("lark.ws.ready")),
        ),
        patch.object(
            server_module.lark_app,
            "lark_ws_ready",
            return_value=True,
        ),
        patch.object(
            server_module.lark_app,
            "begin_lark_ws_shutdown",
            side_effect=lambda: events.append("lark.ws.shutdown.begin"),
        ),
        patch.object(
            server_module.lark_app,
            "disable_lark_intake",
            side_effect=lambda: events.append("lark.disable"),
        ),
        patch.object(
            server_module.lark_app,
            "enable_lark_intake",
            side_effect=lambda: events.append("lark.enable"),
        ),
        patch.object(
            server_module.lark_app,
            "stop_lark_intake",
            new=AsyncMock(side_effect=lambda **_kwargs: events.append("lark.intake.stop")),
        ),
        patch.object(
            server_module.lark_app,
            "stop_lark_ws",
            side_effect=lambda **_kwargs: events.append("lark.ws.stop"),
        ),
    ):
        async with server_module.application_lifespan(application):
            assert application.state.ingestion_runtime is runtime
            assert application.state.webhook_ingress_service is runtime.webhook_ingress_service
            events.append("serving")

    fence_factory.assert_called_once_with(
        settings.database_url,
        fail_stop=server_module._fail_stop_after_checkpoint_fence_loss,
    )
    assert application.state.ingestion_runtime is None
    assert application.state.webhook_ingress_service is None
    assert events == [
        "security",
        "database.preflight",
        "context.create",
        "fence.start",
        "context.bind",
        "context.setup",
        "lark.init",
        "lark.disable",
        "lark.ws.start",
        "lark.ws.ready",
        "runtime.start",
        "lark.enable",
        "serving",
        "lark.ws.shutdown.begin",
        "lark.disable",
        "runtime.stop",
        "lark.intake.stop",
        "lark.ws.stop",
        "context.close",
        "context.release",
        "fence.close",
    ]


@pytest.mark.asyncio
async def test_lark_ws_startup_timeout_never_starts_worker_and_cleans_owned_resources():
    application = SimpleNamespace(state=SimpleNamespace())
    settings = SimpleNamespace(
        DURABLE_INBOX_ENABLED=True,
        database_url="postgresql://runtime@example.invalid/database",
    )
    runtime = SimpleNamespace(
        start=AsyncMock(),
        stop=AsyncMock(),
        webhook_ingress_service=object(),
    )
    context = SimpleNamespace(
        create_ingestion_runtime=MagicMock(return_value=runtime),
        bind_checkpoint_write_guard=MagicMock(),
        setup_async=AsyncMock(),
        db_manager=object(),
        graph=object(),
        exchange_client=object(),
        graph_dependencies=object(),
        close=AsyncMock(),
        release_ingestion_runtime=MagicMock(),
    )
    fence = SimpleNamespace(
        start=AsyncMock(),
        assert_held=AsyncMock(),
        close=AsyncMock(),
    )

    with (
        patch.object(server_module, "get_settings", return_value=settings),
        patch.object(server_module, "validate_runtime_security"),
        patch.object(
            server_module,
            "require_runtime_database_boundary",
            new=AsyncMock(),
        ),
        patch.object(server_module, "get_runtime_app_context", return_value=context),
        patch.object(server_module, "initialize_app_context", return_value=context),
        patch.object(
            server_module,
            "RuntimeCheckpointMaintenanceFence",
            return_value=fence,
        ),
        patch.object(server_module.lark_app, "init_lark_app"),
        patch.object(server_module.lark_app, "disable_lark_intake"),
        patch.object(server_module.lark_app, "enable_lark_intake") as enable,
        patch.object(server_module.lark_app, "start_lark_ws") as start_ws,
        patch.object(server_module.lark_app, "begin_lark_ws_shutdown") as begin_stop,
        patch.object(server_module.lark_app, "stop_lark_intake", new=AsyncMock()),
        patch.object(server_module.lark_app, "stop_lark_ws") as stop_ws,
        patch.object(
            server_module,
            "_wait_for_lark_ws_connection",
            new=AsyncMock(side_effect=RuntimeError("lark_ws_startup_timeout")),
        ),
        pytest.raises(RuntimeError, match="^lark_ws_startup_timeout$"),
    ):
        async with server_module.application_lifespan(application):
            raise AssertionError("lifespan must not yield")

    start_ws.assert_called_once_with(
        fail_stop=server_module._fail_stop_after_processing_control_loss
    )
    runtime.start.assert_not_awaited()
    runtime.stop.assert_awaited_once_with()
    enable.assert_not_called()
    begin_stop.assert_called_once_with()
    stop_ws.assert_called_once_with(timeout_seconds=server_module._LARK_WS_JOIN_SECONDS)
    context.close.assert_awaited_once_with()
    context.release_ingestion_runtime.assert_called_once_with(runtime)
    fence.close.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_context_close_failure_keeps_checkpoint_fence_held() -> None:
    application = SimpleNamespace(state=SimpleNamespace())
    runtime = SimpleNamespace(stop=AsyncMock())
    context = SimpleNamespace(
        close=AsyncMock(side_effect=RuntimeError("checkpoint_close_unknown")),
        release_ingestion_runtime=MagicMock(),
    )
    fence = SimpleNamespace(close=AsyncMock())

    with (
        patch.object(server_module.lark_app, "disable_lark_intake"),
        patch.object(server_module.lark_app, "stop_lark_intake", new=AsyncMock()),
        patch.object(server_module.lark_app, "stop_lark_ws"),
        pytest.raises(
            server_module.ApplicationShutdownError,
            match="^application_shutdown_incomplete$",
        ),
    ):
        await server_module._shutdown_application_components(
            application,
            context=context,
            runtime=runtime,
            fence=fence,
            context_initialize_attempted=True,
            lark_initialize_attempted=True,
            lark_ws_start_attempted=True,
        )

    runtime.stop.assert_awaited_once()
    context.close.assert_awaited_once()
    context.release_ingestion_runtime.assert_not_called()
    fence.close.assert_not_awaited()


@pytest.mark.asyncio
async def test_runtime_stop_failure_keeps_context_runtime_and_fence_owned() -> None:
    application = SimpleNamespace(state=SimpleNamespace())
    runtime = SimpleNamespace(stop=AsyncMock(side_effect=RuntimeError("stop_failed")))
    context = SimpleNamespace(
        close=AsyncMock(),
        release_ingestion_runtime=MagicMock(),
    )
    fence = SimpleNamespace(close=AsyncMock())

    with (
        patch.object(server_module.lark_app, "disable_lark_intake"),
        patch.object(server_module.lark_app, "stop_lark_intake", new=AsyncMock()),
        patch.object(server_module.lark_app, "stop_lark_ws"),
        pytest.raises(
            server_module.ApplicationShutdownError,
            match="^application_shutdown_incomplete$",
        ),
    ):
        await server_module._shutdown_application_components(
            application,
            context=context,
            runtime=runtime,
            fence=fence,
            context_initialize_attempted=True,
            lark_initialize_attempted=True,
            lark_ws_start_attempted=True,
        )

    runtime.stop.assert_awaited_once()
    context.close.assert_not_awaited()
    context.release_ingestion_runtime.assert_not_called()
    fence.close.assert_not_awaited()


@pytest.mark.asyncio
async def test_primary_startup_failure_is_not_replaced_by_cleanup_failure() -> None:
    application = SimpleNamespace(state=SimpleNamespace())
    settings = SimpleNamespace(DURABLE_INBOX_ENABLED=False)
    runtime = SimpleNamespace(
        webhook_ingress_service=object(),
        start=AsyncMock(side_effect=RuntimeError("registration_failed")),
        stop=AsyncMock(side_effect=RuntimeError("stop_failed")),
    )
    context = SimpleNamespace(
        create_ingestion_runtime=MagicMock(return_value=runtime),
        release_ingestion_runtime=MagicMock(),
    )

    with (
        patch.object(server_module, "get_settings", return_value=settings),
        patch.object(server_module, "validate_runtime_security"),
        patch.object(
            server_module,
            "require_runtime_database_boundary",
            new=AsyncMock(),
        ),
        patch.object(server_module, "get_runtime_app_context", return_value=context),
        patch.object(server_module.lark_app, "disable_lark_intake") as disable,
        pytest.raises(RuntimeError, match="^registration_failed$"),
    ):
        async with server_module.application_lifespan(application):
            raise AssertionError("lifespan must not yield")

    disable.assert_not_called()
    runtime.stop.assert_awaited_once()
    context.release_ingestion_runtime.assert_not_called()


@pytest.mark.asyncio
async def test_readiness_reports_active_only_when_processing_is_ready() -> None:
    runtime = SimpleNamespace(
        processing_ready=True,
        check_ready=AsyncMock(return_value=True),
    )
    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(ingestion_runtime=runtime))
    )

    with (
        patch.object(server_module, "get_settings", return_value=SimpleNamespace()),
        patch.object(server_module, "validate_runtime_security"),
        patch.object(
            server_module,
            "_require_cached_runtime_database",
            new=AsyncMock(),
        ),
    ):
        response = await server_module.readiness_check(request)

    assert response == {"status": "ready", "processing": "active"}


@pytest.mark.asyncio
async def test_processing_readiness_requires_live_lark_websocket() -> None:
    runtime = SimpleNamespace(
        processing_ready=True,
        check_ready=AsyncMock(return_value=True),
    )
    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(ingestion_runtime=runtime))
    )
    settings = SimpleNamespace(DURABLE_INBOX_ENABLED=True)

    with (
        patch.object(server_module, "get_settings", return_value=settings),
        patch.object(server_module, "validate_runtime_security"),
        patch.object(
            server_module,
            "_require_cached_runtime_database",
            new=AsyncMock(),
        ),
        patch.object(server_module.lark_app, "lark_ws_ready", return_value=False),
    ):
        response = await server_module.readiness_check(request)

    assert response.status_code == 503
    assert response.body == b'{"status":"not_ready"}'
