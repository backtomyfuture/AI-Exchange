from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src import server as server_module


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

    def create_ingestion_runtime(self, settings) -> _Runtime:
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

    build.assert_called_once_with(settings)
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
