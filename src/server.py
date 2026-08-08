import asyncio
import hashlib
import logging
import math
import os
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version
from weakref import WeakKeyDictionary
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
from src.config import get_settings
from src.daily_digest import DailyDigestScheduler
from src.db.maintenance_fence import RuntimeCheckpointMaintenanceFence
from src.db.schema import require_runtime_database
from src.db.runtime_boundary import require_runtime_database_boundary
from src.init_app import get_app_context as initialize_app_context
from src.init_app import get_runtime_app_context
from src.security.auth import require_metrics_auth, validate_runtime_security
from src.security.redaction import fingerprint_identifier
from src.utils import lark_app

logger = logging.getLogger("WebServer")
DEBUG_BODY_MAX_CHARS = 1_048_576
_READINESS_SUCCESS_TTL_SECONDS = 5.0
_READINESS_FAILURE_TTL_SECONDS = 1.0
_READINESS_DATABASE_TIMEOUT_SECONDS = 5.0
_READINESS_FAILURE_LOG_TTL_SECONDS = 5.0
_LARK_INTAKE_DRAIN_SECONDS = 30.0
_LARK_INTAKE_STOP_SECONDS = 32.0
_LARK_WS_JOIN_SECONDS = 5.0
_LARK_WS_STOP_SECONDS = 11.0
_LARK_WS_START_SECONDS = 30.0
_LARK_WS_START_POLL_SECONDS = 0.1
_LARK_WS_DISCONNECT_GRACE_SECONDS = 30.0
_LARK_WS_MONITOR_INTERVAL_SECONDS = 1.0
_CONTEXT_CLOSE_SECONDS = 10.0
_FENCE_CLOSE_SECONDS = 10.0
_RUNTIME_STOP_MARGIN_SECONDS = 2.0
_MAX_RUNTIME_SHUTDOWN_SECONDS = 30
_RUNTIME_STOP_SECONDS = 2.0 * _MAX_RUNTIME_SHUTDOWN_SECONDS + (
    _RUNTIME_STOP_MARGIN_SECONDS
)


class ReadinessPreflightError(RuntimeError):
    """Safe cached failure for a recently failed database preflight."""


class ApplicationShutdownError(RuntimeError):
    """Fixed failure for an incomplete application-lifecycle shutdown."""


@dataclass(frozen=True, slots=True)
class _RuntimeHealthSnapshot:
    """Identifier-free projection used by the operator endpoints."""

    processing_active: bool
    polling_active: bool
    polling_cursor_ready: bool


def _runtime_health_snapshot(runtime: object) -> _RuntimeHealthSnapshot:
    """Read the runtime seam with a safe fallback for test adapters."""

    try:
        snapshot_factory = getattr(runtime, "health_snapshot", None)
        if callable(snapshot_factory):
            snapshot = snapshot_factory()
            return _RuntimeHealthSnapshot(
                processing_active=bool(
                    getattr(snapshot, "processing_active", False)
                ),
                polling_active=bool(getattr(snapshot, "polling_active", False)),
                polling_cursor_ready=bool(
                    getattr(snapshot, "polling_cursor_ready", False)
                ),
            )
        return _RuntimeHealthSnapshot(
            processing_active=bool(getattr(runtime, "processing_ready", False)),
            polling_active=bool(getattr(runtime, "polling_live", False)),
            polling_cursor_ready=bool(getattr(runtime, "polling_ready", False)),
        )
    except Exception:
        return _RuntimeHealthSnapshot(
            processing_active=False,
            polling_active=False,
            polling_cursor_ready=False,
        )


def _runtime_stop_timeout_seconds(settings: Any) -> float:
    """Cover both runtime drain phases plus a small bounded cleanup margin."""

    value = getattr(settings, "INGESTION_SHUTDOWN_SECONDS", 30)
    if type(value) is not int or not 1 <= value <= _MAX_RUNTIME_SHUTDOWN_SECONDS:
        raise RuntimeError("ingestion_shutdown_budget_invalid")
    return 2.0 * float(value) + _RUNTIME_STOP_MARGIN_SECONDS


_ReadinessContract = tuple[bytes, bool, bool, str, str, str, str, str]


@dataclass
class _ReadinessState:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    contract: _ReadinessContract | None = field(default=None, repr=False)
    expires_at: float = 0.0
    ready: bool = False
    next_failure_log_at: float = 0.0


_READINESS_STATES: WeakKeyDictionary[
    asyncio.AbstractEventLoop,
    _ReadinessState,
] = WeakKeyDictionary()


def _readiness_contract(settings, database_url: str) -> _ReadinessContract:
    return (
        hashlib.sha256(database_url.encode()).digest(),
        bool(getattr(settings, "DURABLE_INBOX_ENABLED", False)),
        bool(getattr(settings, "DATABASE_ROLE_SEPARATION_REQUIRED", False)),
        str(getattr(settings, "POSTGRES_USER", "")),
        str(getattr(settings, "POSTGRES_MIGRATION_OWNER_ROLE", "")),
        str(getattr(settings, "POSTGRES_MAINTENANCE_ROLE", "")),
        str(getattr(settings, "POSTGRES_CHECKPOINT_AUDITOR_ROLE", "")),
        str(getattr(settings, "POSTGRES_SCHEMA", "public")),
    )


def _readiness_state(
    loop: asyncio.AbstractEventLoop,
) -> _ReadinessState:
    state = _READINESS_STATES.get(loop)
    if state is None:
        state = _ReadinessState()
        _READINESS_STATES[loop] = state
    return state


def _log_readiness_failure_once(exc: Exception) -> None:
    loop = asyncio.get_running_loop()
    state = _readiness_state(loop)
    now = loop.time()
    if now < state.next_failure_log_at:
        return
    state.next_failure_log_at = now + _READINESS_FAILURE_LOG_TTL_SECONDS
    logger.warning("Readiness check failed: error_type=%s", type(exc).__name__)


async def _require_cached_runtime_database(settings) -> None:
    """Single-flight the expensive catalog proof and briefly cache its result."""

    loop = asyncio.get_running_loop()
    state = _readiness_state(loop)
    database_url = str(settings.database_url)
    contract = _readiness_contract(settings, database_url)

    def use_cached_result(now: float) -> bool:
        if state.contract != contract or now >= state.expires_at:
            return False
        if not state.ready:
            raise ReadinessPreflightError("readiness_preflight_failed")
        return True

    if use_cached_result(loop.time()):
        return

    async with state.lock:
        if use_cached_result(loop.time()):
            return
        try:
            async with asyncio.timeout(_READINESS_DATABASE_TIMEOUT_SECONDS):
                await require_runtime_database(
                    database_url,
                    durable_inbox_enabled=contract[1],
                    role_separation_required=contract[2],
                    expected_runtime_role=contract[3],
                    expected_migration_role=contract[4],
                    expected_maintenance_role=contract[5],
                    expected_auditor_role=contract[6],
                    target_schema=contract[7],
                )
        except Exception:
            state.contract = contract
            state.ready = False
            state.expires_at = loop.time() + _READINESS_FAILURE_TTL_SECONDS
            raise
        state.contract = contract
        state.ready = True
        state.expires_at = loop.time() + _READINESS_SUCCESS_TTL_SECONDS


try:
    SERVICE_VERSION = version("ai-exchange")
except PackageNotFoundError:
    SERVICE_VERSION = "0.1.0"

_initial_app_env = str(getattr(get_settings(), "APP_ENV", "development")).casefold()
_docs_enabled = _initial_app_env != "production"


def _fail_stop_after_checkpoint_fence_loss(_reason: str) -> None:
    """Disable human intake before the fence forces process termination."""

    logger.critical("Checkpoint maintenance lifecycle fence was lost")
    lark_app.disable_lark_intake()


def _fail_stop_after_processing_control_loss(reason: str) -> None:
    """Terminate so the process supervisor restarts a lost control plane."""

    logger.critical("Processing control plane was lost: reason=%s", reason)
    lark_app.disable_lark_intake()
    os._exit(1)


def _bounded_lark_seconds(name: str, value: object, *, maximum: float) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or not 0 < float(value) <= maximum
    ):
        raise RuntimeError(f"{name}_invalid")
    return float(value)


async def _wait_for_lark_ws_connection(
    *,
    timeout_seconds: float = _LARK_WS_START_SECONDS,
    poll_seconds: float = _LARK_WS_START_POLL_SECONDS,
) -> None:
    """Wait a bounded time for an actual SDK WebSocket connection."""

    timeout = _bounded_lark_seconds(
        "lark_ws_startup_budget",
        timeout_seconds,
        maximum=120.0,
    )
    poll = _bounded_lark_seconds(
        "lark_ws_startup_poll",
        poll_seconds,
        maximum=5.0,
    )
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not lark_app.lark_ws_ready():
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise RuntimeError("lark_ws_startup_timeout")
        await asyncio.sleep(min(poll, remaining))


async def _monitor_lark_ws_connection(
    *,
    fail_stop: Callable[[str], None],
    grace_seconds: float = _LARK_WS_DISCONNECT_GRACE_SECONDS,
    poll_seconds: float = _LARK_WS_MONITOR_INTERVAL_SECONDS,
) -> None:
    """Fail-stop once when a live callback connection stays unavailable."""

    if not callable(fail_stop):
        raise ValueError("lark_ws_fail_stop_invalid")
    grace = _bounded_lark_seconds(
        "lark_ws_disconnect_grace",
        grace_seconds,
        maximum=300.0,
    )
    poll = _bounded_lark_seconds(
        "lark_ws_monitor_interval",
        poll_seconds,
        maximum=5.0,
    )
    loop = asyncio.get_running_loop()
    disconnected_since: float | None = None
    while True:
        if lark_app.lark_ws_ready():
            disconnected_since = None
        else:
            now = loop.time()
            if disconnected_since is None:
                disconnected_since = now
            elif now - disconnected_since >= grace:
                logger.critical(
                    "Lark WebSocket connection remained unavailable past grace"
                )
                fail_stop("lark_ws_disconnected")
                return
        await asyncio.sleep(poll)


async def _cancel_lark_ws_monitor(
    monitor: asyncio.Task[None] | None,
) -> None:
    if monitor is None:
        return
    monitor.cancel()
    try:
        await monitor
    except asyncio.CancelledError as exc:
        current = asyncio.current_task()
        if current is not None and current.cancelling():
            raise exc


async def _async_noop() -> None:
    """Keep optional lifecycle stages awaitable without special branches."""


async def _shutdown_application_components(
    application: FastAPI,
    *,
    context: Any,
    runtime: Any,
    fence: RuntimeCheckpointMaintenanceFence | None,
    context_initialize_attempted: bool,
    lark_initialize_attempted: bool,
    lark_ws_start_attempted: bool,
    lark_ws_monitor_task: asyncio.Task[None] | None = None,
    daily_digest_scheduler: DailyDigestScheduler | None = None,
    runtime_stop_seconds: float = _RUNTIME_STOP_SECONDS,
) -> None:
    """Attempt every owned shutdown stage before releasing the fence."""

    failures: list[tuple[str, BaseException]] = []
    application.state.ingestion_runtime = None
    application.state.daily_digest_scheduler = None

    def attempt_sync(stage: str, operation) -> bool:
        try:
            operation()
        except BaseException as exc:
            failures.append((stage, exc))
            return False
        return True

    async def attempt_async(stage: str, operation) -> bool:
        try:
            await operation()
        except BaseException as exc:
            failures.append((stage, exc))
            return False
        return True

    processing_cleanup_required = bool(
        fence is not None
        or context_initialize_attempted
        or lark_initialize_attempted
        or lark_ws_start_attempted
    )
    lark_ws_shutdown_started = True
    if lark_ws_start_attempted:
        lark_ws_shutdown_started = attempt_sync(
            "lark_ws_shutdown_begin",
            lark_app.begin_lark_ws_shutdown,
        )
    lark_disable_succeeded = True
    if processing_cleanup_required:
        lark_disable_succeeded = attempt_sync(
            "lark_intake_disable",
            lark_app.disable_lark_intake,
        )
    lark_ws_monitor_stopped = await attempt_async(
        "lark_ws_monitor_stop",
        lambda: _cancel_lark_ws_monitor(lark_ws_monitor_task),
    )
    daily_digest_stopped = await attempt_async(
        "daily_digest_stop",
        lambda: (
            daily_digest_scheduler.stop()
            if daily_digest_scheduler is not None
            else _async_noop()
        ),
    )
    runtime_stop_succeeded = await attempt_async(
        "runtime_stop",
        lambda: asyncio.wait_for(
            runtime.stop(),
            timeout=runtime_stop_seconds,
        ),
    )
    lark_intake_stop_succeeded = True
    if lark_initialize_attempted:
        lark_intake_stop_succeeded = await attempt_async(
            "lark_intake_stop",
            lambda: asyncio.wait_for(
                lark_app.stop_lark_intake(
                    timeout_seconds=_LARK_INTAKE_DRAIN_SECONDS,
                ),
                timeout=_LARK_INTAKE_STOP_SECONDS,
            ),
        )
    lark_ws_stop_succeeded = True
    if lark_ws_start_attempted:
        lark_ws_stop_succeeded = await attempt_async(
            "lark_ws_stop",
            lambda: asyncio.wait_for(
                asyncio.to_thread(
                    lark_app.stop_lark_ws,
                    timeout_seconds=_LARK_WS_JOIN_SECONDS,
                ),
                timeout=_LARK_WS_STOP_SECONDS,
            ),
        )
    processing_stopped = (
        runtime_stop_succeeded
        and lark_ws_shutdown_started
        and lark_ws_monitor_stopped
        and daily_digest_stopped
        and lark_disable_succeeded
        and lark_intake_stop_succeeded
        and lark_ws_stop_succeeded
    )
    context_close_succeeded = not context_initialize_attempted
    if context_initialize_attempted:
        if processing_stopped:
            context_close_succeeded = await attempt_async(
                "context_close",
                lambda: asyncio.wait_for(
                    context.close(),
                    timeout=_CONTEXT_CLOSE_SECONDS,
                ),
            )
        else:
            failures.append(
                (
                    "context_close_blocked",
                    RuntimeError("processing_shutdown_unproved"),
                )
            )
    owned_resources_closed = processing_stopped and context_close_succeeded
    if owned_resources_closed:
        attempt_sync(
            "runtime_release",
            lambda: context.release_ingestion_runtime(runtime),
        )
    else:
        failures.append(
            (
                "runtime_release_blocked",
                RuntimeError("runtime_release_blocked"),
            )
        )
    if fence is not None:
        if owned_resources_closed:
            await attempt_async(
                "fence_close",
                lambda: asyncio.wait_for(
                    fence.close(),
                    timeout=_FENCE_CLOSE_SECONDS,
                ),
            )
        else:
            failures.append(
                (
                    "fence_close_blocked",
                    RuntimeError("checkpoint_fence_release_blocked"),
                )
            )
    if failures:
        logger.critical(
            "Application shutdown failed closed: stages=%s",
            ",".join(stage for stage, _exc in failures),
        )
        for _stage, exc in failures:
            if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
                raise exc
        raise ApplicationShutdownError("application_shutdown_incomplete")


@asynccontextmanager
async def application_lifespan(application: FastAPI):
    """Own the one polling runtime and optional processing stack."""

    settings = get_settings()
    runtime_stop_seconds = _runtime_stop_timeout_seconds(settings)
    validate_runtime_security(settings)
    await require_runtime_database_boundary(settings)
    context = get_runtime_app_context()
    runtime = context.create_ingestion_runtime(
        settings,
        fail_stop=_fail_stop_after_processing_control_loss,
    )
    application.state.ingestion_runtime = None
    fence: RuntimeCheckpointMaintenanceFence | None = None
    context_initialize_attempted = False
    lark_initialize_attempted = False
    lark_ws_start_attempted = False
    lark_ws_monitor_task: asyncio.Task[None] | None = None
    daily_digest_scheduler: DailyDigestScheduler | None = None
    application.state.daily_digest_scheduler = None
    try:
        if bool(getattr(settings, "DURABLE_INBOX_ENABLED", False)):
            fence = RuntimeCheckpointMaintenanceFence(
                settings.database_url,
                fail_stop=_fail_stop_after_checkpoint_fence_loss,
            )
            await fence.start()
            context_initialize_attempted = True
            initialized_context = initialize_app_context()
            if initialized_context is not context:
                raise RuntimeError("app_context_ownership_mismatch")
            context.bind_checkpoint_write_guard(fence.assert_held)
            await context.setup_async()
            lark_initialize_attempted = True
            lark_app.init_lark_app(
                context.db_manager,
                context.graph,
                context.exchange_client,
                worker_loop_arg=asyncio.get_running_loop(),
                dependencies=context.graph_dependencies,
            )
            # init_lark_app retains its legacy default of enabling intake. No
            # callback may be accepted until the runtime registration, recovery,
            # and Worker startup below have all succeeded.
            lark_app.disable_lark_intake()
            lark_ws_start_attempted = True
            lark_app.start_lark_ws(
                fail_stop=_fail_stop_after_processing_control_loss,
            )
            await _wait_for_lark_ws_connection()
        await runtime.start()
        if bool(getattr(settings, "DURABLE_INBOX_ENABLED", False)):
            if not lark_app.lark_ws_ready():
                raise RuntimeError("lark_ws_unavailable_after_runtime_start")
            lark_ws_monitor_task = asyncio.create_task(
                _monitor_lark_ws_connection(
                    fail_stop=_fail_stop_after_processing_control_loss,
                ),
                name="lark-websocket-connection-monitor",
            )
            lark_app.enable_lark_intake()
            if bool(getattr(settings, "DAILY_DIGEST_ENABLED", False)):
                daily_digest_scheduler = DailyDigestScheduler(
                    database=context.db_manager,
                    account_id=int(settings.EXCHANGE_ACCOUNT_ID),
                    chat_id=settings.LARK_CHAT_ID,
                    health_snapshot=runtime.health_snapshot,
                    max_message_bytes=int(
                        getattr(settings, "DAILY_DIGEST_MESSAGE_MAX_BYTES", 12_000)
                    ),
                    reconciliation_delay_seconds=int(
                        getattr(
                            settings,
                            "DAILY_DIGEST_RECONCILIATION_DELAY_SECONDS",
                            900,
                        )
                    ),
                )
                await daily_digest_scheduler.start()
                application.state.daily_digest_scheduler = daily_digest_scheduler
        application.state.ingestion_runtime = runtime
        yield
    except BaseException as primary_exc:
        try:
            await _shutdown_application_components(
                application,
                context=context,
                runtime=runtime,
                fence=fence,
                context_initialize_attempted=context_initialize_attempted,
                lark_initialize_attempted=lark_initialize_attempted,
                lark_ws_start_attempted=lark_ws_start_attempted,
                lark_ws_monitor_task=lark_ws_monitor_task,
                daily_digest_scheduler=daily_digest_scheduler,
                runtime_stop_seconds=runtime_stop_seconds,
            )
        except BaseException as cleanup_exc:
            logger.critical(
                "Application cleanup failed while preserving primary failure: "
                "primary_error_type=%s cleanup_error_type=%s",
                type(primary_exc).__name__,
                type(cleanup_exc).__name__,
            )
        raise
    else:
        await _shutdown_application_components(
            application,
            context=context,
            runtime=runtime,
            fence=fence,
            context_initialize_attempted=context_initialize_attempted,
            lark_initialize_attempted=lark_initialize_attempted,
            lark_ws_start_attempted=lark_ws_start_attempted,
            lark_ws_monitor_task=lark_ws_monitor_task,
            daily_digest_scheduler=daily_digest_scheduler,
            runtime_stop_seconds=runtime_stop_seconds,
        )


app = FastAPI(
    docs_url="/docs" if _docs_enabled else None,
    redoc_url="/redoc" if _docs_enabled else None,
    openapi_url="/openapi.json" if _docs_enabled else None,
    lifespan=application_lifespan,
)


@app.middleware("http")
async def hide_production_only_surfaces(request: Request, call_next):
    """Hide DEBUG/preview routes before request-body parsing in production."""

    app_env = str(getattr(get_settings(), "APP_ENV", "development")).casefold()
    path = request.url.path
    if app_env == "production" and (
        path.startswith("/debug/") or path.startswith("/email/")
    ):
        return JSONResponse(status_code=404, content={"detail": "Not found"})
    return await call_next(request)
@app.get("/health")
async def health_check():
    """Dependency-free liveness endpoint used by Docker and supervisors."""
    return {
        "status": "ok",
        "version": SERVICE_VERSION,
        "time": datetime.now(UTC).isoformat(),
    }


@app.get("/ready")
async def readiness_check(request: Request):
    """Read-only schema, policy, authority and Web-session readiness."""
    try:
        settings = get_settings()
        validate_runtime_security(settings)
        await _require_cached_runtime_database(settings)
        runtime = getattr(request.app.state, "ingestion_runtime", None)
        check_ready = getattr(runtime, "check_ready", None)
        if not callable(check_ready) or not await check_ready():
            raise ReadinessPreflightError("ingestion_runtime_not_ready")
        if bool(getattr(settings, "DURABLE_INBOX_ENABLED", False)) and not (
            lark_app.lark_ws_ready()
        ):
            raise ReadinessPreflightError("lark_ws_not_ready")
        processing = (
            "active" if bool(getattr(runtime, "processing_ready", False)) else "standby"
        )
        return {"status": "ready", "processing": processing}
    except Exception as exc:
        _log_readiness_failure_once(exc)
        return JSONResponse(
            status_code=503,
            content={"status": "not_ready"},
        )


@app.get("/metrics")
async def metrics_endpoint(request: Request) -> Response:
    """Prometheus scrape endpoint."""
    require_metrics_auth(request, get_settings())
    from src.observability.metrics import record_durable_ingestion, render_metrics

    runtime = getattr(request.app.state, "ingestion_runtime", None)
    stats = None
    ready = False
    try:
        check_ready = getattr(runtime, "check_ready", None)
        queue_stats = getattr(runtime, "queue_stats", None)
        if callable(check_ready):
            ready = bool(await check_ready())
        if callable(queue_stats):
            stats = await queue_stats()
    except Exception as exc:
        logger.warning(
            "Durable ingestion metrics snapshot failed: error_type=%s",
            type(exc).__name__,
        )
        ready = False
        stats = None
    health = _runtime_health_snapshot(runtime)
    record_durable_ingestion(
        stats,
        ready=ready,
        processing_active=health.processing_active,
        polling_active=health.polling_active,
        polling_cursor_ready=health.polling_cursor_ready,
    )

    body, content_type = render_metrics()
    return Response(content=body, media_type=content_type)


@app.get("/queue")
async def queue_status(request: Request):
    """Return the bounded durable Inbox aggregate without identifiers."""

    require_metrics_auth(request, get_settings())
    runtime = getattr(request.app.state, "ingestion_runtime", None)
    check_ready = getattr(runtime, "check_ready", None)
    queue_stats = getattr(runtime, "queue_stats", None)
    try:
        if (
            not callable(check_ready)
            or not callable(queue_stats)
            or not await check_ready()
        ):
            raise ReadinessPreflightError("ingestion_runtime_not_ready")
        stats = await queue_stats()
    except Exception:
        return JSONResponse(
            status_code=503,
            content={"status": "not_ready"},
        )
    health = _runtime_health_snapshot(runtime)
    return {
        "status": "ready",
        "ingress": "active" if health.polling_active else "standby",
        "cursor": "ready" if health.polling_cursor_ready else "activating",
        "session": "active",
        "processing": "active" if health.processing_active else "standby",
        "queue": {
            "pending": stats.pending,
            "retry_wait": stats.retry_wait,
            "leased": stats.leased,
            "manual_review": stats.manual_review,
            "dead_letter": stats.dead_letter,
            "oldest_pending_seconds": stats.oldest_pending_seconds,
        },
    }


class MockEmailData(BaseModel):
    id: str = Field(min_length=1, max_length=512)
    subject: str = Field(max_length=998)
    sender: str = Field(max_length=1_024)
    to: List[str] = Field(min_length=1, max_length=100)
    cc: List[str] = Field(default_factory=list, max_length=100)
    body: str = Field(max_length=DEBUG_BODY_MAX_CHARS)
    received_at: str = Field(max_length=128)
    attachments: List[Dict[str, Any]] = Field(default_factory=list, max_length=20)
    draft: str = Field(default="", max_length=DEBUG_BODY_MAX_CHARS)
    context: List[Dict[str, Any]] = Field(default_factory=list, max_length=20)
    classification: Dict[str, Any] = Field(default_factory=dict)
    attachment_tokens: List[str] = Field(default_factory=list, max_length=20)
    pdf_token: Optional[str] = Field(default=None, max_length=512)
    recipient_candidates: Dict[str, List[Any]] = Field(
        default_factory=lambda: {"to": [], "cc": []}
    )


def _require_debug_endpoint(settings: Any) -> None:
    if str(getattr(settings, "APP_ENV", "development")).casefold() == "production":
        raise HTTPException(status_code=404, detail="Not found")
    if not bool(getattr(settings, "DEBUG", False)):
        raise HTTPException(status_code=403, detail="Debug endpoints disabled")


@app.post("/debug/inject_email")
async def inject_test_email(data: MockEmailData):
    """
    Inject a test email into the in-memory mock store for viewing.
    """
    settings = get_settings()
    _require_debug_endpoint(settings)
    if not lark_app.is_test_card_id(data.id):
        raise HTTPException(
            status_code=400,
            detail="Debug email id must use the test_push_ namespace",
        )

    logger.info(
        "Injecting DEBUG mock email: email=%s",
        fingerprint_identifier(data.id, namespace="debug_email"),
    )

    # Construct state-like object
    # The view_email function expects state.values.get("email")
    # So we structure it accordingly.

    mock_state = type("MockState", (), {})()
    email_data = {
        "id": data.id,
        "subject": data.subject,
        "sender": data.sender,
        "to": data.to,
        "cc": data.cc,
        "draft_to": list(data.to),
        "draft_cc": list(data.cc),
        "body": data.body,
        "received_at": data.received_at,
        "attachments": data.attachments,
    }
    mock_state.values = {
        "email": email_data,
        "draft": data.draft,
        "context": data.context,
        "classification": data.classification
        or {
            "need_reply": True,
            "reasoning": "debug_injection",
        },
        "attachment_tokens": data.attachment_tokens,
        "pdf_token": data.pdf_token,
        "recipient_candidates": data.recipient_candidates,
    }

    lark_app._mock_store[data.id] = mock_state
    return {"status": "ok", "id": data.id}


@app.delete("/debug/inject_email/{email_id:path}")
async def delete_test_email(email_id: str):
    """Remove only an explicitly namespaced DEBUG test-card state."""
    settings = get_settings()
    _require_debug_endpoint(settings)
    if not lark_app.is_test_card_id(email_id):
        raise HTTPException(
            status_code=400,
            detail="Debug email id must use the test_push_ namespace",
        )
    removed = lark_app._mock_store.pop(email_id, None) is not None
    return {
        "status": "ok",
        "id": email_id,
        "removed": removed,
    }


@app.get("/email/{email_id:path}", response_class=HTMLResponse)
async def view_email(email_id: str):
    """Render only an explicitly seeded DEBUG test card until Phase 5."""
    settings = get_settings()
    is_explicit_debug_email = (
        bool(settings.DEBUG)
        and str(getattr(settings, "APP_ENV", "development")).casefold() != "production"
        and lark_app.is_test_card_id(email_id)
        and email_id in lark_app._mock_store
    )
    if not is_explicit_debug_email:
        raise HTTPException(status_code=404, detail="Not found")

    state = lark_app._mock_store[email_id]
    email_data = state.values.get("email", {})

    # Use shared renderer
    from src.utils.email_renderer import render_email_html

    full_email_html = render_email_html(email_data)

    return HTMLResponse(
        content=full_email_html,
        headers={
            "Cache-Control": "no-store",
            "Referrer-Policy": "no-referrer",
            "X-Content-Type-Options": "nosniff",
        },
    )
