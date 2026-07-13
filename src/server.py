import asyncio
import hashlib
import hmac
import json
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version
from weakref import WeakKeyDictionary
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
from src.config import get_settings, resolve_secret
from src.db.schema import require_runtime_database
from src.safety.input_limits import input_limits_from_settings
from src.security.auth import require_metrics_auth, validate_runtime_security
from src.security.redaction import fingerprint_identifier, safe_log_metadata
from src.utils import lark_app

logger = logging.getLogger("WebServer")
DEBUG_BODY_MAX_CHARS = 1_048_576
_READINESS_SUCCESS_TTL_SECONDS = 5.0
_READINESS_FAILURE_TTL_SECONDS = 1.0
_READINESS_DATABASE_TIMEOUT_SECONDS = 5.0
_READINESS_FAILURE_LOG_TTL_SECONDS = 5.0


class ReadinessPreflightError(RuntimeError):
    """Safe cached failure for a recently failed database preflight."""


_ReadinessContract = tuple[bytes, bool, bool, bool, bool, str, str, str, str, str]


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
        bool(getattr(settings, "INGESTION_SHADOW_ENABLED", False)),
        bool(getattr(settings, "SYNC_RECONCILIATION_ENABLED", False)),
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
                    ingestion_shadow_enabled=contract[2],
                    sync_reconciliation_enabled=contract[3],
                    role_separation_required=contract[4],
                    expected_runtime_role=contract[5],
                    expected_migration_role=contract[6],
                    expected_maintenance_role=contract[7],
                    expected_auditor_role=contract[8],
                    target_schema=contract[9],
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


@asynccontextmanager
async def secure_service_lifespan(application: FastAPI):
    """Make the exported server app use the same guarded unified runtime."""

    validate_runtime_security(get_settings())
    from src.main import lifespan as unified_lifespan

    async with unified_lifespan(application):
        yield


app = FastAPI(
    docs_url="/docs" if _docs_enabled else None,
    redoc_url="/redoc" if _docs_enabled else None,
    openapi_url="/openapi.json" if _docs_enabled else None,
    lifespan=secure_service_lifespan,
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


def _get_app_context():
    """
    Lazy import to avoid loading heavy graph dependencies during lightweight API tests.
    """
    from src.init_app import get_app_context

    return get_app_context()


def get_app_context():
    """Backward-compatible alias for tests and legacy imports."""
    return _get_app_context()


async def enqueue_exchange_webhook(
    payload: Dict[str, Any],
    header_event: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Forward webhook payload into exchange worker queue.
    """
    from src import exchange_service

    return await exchange_service.enqueue_webhook_event(
        payload, header_event=header_event
    )


@app.get("/health")
async def health_check():
    """Dependency-free liveness endpoint used by Docker and supervisors."""
    return {
        "status": "ok",
        "version": SERVICE_VERSION,
        "time": datetime.now(UTC).isoformat(),
    }


@app.get("/ready")
async def readiness_check():
    """Read-only database/schema readiness without leaking failure details."""
    try:
        settings = get_settings()
        validate_runtime_security(settings)
        await _require_cached_runtime_database(settings)
        return {"status": "ready"}
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
    from src.observability.metrics import render_metrics

    body, content_type = render_metrics()
    return Response(content=body, media_type=content_type)


@app.post("/webhooks/exchange")
async def exchange_webhook(request: Request):
    """
    Exchange NewMail Webhook endpoint with HMAC-SHA256 signature verification.
    """
    settings = get_settings()
    signature = request.headers.get("X-Webhook-Signature") or request.headers.get(
        "X-Exchange-Signature"
    )
    if not signature:
        logger.warning("Missing X-Webhook-Signature in webhook request")
        raise HTTPException(status_code=400, detail="Missing signature")

    # Strip the 'sha256=' prefix if present (sent by Exchange server)
    if signature.startswith("sha256="):
        signature = signature[len("sha256=") :]

    webhook_secret = resolve_secret(settings.EXCHANGE_WEBHOOK_SECRET)
    if not webhook_secret:
        raise HTTPException(status_code=503, detail="Webhook secret not configured")

    media_type = request.headers.get("Content-Type", "").partition(";")[0]
    if media_type.strip().casefold() != "application/json":
        raise HTTPException(
            status_code=415,
            detail="Content-Type must be application/json",
        )

    max_bytes = input_limits_from_settings(settings).webhook_bytes
    body_parts: list[bytes] = []
    body_size = 0
    async for chunk in request.stream():
        body_size += len(chunk)
        if body_size > max_bytes:
            raise HTTPException(status_code=413, detail="Webhook payload too large")
        body_parts.append(chunk)
    body_bytes = b"".join(body_parts)
    header_event = request.headers.get("X-Exchange-Event")

    expected_signature = hmac.new(
        webhook_secret.encode("utf-8"),
        body_bytes,
        hashlib.sha256,
    ).hexdigest()

    if not hmac.compare_digest(signature, expected_signature):
        raise HTTPException(status_code=403, detail="Invalid signature")

    try:
        payload = json.loads(body_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        logger.warning("Exchange webhook payload is not valid JSON")
        raise HTTPException(status_code=400, detail="Invalid JSON payload") from None

    if not isinstance(payload, dict):
        logger.warning("Exchange webhook payload root is not an object")
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    payload_event = payload.get("event_type") or payload.get("event")
    if header_event and (
        not isinstance(payload_event, str)
        or not hmac.compare_digest(header_event, payload_event)
    ):
        logger.warning("Rejected Exchange webhook event-header mismatch")
        raise HTTPException(status_code=400, detail="Webhook event mismatch")

    try:
        result = await enqueue_exchange_webhook(payload, header_event=header_event)
        if not isinstance(result, dict):
            raise RuntimeError("invalid_webhook_enqueue_result")
        outcome = safe_log_metadata(
            "queue_full" if result.get("reason") == "queue_full" else "accepted",
            allowed_values={"accepted", "queue_full"},
        )
        logger.info(
            "Exchange webhook routed: queued=%s outcome=%s",
            bool(result.get("queued")),
            outcome,
        )
    except ValueError as exc:
        logger.warning(
            "Rejected invalid Exchange webhook event: error_type=%s",
            type(exc).__name__,
        )
        raise HTTPException(status_code=400, detail="Invalid webhook event") from None
    except Exception as exc:
        logger.error(
            "Failed to process Exchange webhook: error_type=%s",
            type(exc).__name__,
        )
        raise HTTPException(
            status_code=502,
            detail="Failed to process webhook event",
        ) from None

    if result.get("reason") == "queue_full":
        raise HTTPException(
            status_code=503,
            detail={"status": "queue_full", "reason": "queue_full"},
        )

    return {"status": "ok", "queued": bool(result.get("queued"))}


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
