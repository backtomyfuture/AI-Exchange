import hashlib
import hmac
import html
import json
import logging
import re
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
from src.config import get_settings, resolve_secret
from src.utils import lark_app

logger = logging.getLogger("WebServer")

app = FastAPI()


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

    return await exchange_service.enqueue_webhook_event(payload, header_event=header_event)


@app.get("/health")
async def health_check():
    """
    服务健康检查 endpoint，用于 Docker healthcheck 和外部监控。
    """
    try:
        ctx = get_app_context()

        # DB ping
        db_ok = False
        try:
            async with ctx.db_manager.get_connection() as conn:
                await conn.execute("SELECT 1")
                db_ok = True
        except Exception:
            pass

        # Queue depth
        from src.exchange_service import _webhook_queue, WEBHOOK_QUEUE_MAXSIZE
        queue_depth = _webhook_queue.qsize() if _webhook_queue else 0
        queue_capacity = (
            _webhook_queue.maxsize if _webhook_queue and _webhook_queue.maxsize
            else WEBHOOK_QUEUE_MAXSIZE
        )

        # Circuit breaker
        from src.utils.circuit_breaker import circuit_breaker
        cb_open = circuit_breaker.is_open

        checks = {
            "db_ping": db_ok,
            "graph": ctx.graph is not None,
            "lark_client": lark_app.lark_api_client is not None,
            "circuit_breaker_open": cb_open,
        }

        circuit_breaker_state = {
            "open": cb_open,
            "failure_count": circuit_breaker.failure_count,
            "failure_threshold": circuit_breaker.failure_threshold,
            "window_seconds": circuit_breaker.window_seconds,
            "last_error": circuit_breaker.last_error,
        }

        healthy = db_ok and ctx.graph is not None and not cb_open

        return JSONResponse(
            status_code=200 if healthy else 503,
            content={
                "status": "healthy" if healthy else "degraded",
                "checks": checks,
                "queue_depth": queue_depth,
                "queue_capacity": queue_capacity,
                "circuit_breaker": circuit_breaker_state,
            }
        )
    except Exception as e:
        logger.error(f"Health check error: {e}")
        return JSONResponse(
            status_code=503,
            content={"status": "error", "message": str(e)}
        )


@app.get("/metrics")
async def metrics_endpoint() -> Response:
    """Prometheus scrape endpoint."""
    from src.observability.metrics import render_metrics
    body, content_type = render_metrics()
    return Response(content=body, media_type=content_type)


@app.post("/webhooks/exchange")
async def exchange_webhook(request: Request):
    """
    Exchange NewMail Webhook endpoint with HMAC-SHA256 signature verification.
    """
    signature = request.headers.get("X-Webhook-Signature") or request.headers.get("X-Exchange-Signature")
    header_event = request.headers.get("X-Exchange-Event")
    logger.info(f"Received webhook request: method={request.method} headers={dict(request.headers)}")

    if not signature:
        logger.warning("Missing X-Webhook-Signature in webhook request")
        raise HTTPException(status_code=400, detail="Missing signature")

    # Strip the 'sha256=' prefix if present (sent by Exchange server)
    if signature.startswith("sha256="):
        signature = signature[len("sha256="):]

    settings = get_settings()
    webhook_secret = resolve_secret(settings.EXCHANGE_WEBHOOK_SECRET)
    if not webhook_secret:
        raise HTTPException(status_code=503, detail="Webhook secret not configured")

    body_bytes = await request.body()
    expected_signature = hmac.new(
        webhook_secret.encode("utf-8"),
        body_bytes,
        hashlib.sha256,
    ).hexdigest()

    if not hmac.compare_digest(signature, expected_signature):
        raise HTTPException(status_code=403, detail="Invalid signature")

    try:
        payload = json.loads(body_bytes.decode("utf-8"))
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON payload: {body_bytes.decode('utf-8')} ({e})")
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    try:
        result = await enqueue_exchange_webhook(payload, header_event=header_event)
        logger.info(
            "Exchange webhook routed: event_header=%s event_payload=%s item_id=%s parent_folder_id=%s queued=%s reason=%s route=%s folder=%s",
            header_event,
            payload.get("event_type") or payload.get("event"),
            payload.get("item_id"),
            payload.get("parent_folder_id"),
            result.get("queued"),
            result.get("reason"),
            result.get("route"),
            result.get("folder"),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Failed to process Exchange webhook: {e}")
        raise HTTPException(status_code=502, detail="Failed to process webhook event")

    if result.get("reason") == "queue_full":
        raise HTTPException(
            status_code=503,
            detail={"status": "queue_full", **result},
        )

    return {"status": "ok", **result}


def _format_address_str(raw_str: str) -> str:
    """Format address string like 'name=..., email=...' to 'Name <email>'"""
    if not raw_str:
        return ""
    try:
        # Check for our project's specific string format
        m = re.search(r"name=['\"](.*?)['\"],?\s*email_address=['\"](.*?)['\"]", str(raw_str))
        if m:
            name, email = m.groups()
            return f"{html.escape(name)} &lt;{html.escape(email)}&gt;"
        
        # Check for standard "Name <email>" format
        m2 = re.search(r"(.*?) <(.*?)>", str(raw_str))
        if m2:
            return f"{html.escape(m2.group(1).strip())} &lt;{html.escape(m2.group(2).strip())}&gt;"

        return html.escape(str(raw_str))
    except Exception:
        return html.escape(str(raw_str))

class MockEmailData(BaseModel):
    id: str
    subject: str
    sender: str
    to: List[str]
    cc: List[str] = []
    body: str
    received_at: str
    attachments: List[Dict[str, Any]] = []

@app.post("/debug/inject_email")
async def inject_test_email(data: MockEmailData):
    """
    Inject a test email into the in-memory mock store for viewing.
    """
    settings = get_settings()
    if not settings.DEBUG:
        raise HTTPException(status_code=403, detail="Debug endpoints disabled in production")

    logger.info(f"Injecting mock email: {data.id}")
    
    # Construct state-like object
    # The view_email function expects state.values.get("email")
    # So we structure it accordingly.
    
    mock_state = type('MockState', (), {})()
    mock_state.values = {
        "email": {
            "id": data.id,
            "subject": data.subject,
            "sender": data.sender,
            "to": data.to,
            "cc": data.cc,
            "body": data.body,
            "received_at": data.received_at,
            "attachments": data.attachments
        }
    }
    
    lark_app._mock_store[data.id] = mock_state
    return {"status": "ok", "id": data.id}

@app.get("/email/{email_id:path}", response_class=HTMLResponse)
async def view_email(email_id: str):
    """
    Serve the email content as Outlook-style HTML.
    """
    app_ctx = get_app_context()
    
    # 1. Try to get state from Graph
    if not app_ctx.graph:
        logger.error("Graph not initialized.")
        raise HTTPException(status_code=503, detail="Service not ready")

    config = {"configurable": {"thread_id": email_id}}
    state = None
    
    # Check for test card
    if str(email_id).startswith("test_push_"):
        if email_id in lark_app._mock_store:
             state = lark_app._mock_store[email_id]
        else:
             # Fallback for cross-process test
             if email_id == "test_push_REAL_USER":
                 return HTMLResponse("""
                 <html><body>
                 <div style="padding: 20px; font-family: sans-serif;">
                     <h1>🚀 Flight Status Update [TEST FALLBACK]</h1>
                     <p>This is a test email content served from the server fallback.</p>
                     <p><b>Sender:</b> System &lt;q-fu@tianjin-air.com&gt;</p>
                     <p><b>Subject:</b> TEST: Complex Email Rendering</p>
                     <p>If you see this, the Web View link is working!</p>
                 </div>
                 </body></html>
                 """)
             return HTMLResponse("<h1>Test Card Not Found in Memory</h1>")
    else:
        try:
             state = await app_ctx.graph.aget_state(config)
        except Exception as e:
             logger.error(f"Error getting state: {e}")
             raise HTTPException(status_code=500, detail="Internal Error")

    if not state or not state.values:
        raise HTTPException(status_code=404, detail="Email not found or session expired")
        
    email_data = state.values.get("email", {})
    
    # Use shared renderer
    from src.utils.email_renderer import render_email_html
    full_email_html = render_email_html(email_data)
    
    return full_email_html
