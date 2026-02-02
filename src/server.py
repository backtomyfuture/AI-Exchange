import logging
import os
import html
import re
from fastapi import FastAPI, HTTPException
from fastapi import FastAPI, HTTPException, Body
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
from src.init_app import get_app_context
from src.utils import lark_app

logger = logging.getLogger("WebServer")

app = FastAPI()

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
