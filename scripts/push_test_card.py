import os
import sys
import json
import logging
from dotenv import load_dotenv

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.utils import lark_app

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TestPush")

import asyncio

async def main():
    logger.info("Loading environment variables...")
    load_dotenv()
    
    # Simulate "Me" for testing Layout
    # os.environ["EXCHANGE_ACCOUNT_EMAIL"] = "q-fu@tianjin-air.com" # REMOVED: Testing dynamic resolution
    from src.config import get_settings
    get_settings.cache_clear() # Ensure we get fresh settings
    
    app_id = os.getenv("LARK_APP_ID")
    chat_id = os.getenv("LARK_CHAT_ID")
    
    if not app_id or not chat_id:
        logger.error("Error: LARK_APP_ID or LARK_CHAT_ID not found in environment.")
        return

    logger.info(f"Initializing Lark Client for App ID: {app_id[:5]}***")
    
    # Initialize only what's needed for sending
    # We pass None for DB/Graph/Exchange as we only test card sending
    lark_app.init_lark_app(None, None, None)
    
    if not lark_app.lark_api_client:
        logger.error("Failed to initialize Lark API Client.")
        return

    # Process EML File
    eml_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'tests', 'fixtures', 'nas.eml'))
    
    if not os.path.exists(eml_path):
        logger.error(f"EML file not found: {eml_path}")
        return

    import email
    from email import policy
    from email.header import decode_header

    def decode_str(header_value):
        if not header_value:
            return ""
        decoded_list = decode_header(header_value)
        result = ""
        for bytes_data, encoding in decoded_list:
            if isinstance(bytes_data, bytes):
                if encoding:
                    try:
                        result += bytes_data.decode(encoding)
                    except:
                        result += bytes_data.decode('gb18030', errors='replace')
                else:
                     result += bytes_data.decode('utf-8', errors='replace')
            else:
                result += str(bytes_data)
        return result

    logger.info(f"Parsing EML file: {eml_path}")
    with open(eml_path, 'rb') as f:
        msg = email.message_from_binary_file(f, policy=policy.default)

    subject = decode_str(msg['subject'])
    sender = decode_str(msg['from'])
    to_list = [decode_str(x).strip() for x in str(msg['to']).split(',') if x]
    cc_list = [decode_str(x).strip() for x in str(msg['cc']).split(',') if x]
    
    # Extract Body
    body_content = ""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == 'text/html':
                try:
                    body_content = part.get_content()
                except:
                    body_content = part.get_payload(decode=True).decode('gb18030', errors='replace')
                break # Prefer HTML
    if not body_content: # Fallback to text
         for part in msg.walk():
            if part.get_content_type() == 'text/plain':
                try:
                    body_content = part.get_content()
                except:
                    body_content = part.get_payload(decode=True).decode('gb18030', errors='replace')
                break

    # Extract Attachments & Images
    attachments = []
    
    import base64
    
    for part in msg.walk():
        if part.get_content_maintype() == 'multipart':
            continue
        if part.get_content_disposition() is None and part.get('Content-ID') is None:
            continue
            
        filename = part.get_filename() or "untitled"
        filename = decode_str(filename)
        
        # Get Content
        content_bytes = part.get_payload(decode=True)
        if not content_bytes:
            continue
            
        # Get Content-ID (strip angle brackets)
        content_id = part.get('Content-ID', '').strip('<>')
        
        logger.info(f"Found attachment/image: {filename} (CID: {content_id}, {len(content_bytes)} bytes)")
        
        # Upload to Lark (for card link)
        res = lark_app.upload_file_to_drive(filename, content_bytes, len(content_bytes))
        
        # Prepare data for server injection (so View Original works)
        # We need to pass the base64 string so the server can embed it
        content_b64 = base64.b64encode(content_bytes).decode('utf-8')
        
        att_data = {
            "name": filename,
            "content_id": content_id,
            "content": content_b64,  # IMPORTANT: Send content to server
            "size": len(content_bytes)
        }
        
        if res:
            logger.info(f"Upload success: {res['url']}")
            att_data["lark_file_url"] = res['url']
            att_data["lark_file_token"] = res['file_token']
        else:
            logger.warning(f"Upload failed for {filename}")
            
        attachments.append(att_data)

    email_data = {
        "id": "test_push_REAL_EML",
        "subject": subject,
        "sender": sender,
        "to": to_list,
        "cc": cc_list,
        "received_at": str(msg['date']),
        "body": body_content if body_content else "No body content found.",
        "attachments": attachments
    }
    
    classification = {
        "reasoning": "This is a test notification sent manually."
    }
    
    draft = "Thank you, I have received the update."

    # Inject into Server for "View Original" to work
    try:
        import requests
        external_url = os.getenv("EXTERNAL_URL", "http://localhost:8000")
        debug_url = f"{external_url}/debug/inject_email"
        logger.info(f"Injecting mock email to server: {debug_url}")
        requests.post(debug_url, json=email_data, timeout=5)
        logger.info("✅ Mock email injected successfully.")
    except Exception as e:
        logger.warning(f"Failed to inject mock email (View Original might fail): {e}")
    
    logger.info(f"Generating PDF for email {email_data['id']}...")
    try:
        pdf_url = await lark_app.generate_and_upload_pdf(email_data['id'], email_data)
        logger.info(f"PDF Generated: {pdf_url}")
    except Exception as e:
        logger.error(f"PDF Generation failed: {e}")
        pdf_url = None

    logger.info(f"Sending card to Chat ID: {chat_id}...")
    
    try:
        lark_app.send_approval_card(
            email_id=email_data['id'],
            draft=draft,
            context=[{"chunk_text": "Flight details preview..."}],
            email_data=email_data,
            classification=classification,
            pdf_url=pdf_url
        )
        logger.info("✅ Card sent successfully! Please check your Lark client.")
    except Exception as e:
        logger.error(f"❌ Failed to send card: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
