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

def main():
    logger.info("Loading environment variables...")
    load_dotenv()
    
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

    # Construct Complex Mock Data
    complex_html = """
    <html>
    <body>
        <h1 style="color: #D35400;">🚀 Flight Status Update</h1>
        <p>Dear Customer, here is your <strong>travel itinerary</strong>:</p>
        
        <h3>Flight Details</h3>
        <table border="1">
            <tr>
                <th>Flight</th>
                <th>Departure</th>
                <th>Arrival</th>
                <th>Status</th>
            </tr>
            <tr>
                <td>SQ321</td>
                <td>Singapore (SIN)</td>
                <td>London (LHR)</td>
                <td>On Time</td>
            </tr>
        </table>

        <ul>
            <li>Check-in: <strong>Open</strong></li>
            <li>Gate: B42</li>
        </ul>
        
        <p>Weather at destination:</p>
        <img src="cid:weather_icon" alt="Sunny" />
        
        <p>View full details <a href="https://example.com/mytrip">here</a>.</p>
    </body>
    </html>
    """
    
    email_data = {
        "id": "test_push_REAL_USER",
        "subject": "TEST: Complex Email Rendering",
        "sender": "name='System', email_address='q-fu@tianjin-air.com'",
        "to": ["name='Jarod', email_address='q-fu@tianjin-air.com'", "name='Zhang', email_address='yy-zhang1@tianjin-air.com'"],
        "cc": ["name='Jarod-CC', email_address='q-fu@tianjin-air.com'", "name='Zhang-CC', email_address='yy-zhang1@tianjin-air.com'", "name='Extra', email_address='q-fu@tianjin-air.com'"],
        "received_at": "2023-10-30 14:30:00",
        "body": complex_html,
        "attachments": [
            {"name": "itinerary.pdf"},
            {"name": "invoice_scan.jpg"},
            {"name": "meeting_notes.docx"}
        ]
    }   
    
    classification = {
        "reasoning": "This is a test notification sent manually."
    }
    
    draft = "Thank you, I have received the update."
    
    logger.info(f"Sending card to Chat ID: {chat_id}...")
    
    try:
        lark_app.send_approval_card(
            email_id=email_data['id'],
            draft=draft,
            context=[{"chunk_text": "Flight details preview..."}],
            email_data=email_data,
            classification=classification
        )
        logger.info("✅ Card sent successfully! Please check your Lark client.")
    except Exception as e:
        logger.error(f"❌ Failed to send card: {e}")

if __name__ == "__main__":
    main()
