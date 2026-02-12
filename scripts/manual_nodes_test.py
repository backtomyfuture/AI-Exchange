
import os
import logging
from dotenv import load_dotenv
from src.nodes.categorizer import categorize_email
from src.nodes.drafter import generate_draft

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import asyncio

async def test_nodes():
    # Mock state
    state = {
        "email": {
            "subject": "Test Email",
            "body": "This is a test email to check if the system can categorize and draft a response.",
            "sender": "test@example.com"
        },
        "context": [],
        "classification": {},
        "draft": "",
        "approval_status": "pending",
        "next_step": ""
    }

    print("Testing Categorizer...")
    try:
        new_state = await categorize_email(state)
        print("Categorization Result:", new_state.get("classification"))
    except Exception as e:
        print(f"Categorizer failed: {e}")
        new_state = state # Fallback for next step

    print("\nTesting Drafter...")
    try:
        # Drafter needs context from retrieval usually, but we'll test basic generation
        new_state = await generate_draft(new_state)
        print("Draft Result:", new_state.get("draft")[:200] + "...")
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Drafter failed: {e}")

if __name__ == "__main__":
    asyncio.run(test_nodes())
