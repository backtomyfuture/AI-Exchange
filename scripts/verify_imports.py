
import sys
import os

# Add src to path
sys.path.append(os.getcwd())

print("Verifying imports...")

try:
    print("Checking src.utils.llm_factory...")
    from src.utils.llm_factory import LLMFactory
    print("PASS: src.utils.llm_factory")
    
    print("Checking src.utils.email_processor...")
    from src.utils.email_processor import EmailProcessor
    print("PASS: src.utils.email_processor")

    print("Checking src.nodes.sender...")
    from src.nodes.sender import send_final_email
    print("PASS: src.nodes.sender")

    print("Checking src.nodes.categorizer...")
    from src.nodes.categorizer import categorize_email
    print("PASS: src.nodes.categorizer")

    print("Checking src.nodes.drafter...")
    from src.nodes.drafter import generate_draft
    print("PASS: src.nodes.drafter")

    print("Checking src.utils.lark_app...")
    from src.utils import lark_app
    print("PASS: src.utils.lark_app")

    print("ALL MODULES IMPORTED SUCCESSFULLY.")

except Exception as e:
    print(f"FAIL: Import error: {e}")
    sys.exit(1)
