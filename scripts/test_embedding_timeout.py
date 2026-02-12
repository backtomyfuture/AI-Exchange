
import sys
import os
import time
from typing import List

# Add src to path
sys.path.append(os.getcwd())

from openai import OpenAI, APIError, APIConnectionError
from tenacity import retry, stop_after_attempt, wait_random_exponential

# Mock settings
API_BASE = "https://api.siliconflow.cn/v1"
API_KEY = "sk-mock-key" # We don't need a real key to test timeout/500 if the service is down/slow
MODEL = "Qwen/Qwen3-Embedding-4B"

def test_timeout():
    print(f"Testing embedding with 5s timeout...")
    
    client = OpenAI(
        base_url=API_BASE,
        api_key=API_KEY,
        timeout=5.0
    )
    
    start_time = time.time()
    try:
        # We expect this to either fail efficiently (500) or timeout (if network hangs)
        # If the service returns 500 immediately, it's fast.
        # If the service hangs, it should stop at 5s.
        response = client.embeddings.create(
            input="test string",
            model=MODEL
        )
        print("Success (unexpected if service is down)")
    except Exception as e:
        duration = time.time() - start_time
        print(f"Caught exception: {type(e).__name__}: {e}")
        print(f"Duration: {duration:.2f}s")
        if duration > 6.0:
            print("❌ FAIL: Timeout didn't trigger fast enough")
        else:
            print("✅ PASS: Failed fast")

@retry(stop=stop_after_attempt(2), wait=wait_random_exponential(multiplier=1, max=10))
def test_retry_logic(client):
    print("\nTesting retry logic (max 2 attempts)...")
    start_time = time.time()
    try:
        client.embeddings.create(
            input="test string",
            model=MODEL
        )
    except Exception:
        print(f"Attempt failed at {time.time() - start_time:.2f}s")
        raise

if __name__ == "__main__":
    test_timeout()
    
    # Test retry wrapper
    client = OpenAI(
        base_url=API_BASE,
        api_key=API_KEY,
        timeout=5.0
    )
    
    start_time = time.time()
    try:
        test_retry_logic(client)
    except Exception as e:
        total_duration = time.time() - start_time
        print(f"\nTotal Retry Duration: {total_duration:.2f}s")
        # 2 attempts:
        # 1. Start (0s) -> Fail (fast or 5s)
        # 2. Add retry wait (random 0-10s, exponential)
        # 3. Retry (fail fast or 5s)
        # Max theoretical if timeouts: 5s + 10s + 5s = 20s
        # Max theoretical if 500 immediate: 0s + 10s + 0s = 10s
        # Previous was: 5 attempts, max 60s wait. Could be minutes.
        
        if total_duration < 30:
            print("✅ PASS: Retry loop exited reasonably fast")
        else:
            print("❌ FAIL: Retry loop took too long")
