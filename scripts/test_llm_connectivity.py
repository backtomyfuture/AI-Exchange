import asyncio
import os
import time
import logging
from dotenv import load_dotenv
from openai import AsyncOpenAI
import httpx

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_BASE = os.getenv("OPENAI_API_BASE")
LLM_MODEL = os.getenv("LLM_MODEL", "gemini-3-flash")

async def test_connectivity():
    """Test basic connectivity to the LLM."""
    base_url = OPENAI_API_BASE
    # Local fallback: if host.docker.internal fails, try localhost
    if "host.docker.internal" in base_url:
        logger.info("Detected docker internal URL, will try localhost fallback if needed.")
    
    logger.info(f"Testing connectivity to {base_url} with model {LLM_MODEL}...")
    
    async def try_request(url):
        client = AsyncOpenAI(
            api_key=OPENAI_API_KEY,
            base_url=url,
            timeout=10.0
        )
        try:
            start_time = time.time()
            response = await client.chat.completions.create(
                model=LLM_MODEL,
                messages=[{"role": "user", "content": "hi"}],
                max_tokens=5
            )
            duration = time.time() - start_time
            logger.info(f"✅ Success with {url}! Latency: {duration:.2f}s")
            logger.info(f"Response: {response.choices[0].message.content.strip()}")
            return True, url
        except Exception as e:
            logger.error(f"❌ Failed with {url}: {e}")
            return False, str(e)

    success, result = await try_request(base_url)
    if not success and "host.docker.internal" in base_url:
        alt_url = base_url.replace("host.docker.internal", "localhost")
        logger.info(f"Retrying with localhost fallback: {alt_url}")
        success, result = await try_request(alt_url)
        if success:
            base_url = alt_url
    
    return success, base_url

async def test_rate_limits(base_url, num_requests=5):
    """Attempt concurrent requests to check for rate limits and headers."""
    logger.info(f"Attempting {num_requests} concurrent requests to probe rate limits using {base_url}...")
    
    client = AsyncOpenAI(
        api_key=OPENAI_API_KEY,
        base_url=base_url
    )
    
    async def make_request(i):
        try:
            start_time = time.time()
            response = await client.chat.completions.with_raw_response.create(
                model=LLM_MODEL,
                messages=[{"role": "user", "content": f"ping {i}"}],
                max_tokens=5
            )
            duration = time.time() - start_time
            headers = response.headers
            
            rl_info = {k: v for k, v in headers.items() if "ratelimit" in k.lower()}
            logger.info(f"Request {i} success ({duration:.2f}s). RateLimit Headers: {rl_info}")
            return True, rl_info
        except Exception as e:
            logger.warning(f"Request {i} failed: {e}")
            return False, str(e)

    tasks = [make_request(i) for i in range(num_requests)]
    results = await asyncio.gather(*tasks)
    
    success_count = sum(1 for r in results if r[0])
    logger.info(f"Rate limit test finished. Successful: {success_count}/{num_requests}")
    
    for success, info in results:
        if success and info:
            print(f"\n--- Observed Rate Limits (from {base_url}) ---")
            for k, v in info.items():
                print(f"{k}: {v}")
            break
    else:
        print("\nNo rate limit headers found in successful responses.")

async def main():
    if not OPENAI_API_KEY or not OPENAI_API_BASE:
        logger.error("Missing OPENAI_API_KEY or OPENAI_API_BASE in .env")
        return

    is_ok, effective_url = await test_connectivity()
    if is_ok:
        await test_rate_limits(effective_url, num_requests=5)

if __name__ == "__main__":
    asyncio.run(main())
