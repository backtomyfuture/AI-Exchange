import asyncio
import time
import logging

logger = logging.getLogger(__name__)

class AsyncRateLimiter:
    """
    令牌桶限流器 (Token Bucket) 的异步实现，用于控制 LLM 调用频率。
    """
    def __init__(self, rpm: float):
        """
        :param rpm: 每分钟最大请求数 (Requests Per Minute)
        """
        self.rpm = rpm
        self.interval = 60.0 / rpm if rpm > 0 else 0
        self.last_request_time = 0
        self._lock = asyncio.Lock()

    async def acquire(self):
        """
        获取请求许可。如果频率过高，将异步等待。
        """
        if self.rpm <= 0:
            return

        async with self._lock:
            current_time = asyncio.get_event_loop().time()
            elapsed = current_time - self.last_request_time
            
            wait_time = self.interval - elapsed
            if wait_time > 0:
                logger.info(f"RateLimiter: Sleeping for {wait_time:.2f}s to respect {self.rpm} RPM limit.")
                await asyncio.sleep(wait_time)
                # 更新当前时间，因为 sleep 了
                self.last_request_time = asyncio.get_event_loop().time()
            else:
                self.last_request_time = current_time

# 全局单例
# 默认为 15 RPM (Gemini Flash Free Tier limit)
import os
default_rpm = float(os.getenv("LLM_MAX_RPM", "12")) # 保守起见设为 12
llm_rate_limiter = AsyncRateLimiter(default_rpm)
