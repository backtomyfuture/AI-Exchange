"""
通用 LLM 重试装饰器模块

提供带速率限制 + 熔断器集成的 LLM 调用重试逻辑，减少代码重复。
"""

from functools import wraps
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type
from openai import RateLimitError, APIError, APIConnectionError

from src.utils.rate_limiter import llm_rate_limiter


class CircuitOpenError(RuntimeError):
    """Raised when the LLM circuit breaker is currently open."""


def with_llm_retry(
    max_attempts: int = 3,
    max_wait: int = 120,
    base_wait: int = 2,
):
    """
    通用 LLM 调用重试装饰器。

    行为:
    1. 调用前检查熔断器，若 OPEN 则直接抛出 ``CircuitOpenError``，避免无谓重试。
    2. 重试结束后仍失败：上报 ``circuit_breaker.report_failure`` 累计失败次数。
    3. 自愈/恢复由 SelfHealer 调用 ``report_success`` 关闭熔断器。
    """

    def decorator(func):
        @retry(
            wait=wait_random_exponential(multiplier=base_wait, max=max_wait),
            stop=stop_after_attempt(max_attempts),
            retry=retry_if_exception_type(Exception),
            reraise=True,
        )
        @wraps(func)
        async def _retried_async(*args, **kwargs):
            await llm_rate_limiter.acquire()
            return await func(*args, **kwargs)

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            from src.utils.circuit_breaker import circuit_breaker

            if not circuit_breaker.can_proceed():
                raise CircuitOpenError(
                    "LLM circuit breaker is OPEN; refusing to call until recovery."
                )

            node = func.__module__.rsplit(".", 1)[-1]

            try:
                from src.observability.metrics import (
                    llm_call_duration_seconds,
                    llm_calls_total,
                )
            except Exception:
                llm_call_duration_seconds = None
                llm_calls_total = None

            import time as _time
            t0 = _time.monotonic()
            try:
                result = await _retried_async(*args, **kwargs)
                if llm_calls_total is not None:
                    llm_calls_total.labels(node=node, outcome="success").inc()
                return result
            except RateLimitError:
                if llm_calls_total is not None:
                    llm_calls_total.labels(node=node, outcome="rate_limited").inc()
                circuit_breaker.report_failure(RuntimeError("rate_limited"))
                raise
            except Exception as exc:
                if llm_calls_total is not None:
                    llm_calls_total.labels(node=node, outcome="error").inc()
                circuit_breaker.report_failure(exc)
                raise
            finally:
                if llm_call_duration_seconds is not None:
                    llm_call_duration_seconds.labels(node=node).observe(
                        _time.monotonic() - t0
                    )

        @retry(
            wait=wait_random_exponential(multiplier=base_wait, max=max_wait),
            stop=stop_after_attempt(max_attempts),
            retry=retry_if_exception_type(Exception),
            reraise=True,
        )
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        import asyncio

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper

    return decorator


def with_simple_retry(
    max_attempts: int = 3,
    max_wait: int = 30,
):
    """
    简化版重试装饰器，不含速率限制。
    """

    def decorator(func):
        @retry(
            wait=wait_random_exponential(multiplier=1, max=max_wait),
            stop=stop_after_attempt(max_attempts),
            reraise=True,
        )
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            return await func(*args, **kwargs)

        @retry(
            wait=wait_random_exponential(multiplier=1, max=max_wait),
            stop=stop_after_attempt(max_attempts),
            reraise=True,
        )
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        import asyncio

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper

    return decorator
