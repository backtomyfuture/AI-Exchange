"""
Verify that with_llm_retry integrates with the global circuit_breaker:
- Reports failure after retries are exhausted.
- Fast-fails with CircuitOpenError when the breaker is already open.
"""

import asyncio
import pytest

from src.utils import circuit_breaker as cb_module
from src.utils.retry_decorator import with_llm_retry, CircuitOpenError


@pytest.fixture(autouse=True)
def fresh_breaker(monkeypatch):
    breaker = cb_module.CircuitBreaker(
        failure_threshold=2, window_seconds=60, recovery_timeout=300
    )
    monkeypatch.setattr(cb_module, "circuit_breaker", breaker)
    yield breaker


@pytest.mark.asyncio
async def test_breaker_opens_after_repeated_terminal_failures(fresh_breaker):
    @with_llm_retry(max_attempts=2, base_wait=0, max_wait=0)
    async def always_fail():
        raise RuntimeError("upstream LLM 500")

    # Two terminal failures -> threshold reached -> breaker opens.
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await always_fail()

    assert fresh_breaker.is_open is True
    assert fresh_breaker.failure_count >= 2


@pytest.mark.asyncio
async def test_breaker_open_short_circuits_subsequent_calls(fresh_breaker):
    fresh_breaker._is_open = True
    fresh_breaker.last_error = "induced"

    @with_llm_retry(max_attempts=2, base_wait=0, max_wait=0)
    async def call_llm():
        return "ok"

    with pytest.raises(CircuitOpenError):
        await call_llm()
