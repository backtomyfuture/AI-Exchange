import time


def _make_fresh_cb():
    """Create a fresh CircuitBreaker instance."""
    from src.utils.circuit_breaker import CircuitBreaker
    return CircuitBreaker()


def test_circuit_breaker_stays_closed_on_single_failure():
    """Single failure should NOT open the circuit breaker."""
    cb = _make_fresh_cb()
    cb.report_failure(Exception("transient"))
    assert cb.can_proceed() is True


def test_circuit_breaker_opens_after_threshold():
    """Circuit should open after N failures within the window."""
    cb = _make_fresh_cb()
    cb.failure_threshold = 3
    cb.window_seconds = 60

    cb.report_failure(Exception("err1"))
    cb.report_failure(Exception("err2"))
    assert cb.can_proceed() is True

    cb.report_failure(Exception("err3"))
    assert cb.can_proceed() is False


def test_circuit_breaker_old_failures_expire():
    """Failures outside the time window should not count."""
    cb = _make_fresh_cb()
    cb.failure_threshold = 3
    cb.window_seconds = 10

    now = time.monotonic()
    cb._failure_timestamps = [now - 20, now - 15]  # expired
    cb.report_failure(Exception("recent"))
    assert cb.can_proceed() is True  # only 1 recent failure


def test_circuit_breaker_resets_on_success():
    """Success should close the circuit and clear failure history."""
    cb = _make_fresh_cb()
    cb.failure_threshold = 3
    cb.window_seconds = 60

    for i in range(3):
        cb.report_failure(Exception(f"err{i}"))
    assert cb.can_proceed() is False

    cb.report_success()
    assert cb.can_proceed() is True
    assert cb.failure_count == 0
