import inspect


def test_rate_limiter_no_direct_os_getenv():
    """rate_limiter module should not use os.getenv() directly."""
    import src.utils.rate_limiter as mod

    source = inspect.getsource(mod)
    assert "os.getenv" not in source, (
        "rate_limiter.py should use get_settings() instead of os.getenv()"
    )
