import logging
from contextlib import contextmanager
from typing import Iterator

import structlog


def setup_logging(log_level: str = "INFO"):
    """Configure structured logging for the entire application."""
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.dev.ConsoleRenderer()
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(getattr(logging, log_level.upper(), logging.INFO))


@contextmanager
def log_email_context(email_id: str | None) -> Iterator[None]:
    """
    Bind ``email_id`` into the structlog contextvars for the duration of the
    ``with`` block. Every structlog log record emitted from inside the block
    (including from background tasks scheduled with ``await``) carries the
    ``email_id`` field automatically. Idempotent: nested context calls layer
    correctly because we use ``bound_contextvars`` from structlog.
    """
    if not email_id:
        yield
        return
    with structlog.contextvars.bound_contextvars(email_id=email_id):
        yield
