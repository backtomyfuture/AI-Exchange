import logging
from contextlib import contextmanager
from typing import Iterator

import structlog

from src.security.redaction import fingerprint_identifier


_THIRD_PARTY_MIN_LEVELS = {
    # HTTP client request logs can include full URLs or credentials.
    "httpx": logging.WARNING,
    "httpcore": logging.WARNING,
    "urllib3": logging.WARNING,
    "websockets": logging.WARNING,
    # WeasyPrint temporarily lowers the fontTools parent logger to DEBUG while
    # subsetting fonts.  A handler filter is therefore required in addition to
    # setting logger levels, otherwise every glyph and timing step reaches the
    # Docker console.
    "fontTools": logging.ERROR,
    "weasyprint": logging.ERROR,
    "pypdf": logging.ERROR,
    "pdfminer": logging.ERROR,
    # The Lark SDK can log request payloads even at high verbosity levels.
    "Lark": logging.CRITICAL + 1,
}


class _ThirdPartyNoiseFilter(logging.Filter):
    """Enforce minimum levels even when a dependency changes its logger level."""

    def filter(self, record: logging.LogRecord) -> bool:
        for namespace, minimum_level in _THIRD_PARTY_MIN_LEVELS.items():
            if record.name == namespace or record.name.startswith(f"{namespace}."):
                return record.levelno >= minimum_level
        return True


def harden_third_party_loggers() -> None:
    """Suppress third-party request/SDK logs that can contain URLs or tokens."""

    for logger_name, minimum_level in _THIRD_PARTY_MIN_LEVELS.items():
        logging.getLogger(logger_name).setLevel(minimum_level)


def setup_logging(log_level: str = "INFO") -> None:
    """Configure concise, structured console logging for the application."""
    timestamper = structlog.processors.TimeStamper(
        fmt="%Y-%m-%d %H:%M:%S",
        utc=False,
    )
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        timestamper,
    ]

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            timestamper,
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
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.dev.ConsoleRenderer(
                colors=False,
                pad_event_to=0,
                sort_keys=False,
            ),
        ],
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    handler.addFilter(_ThirdPartyNoiseFilter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    harden_third_party_loggers()


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
    with structlog.contextvars.bound_contextvars(
        email_id=fingerprint_identifier(email_id, namespace="email")
    ):
        yield
