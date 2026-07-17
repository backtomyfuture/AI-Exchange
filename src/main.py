"""Executable entrypoint for the single FastAPI application runtime."""

import uvicorn

from src.config import get_settings
from src.security.auth import validate_runtime_security
from src.server import app
from src.utils.logging_setup import setup_logging


def run_server() -> None:
    """Validate process security, then bind Uvicorn around the reviewed app."""

    settings = get_settings()
    validate_runtime_security(settings)
    setup_logging(settings.LOG_LEVEL)
    uvicorn.run(app, host="0.0.0.0", port=8000, access_log=False)


if __name__ == "__main__":
    run_server()
