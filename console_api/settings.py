"""Settings for the local Operations Console.

The console deliberately has its own settings object.  It must never import
the production application settings or inherit a runtime, migration, or
maintenance DSN.
"""

from __future__ import annotations

import os
import ipaddress
from functools import lru_cache
from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class ConsoleSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="CONSOLE_",
        env_file=".env.console",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: SecretStr = Field(default=SecretStr(""))
    database_url_file: str = ""
    database_role: str = "ai_exchange_operations_console"
    account_id: int = Field(default=8, ge=1)
    schema_name: str = "public"
    rules_dir: Path = Path("tier1_rules")
    artifact_dir: Path = Path("artifacts/tier1")
    internal_email_domains: str = ""
    me_email: str = ""
    allowed_origins: str = "http://127.0.0.1:5173,http://localhost:5173"
    allowed_client_hosts: str = "127.0.0.1,::1,localhost,testclient"
    allow_private_client_hosts: bool = False
    statement_timeout_ms: int = Field(default=5000, ge=250, le=30000)
    max_page_size: int = Field(default=50, ge=1, le=100)

    def resolved_database_url(self) -> str:
        direct = self.database_url.get_secret_value().strip()
        if direct:
            return direct
        path = self.database_url_file.strip()
        if not path:
            return ""
        try:
            mode = Path(path).stat().st_mode & 0o777
            if mode not in {0o400, 0o600}:
                return ""
            value = Path(path).read_text(encoding="utf-8").strip()
        except (OSError, UnicodeError):
            return ""
        return value

    def origin_list(self) -> list[str]:
        return [origin.strip() for origin in self.allowed_origins.split(",") if origin.strip()]

    def client_host_list(self) -> list[str]:
        return [host.strip() for host in self.allowed_client_hosts.split(",") if host.strip()]

    def client_host_allowed(self, host: str | None) -> bool:
        if host in self.client_host_list():
            return True
        if not self.allow_private_client_hosts or not host:
            return False
        try:
            return ipaddress.ip_address(host).is_private
        except ValueError:
            return False


@lru_cache
def get_console_settings() -> ConsoleSettings:
    """Return process-local settings without creating a database connection."""

    return ConsoleSettings()


def reset_console_settings_for_tests() -> None:
    """Clear the settings cache for isolated test processes."""

    get_console_settings.cache_clear()


def running_in_production() -> bool:
    return os.getenv("APP_ENV", "").casefold() == "production"
