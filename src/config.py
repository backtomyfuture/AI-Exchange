import os
from functools import lru_cache
from typing import Literal

from psycopg.conninfo import make_conninfo
from pydantic import Field, PositiveInt, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def resolve_secret(value) -> str:
    """Extract plain string from SecretStr or return string as-is (mock-safe)."""
    if hasattr(value, "get_secret_value"):
        return value.get_secret_value()
    return str(value) if value is not None else ""


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls,
        init_settings,
        env_settings,
        dotenv_settings,
        file_secret_settings,
    ):
        """Allow tests and sealed containers to opt out of local dotenv loading."""
        dotenv_disabled = os.getenv("PYTHON_DOTENV_DISABLED", "").lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        if dotenv_disabled:
            return init_settings, env_settings, file_secret_settings
        return init_settings, env_settings, dotenv_settings, file_secret_settings

    # Database
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "email_agent"
    POSTGRES_USER: str = "ai_exchange_runtime"
    POSTGRES_PASSWORD: SecretStr = SecretStr("")
    # Internal container wiring. Users configure neither path in `.env`.
    POSTGRES_PASSWORD_FILE: str = Field(default="", repr=False)
    POSTGRES_SCHEMA: str = "public"
    POSTGRES_MIGRATION_OWNER_ROLE: str = "ai_exchange_migration_owner"
    POSTGRES_MAINTENANCE_ROLE: str = "ai_exchange_checkpoint_maintenance"
    POSTGRES_CHECKPOINT_AUDITOR_ROLE: str = "ai_exchange_checkpoint_auditor"
    DATABASE_ROLE_SEPARATION_REQUIRED: bool = False

    # Phase4-Lite keeps safe defaults off. A fresh single-account deployment may
    # enable only Durable Inbox processing; Shadow and Sync remain out of scope.
    DURABLE_INBOX_ENABLED: bool = False
    INGESTION_SHADOW_ENABLED: bool = False
    SYNC_RECONCILIATION_ENABLED: bool = False
    INGESTION_INSTANCE_ID: str = "ai-exchange-web"
    INGESTION_LEASE_SECONDS: PositiveInt = 30
    INGESTION_HEARTBEAT_SECONDS: PositiveInt = 10
    INGESTION_SHUTDOWN_SECONDS: int = Field(default=30, ge=1, le=30)

    # Exchange
    EXCHANGE_API_URL: str = ""
    EXCHANGE_API_KEY: SecretStr = SecretStr("")
    EXCHANGE_ACCOUNT_ID: int = 8
    EXCHANGE_ACCOUNT_EMAIL: str = ""
    EXCHANGE_SSL_VERIFY: bool = True
    EXCHANGE_CA_FILE: str = ""
    EXCHANGE_WEBHOOK_SECRET: SecretStr = SecretStr("")
    EXCHANGE_FOLDERS_FULL: str = "收件箱"
    EXCHANGE_FOLDERS_ARCHIVE: str = ""
    EXCHANGE_FOLDER_SENTITEMS: str = "Sent Items"
    EXCHANGE_FOLDER_DRAFTS: str = "Drafts"
    WEBHOOK_MAX_BYTES: PositiveInt = 1_048_576
    EXCHANGE_RESPONSE_MAX_BYTES: PositiveInt = 67_108_864
    EMAIL_BODY_MAX_BYTES: PositiveInt = 10_485_760
    EMAIL_ATTACHMENT_MAX_COUNT: PositiveInt = 20
    EMAIL_ATTACHMENT_SINGLE_MAX_BYTES: PositiveInt = 26_214_400
    EMAIL_ATTACHMENT_TOTAL_MAX_BYTES: PositiveInt = 52_428_800
    # 领导/VIP 发件人名单（CSV，逗号分隔）。用于「非回复但值得阅读」的推送判定。
    LEADER_SENDERS: str = ""

    # Lark
    LARK_APP_ID: str = ""
    LARK_APP_SECRET: SecretStr = SecretStr("")
    LARK_ENCRYPT_KEY: SecretStr = SecretStr("")
    LARK_CHAT_ID: str = ""
    LARK_DRIVE_FOLDER_TOKEN: str = ""
    LARK_ALLOWED_OPEN_IDS: str = ""

    # Server
    EXTERNAL_URL: str = "http://localhost:8000"
    METRICS_TOKEN: SecretStr = SecretStr("")
    METRICS_TOKEN_FILE: str = Field(default="", repr=False)

    # Encrypted content storage. Empty key is an intentional fail-closed default.
    CONTENT_STORE_ROOT: str = "/app/data/content"
    CONTENT_STORE_KEY: SecretStr = SecretStr("")
    CONTENT_STORE_KEY_FILE: str = Field(default="", repr=False)
    CONTENT_STORE_KEY_VERSION: str = "v1"

    # LLM (global defaults)
    OPENAI_API_KEY: SecretStr = SecretStr("")
    OPENAI_API_BASE: str = ""
    LLM_MODEL: str = "gemini-3-flash"
    LLM_MAX_RPM: float = 15.0
    LLM_MAX_INPUT_TOKENS: PositiveInt = 122_880
    LLM_MAX_OUTPUT_TOKENS: PositiveInt = 8_192
    LLM_MAX_TOTAL_TOKENS: PositiveInt = 131_072

    # Per-role model overrides (leave empty to use LLM_MODEL)
    LLM_CATEGORIZER_MODEL: str = ""
    LLM_DRAFTER_MODEL: str = ""
    LLM_REVIEWER_MODEL: str = ""
    LLM_ROUTER_MODEL: str = ""
    LLM_SUMMARY_MODEL: str = ""
    LLM_CONSOLIDATOR_MODEL: str = ""

    # Additional provider API keys (auto-detected by model name)
    ANTHROPIC_API_KEY: SecretStr = SecretStr("")
    GOOGLE_API_KEY: SecretStr = SecretStr("")
    DEEPSEEK_API_KEY: SecretStr = SecretStr("")
    DASHSCOPE_API_KEY: SecretStr = SecretStr("")
    MOONSHOT_API_KEY: SecretStr = SecretStr("")
    ZHIPUAI_API_KEY: SecretStr = SecretStr("")
    XAI_API_KEY: SecretStr = SecretStr("")
    GROQ_API_KEY: SecretStr = SecretStr("")
    MISTRAL_API_KEY: SecretStr = SecretStr("")

    # Embedding
    QDRANT_URL: str = "http://localhost:6333"
    EMBEDDING_API_KEY: SecretStr = SecretStr("")
    EMBEDDING_BASE_URL: str = ""
    EMBEDDING_MODEL: str = "Qwen/Qwen3-Embedding-4B"

    # App
    APP_ENV: Literal["development", "test", "production"] = "development"
    DEBUG: bool = False
    LOG_LEVEL: str = "INFO"

    @model_validator(mode="after")
    def load_internal_file_secrets(self) -> "Settings":
        """Resolve generated runtime secrets without exposing them in user `.env`."""

        from src.db.migration_settings import _read_secret_file

        for value_field, path_field in (
            ("POSTGRES_PASSWORD", "POSTGRES_PASSWORD_FILE"),
            ("METRICS_TOKEN", "METRICS_TOKEN_FILE"),
            ("CONTENT_STORE_KEY", "CONTENT_STORE_KEY_FILE"),
        ):
            current = resolve_secret(getattr(self, value_field)).strip()
            path = str(getattr(self, path_field) or "").strip()
            if current or not path:
                continue
            object.__setattr__(
                self,
                value_field,
                SecretStr(_read_secret_file(path)),
            )
        return self

    @property
    def database_url(self) -> str:
        """Compute PostgreSQL DSN from individual fields."""
        return make_conninfo(
            host=self.POSTGRES_HOST,
            port=self.POSTGRES_PORT,
            dbname=self.POSTGRES_DB,
            user=self.POSTGRES_USER,
            password=resolve_secret(self.POSTGRES_PASSWORD),
            options=f"-csearch_path=pg_catalog,{self.POSTGRES_SCHEMA}",
        )


@lru_cache()
def get_settings() -> Settings:
    return Settings()
