from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache


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

    # Database
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "email_agent"
    POSTGRES_USER: str = "user"
    POSTGRES_PASSWORD: SecretStr = SecretStr("password")

    # Exchange
    EXCHANGE_API_URL: str = ""
    EXCHANGE_API_KEY: SecretStr = SecretStr("")
    EXCHANGE_ACCOUNT_ID: int = 8
    EXCHANGE_ACCOUNT_EMAIL: str = ""
    EXCHANGE_SSL_VERIFY: bool = False
    EXCHANGE_WEBHOOK_SECRET: SecretStr = SecretStr("")
    EXCHANGE_FOLDERS_FULL: str = "收件箱"
    EXCHANGE_FOLDERS_ARCHIVE: str = ""
    EXCHANGE_FOLDER_SENTITEMS: str = "已发送邮件"
    EXCHANGE_FOLDER_DRAFTS: str = "草稿"

    # Lark
    LARK_APP_ID: str = ""
    LARK_APP_SECRET: SecretStr = SecretStr("")
    LARK_VERIFICATION_TOKEN: str = ""
    LARK_ENCRYPT_KEY: SecretStr = SecretStr("")
    LARK_CHAT_ID: str = ""
    LARK_DRIVE_FOLDER_TOKEN: str = ""

    # Server
    EXTERNAL_URL: str = "http://localhost:8000"

    # Slack
    SLACK_BOT_TOKEN: str = ""
    SLACK_APP_TOKEN: str = ""
    SLACK_CHANNEL_ID: str = ""

    # LLM (global defaults)
    OPENAI_API_KEY: SecretStr = SecretStr("")
    OPENAI_API_BASE: str = ""
    LLM_MODEL: str = "gemini-3-flash"
    LLM_RATE_LIMIT_DELAY: int = 10
    LLM_MAX_RPM: float = 15.0

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

    # Polling (Hybrid Mode)
    POLLING_INTERVAL: int = 3600

    # Embedding
    QDRANT_URL: str = "http://localhost:6333"
    EMBEDDING_API_KEY: SecretStr = SecretStr("")
    EMBEDDING_BASE_URL: str = ""
    EMBEDDING_MODEL: str = "Qwen/Qwen3-Embedding-4B"

    # App
    DEBUG: bool = False
    LOG_LEVEL: str = "INFO"

    @property
    def database_url(self) -> str:
        """Compute PostgreSQL DSN from individual fields."""
        return (
            f"postgresql://{self.POSTGRES_USER}:{resolve_secret(self.POSTGRES_PASSWORD)}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )


@lru_cache()
def get_settings() -> Settings:
    return Settings()
