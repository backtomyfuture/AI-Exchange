
from pydantic_settings import BaseSettings
from functools import lru_cache
from typing import Optional

class Settings(BaseSettings):
    # Database
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "email_agent"
    POSTGRES_USER: str = "user"
    POSTGRES_PASSWORD: str = "password"

    # Exchange
    EXCHANGE_API_URL: str = ""
    EXCHANGE_API_KEY: str = ""
    EXCHANGE_ACCOUNT_ID: int = 8
    EXCHANGE_ACCOUNT_EMAIL: str = "" # Identify "Me"
    EXCHANGE_SSL_VERIFY: bool = False
    EXCHANGE_WEBHOOK_SECRET: str = ""

    # Lark
    LARK_APP_ID: str = ""
    LARK_APP_SECRET: str = ""
    LARK_VERIFICATION_TOKEN: str = ""
    LARK_ENCRYPT_KEY: str = ""
    LARK_CHAT_ID: str = ""
    LARK_DRIVE_FOLDER_TOKEN: str = ""

    # Server
    EXTERNAL_URL: str = "http://localhost:8000"

    # Slack
    SLACK_BOT_TOKEN: str = ""
    SLACK_APP_TOKEN: str = ""
    SLACK_CHANNEL_ID: str = ""

    # LLM
    OPENAI_API_KEY: str = ""
    OPENAI_API_BASE: str = ""  # 与 .env 保持一致
    LLM_MODEL: str = "gemini-3-flash"
    LLM_RATE_LIMIT_DELAY: int = 10
    LLM_MAX_RPM: float = 15.0

    # Embedding
    QDRANT_URL: str = "http://localhost:6333"
    EMBEDDING_API_KEY: str = ""
    EMBEDDING_BASE_URL: str = ""
    EMBEDDING_MODEL: str = "Qwen/Qwen3-Embedding-4B"

    # App
    LOG_LEVEL: str = "INFO"

    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'
        extra = "ignore"

@lru_cache()
def get_settings():
    return Settings()
