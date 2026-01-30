
from pydantic_settings import BaseSettings
from functools import lru_cache

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
    EXCHANGE_AI_FOLDERS: str = "INBOX"
    EXCHANGE_ARCHIVE_FOLDERS: str = ""
    EXCHANGE_SSL_VERIFY: bool = False

    # Lark
    LARK_APP_ID: str = ""
    LARK_APP_SECRET: str = ""
    LARK_VERIFICATION_TOKEN: str = ""
    LARK_ENCRYPT_KEY: str = ""

    # LLM
    OPENAI_API_KEY: str = ""
    OPENAI_BASE_URL: str = ""
    LLM_MODEL: str = "gemini-3-flash"

    # App
    LOG_LEVEL: str = "INFO"

    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'
        extra = "ignore"

@lru_cache()
def get_settings():
    return Settings()
