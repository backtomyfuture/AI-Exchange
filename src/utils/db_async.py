"""
Async Database Manager - 使用 psycopg (v3) 异步版本
提供与同步 DatabaseManager 兼容的接口，但使用异步操作
"""
import os
import json
import logging
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager

import psycopg
from psycopg.rows import dict_row

logger = logging.getLogger(__name__)


class AsyncDatabaseManager:
    """
    Manages PostgreSQL async connection and operations for email tracking.
    """

    def __init__(self):
        self.host = os.getenv("POSTGRES_HOST", "localhost")
        self.port = os.getenv("POSTGRES_PORT", "5432")
        self.database = os.getenv("POSTGRES_DB", "email_agent")
        self.user = os.getenv("POSTGRES_USER", "user")
        self.password = os.getenv("POSTGRES_PASSWORD", "password")
        self._conn: Optional[psycopg.AsyncConnection] = None
        self._initialized = False

    @property
    def dsn(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    async def get_connection(self) -> psycopg.AsyncConnection:
        if self._conn is None or self._conn.closed:
            try:
                self._conn = await psycopg.AsyncConnection.connect(
                    self.dsn,
                    autocommit=True,
                    row_factory=dict_row
                )
                if not self._initialized:
                    await self._init_db()
                    self._initialized = True
            except psycopg.OperationalError as e:
                logger.error(f"Failed to connect to PostgreSQL: {e}")
                raise
        return self._conn

    async def _init_db(self):
        """Initialize the audit log table and key-value store."""
        try:
            conn = await self.get_connection()
            async with conn.cursor() as cur:
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS emails_log (
                        id TEXT PRIMARY KEY,
                        subject TEXT,
                        sender TEXT,
                        received_at TIMESTAMP,
                        status TEXT DEFAULT 'pending',
                        classification JSONB,
                        draft_content TEXT,
                        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)

                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS app_kv_store (
                        key TEXT PRIMARY KEY,
                        value TEXT,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)

                await cur.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name='emails_log';"
                )
                rows = await cur.fetchall()
                columns = [row['column_name'] for row in rows]
                if 'classification' not in columns:
                    await cur.execute("ALTER TABLE emails_log ADD COLUMN classification JSONB;")

                await cur.execute("""
                    DO $$ 
                    BEGIN 
                        IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'processed_emails') THEN
                            DROP TABLE processed_emails;
                        END IF;
                    END $$;
                """)

                await cur.execute("""
                    CREATE OR REPLACE VIEW processed_emails AS 
                    SELECT id, processed_at FROM emails_log;
                """)
        except psycopg.Error as e:
            logger.error(f"DB Initialization failed: {e}")

    async def log_initial_email(self, email_data: Dict[str, Any]) -> bool:
        """
        Record a new email in the database.
        Returns True if this is a new record, False if it already exists.
        """
        try:
            conn = await self.get_connection()
            async with conn.cursor() as cur:
                await cur.execute("""
                    INSERT INTO emails_log (id, subject, sender, received_at, status)
                    VALUES (%s, %s, %s, %s, 'pending')
                    ON CONFLICT (id) DO NOTHING
                """, (
                    email_data.get("id"),
                    email_data.get("subject"),
                    str(email_data.get("sender")),
                    email_data.get("received_at")
                ))
                return cur.rowcount > 0
        except psycopg.Error as e:
            logger.error(f"Failed to log initial email {email_data.get('id')}: {e}")
            return False

    async def update_status(self, email_id: str, status: str, **kwargs):
        """Update the status and optional fields of an email log."""
        try:
            conn = await self.get_connection()
            update_fields = ["status = %s", "updated_at = CURRENT_TIMESTAMP"]
            params = [status]

            for key, value in kwargs.items():
                if key == "classification":
                    update_fields.append(f"{key} = %s")
                    params.append(json.dumps(value))
                else:
                    update_fields.append(f"{key} = %s")
                    params.append(value)

            params.append(email_id)
            query = f"UPDATE emails_log SET {', '.join(update_fields)} WHERE id = %s"

            async with conn.cursor() as cur:
                await cur.execute(query, tuple(params))
        except psycopg.Error as e:
            logger.error(f"Failed to update status for {email_id}: {e}")

    async def check_email_exists(self, email_id: str) -> bool:
        """Check if an email ID has already been logged/processed."""
        try:
            conn = await self.get_connection()
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1 FROM emails_log WHERE id = %s", (email_id,))
                return await cur.fetchone() is not None
        except psycopg.Error as e:
            logger.error(f"Failed to check email existence for {email_id}: {e}")
            return False

    async def get_processed_count(self) -> int:
        """Get total count of processed emails for reporting."""
        try:
            conn = await self.get_connection()
            async with conn.cursor() as cur:
                await cur.execute("SELECT count(*) as cnt FROM emails_log")
                row = await cur.fetchone()
                return row['cnt'] if row else 0
        except psycopg.Error:
            return 0

    async def mark_as_processed(self, email_id: str):
        """Quick shortcut for dedup, sets status to 'ingested'."""
        await self.update_status(email_id, "ingested")

    async def get_sync_state(self, account_id: str, folder: str = "INBOX") -> Optional[str]:
        """Retrieve the last sync state for a given account and folder."""
        try:
            conn = await self.get_connection()
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT value FROM app_kv_store WHERE key = %s",
                    (f"sync_state_{account_id}_{folder}",)
                )
                result = await cur.fetchone()
                return result['value'] if result else None
        except psycopg.Error as e:
            logger.error(f"Failed to get sync state for {folder}: {e}")
            return None

    async def save_sync_state(self, account_id: str, state: str, folder: str = "INBOX"):
        """Save the sync state for a given account and folder."""
        try:
            conn = await self.get_connection()
            async with conn.cursor() as cur:
                await cur.execute("""
                    INSERT INTO app_kv_store (key, value, updated_at)
                    VALUES (%s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (key) DO UPDATE SET 
                        value = EXCLUDED.value,
                        updated_at = CURRENT_TIMESTAMP;
                """, (f"sync_state_{account_id}_{folder}", state))
        except psycopg.Error as e:
            logger.error(f"Failed to save sync state for {folder}: {e}")

    async def close(self):
        if self._conn and not self._conn.closed:
            await self._conn.close()
