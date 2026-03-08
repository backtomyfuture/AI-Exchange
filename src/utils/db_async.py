"""
Async Database Manager - 使用 psycopg (v3) 异步版本
提供与同步 DatabaseManager 兼容的接口，但使用异步操作
"""
import json
import logging
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

logger = logging.getLogger(__name__)


def normalize_timestamp_input(value: Any) -> Any:
    """Normalize timestamp input before DB write."""
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return value


class AsyncDatabaseManager:
    """
    Manages PostgreSQL async connection and operations for email tracking.
    """

    def __init__(self, settings):
        self._dsn = settings.database_url
        self._pool: Optional[AsyncConnectionPool] = None
        self._initialized = False

    @property
    def dsn(self) -> str:
        return self._dsn

    async def open(self):
        """Open the connection pool and initialize database schema."""
        try:
            if self._pool is None:
                self._pool = AsyncConnectionPool(
                    conninfo=self._dsn,
                    min_size=2,
                    max_size=10,
                    open=False,
                    kwargs={"autocommit": True, "row_factory": dict_row},
                )
                await self._pool.open()
                logger.info("AsyncDatabaseManager connection pool opened (min=2, max=10).")

            if not self._initialized:
                await self._init_db()
                self._initialized = True
        except psycopg.OperationalError as e:
            logger.error(f"Failed to open PostgreSQL connection pool: {e}")
            raise

    @asynccontextmanager
    async def get_connection(self):
        if self._pool is None:
            await self.open()
        async with self._pool.connection() as conn:
            yield conn

    async def _init_db(self):
        """Initialize the audit log table and key-value store."""
        try:
            async with self.get_connection() as conn:
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
                    columns = [row["column_name"] for row in rows]
                    if "classification" not in columns:
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
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("""
                    INSERT INTO emails_log (id, subject, sender, received_at, status)
                    VALUES (%s, %s, %s, %s, 'pending')
                    ON CONFLICT (id) DO NOTHING
                """, (
                    email_data.get("id"),
                    email_data.get("subject"),
                    str(email_data.get("sender")),
                    normalize_timestamp_input(email_data.get("received_at"))
                ))
                    return cur.rowcount > 0
        except psycopg.Error as e:
            logger.error(f"Failed to log initial email {email_data.get('id')}: {e}")
            return False

    async def update_status(self, email_id: str, status: Optional[str], **kwargs):
        """Update the status and optional fields of an email log.

        Args:
            email_id: The email ID.
            status: New status string, or None to skip status change (metadata-only update).
            **kwargs: Additional columns to update.
        """
        ALLOWED_COLUMNS = {
            "classification", "summary", "priority", "need_reply",
            "card_type", "draft", "draft_content", "message_id", "intent",
            "reasoning", "error_message",
            "routing_log", "active_skills",
            "original_draft", "final_draft", "draft_diff",
            "approver_user_id", "rejection_reason",
        }
        JSONB_COLUMNS = {"classification", "routing_log", "active_skills"}
        try:
            async with self.get_connection() as conn:
                update_fields = ["updated_at = CURRENT_TIMESTAMP"]
                params: list = []

                if status is not None:
                    update_fields.insert(0, "status = %s")
                    params.append(status)

                for key, value in kwargs.items():
                    if key not in ALLOWED_COLUMNS:
                        logger.warning(f"Rejected update_status column: {key}")
                        continue
                    if key in JSONB_COLUMNS:
                        update_fields.append(f"{key} = %s")
                        params.append(json.dumps(value) if not isinstance(value, str) else value)
                    else:
                        update_fields.append(f"{key} = %s")
                        params.append(value)

                if not update_fields:
                    return

                params.append(email_id)
                query = f"UPDATE emails_log SET {', '.join(update_fields)} WHERE id = %s"

                async with conn.cursor() as cur:
                    await cur.execute(query, tuple(params))
        except psycopg.Error as e:
            logger.error(f"Failed to update status for {email_id}: {e}")

    async def check_email_exists(self, email_id: str) -> bool:
        """Check if an email ID has already been logged/processed."""
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 1 FROM emails_log WHERE id = %s", (email_id,))
                    return await cur.fetchone() is not None
        except psycopg.Error as e:
            logger.error(f"Failed to check email existence for {email_id}: {e}")
            return False

    async def get_processed_count(self) -> int:
        """Get total count of processed emails for reporting."""
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT count(*) as cnt FROM emails_log")
                    row = await cur.fetchone()
                    return row["cnt"] if row else 0
        except psycopg.Error:
            return 0

    async def mark_as_processed(self, email_id: str):
        """Quick shortcut for dedup, sets status to 'ingested'."""
        await self.update_status(email_id, "ingested")

    async def get_records_by_date(self, target_date) -> list:
        """Query email records processed on a specific date."""
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT * FROM emails_log WHERE DATE(processed_at) = %s ORDER BY processed_at DESC",
                        (target_date,)
                    )
                    return await cur.fetchall()
        except Exception as e:
            logger.error(f"Failed to get records by date: {e}")
            return []

    async def close(self):
        if self._pool:
            await self._pool.close()
            logger.info("AsyncDatabaseManager connection pool closed.")
            self._pool = None
