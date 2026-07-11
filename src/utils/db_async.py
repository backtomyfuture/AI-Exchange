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
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from src.domain.email_state import InitialEmailWriteResult
from src.domain.errors import DatabaseOperationError
from src.graph.state_factory import content_ref_from_json, content_ref_to_json
from src.storage import ContentRef

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

    @property
    def dsn(self) -> str:
        return self._dsn

    async def open(self):
        """Open the connection pool without mutating database schema."""
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
        except psycopg.OperationalError as exc:
            logger.error(
                "Failed to open PostgreSQL connection pool: error_type=%s",
                type(exc).__name__,
            )
            raise

    @asynccontextmanager
    async def get_connection(self):
        if self._pool is None:
            await self.open()
        async with self._pool.connection() as conn:
            yield conn

    async def log_initial_email(
        self, email_data: Dict[str, Any]
    ) -> InitialEmailWriteResult:
        """
        Record a new email in the database.
        Return a typed result that distinguishes creation from duplication.
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
                    if cur.rowcount > 0:
                        return InitialEmailWriteResult.CREATED
                    return InitialEmailWriteResult.DUPLICATE
        except psycopg.Error as exc:
            logger.error(
                "Failed to log initial email: error_type=%s",
                type(exc).__name__,
            )
            raise DatabaseOperationError(
                operation="log_initial_email",
                retryable=isinstance(exc, psycopg.OperationalError),
                message="initial email persistence failed",
            ) from None

    async def get_email_status(self, email_id: str) -> str | None:
        """Return the persisted processing status for an email, if present."""
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT status FROM emails_log WHERE id = %s", (email_id,)
                    )
                    row = await cur.fetchone()
                    return row["status"] if row else None
        except psycopg.Error as exc:
            logger.error(
                "Failed to get email status: error_type=%s",
                type(exc).__name__,
            )
            raise DatabaseOperationError(
                operation="get_email_status",
                retryable=isinstance(exc, psycopg.OperationalError),
                message="email status read failed",
            ) from None

    async def set_content_ref(self, email_id: str, ref: ContentRef) -> None:
        payload = content_ref_to_json(ref)
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE emails_log
                        SET content_ref = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                        """,
                        (Jsonb(payload), email_id),
                    )
                    if cur.rowcount != 1:
                        raise DatabaseOperationError(
                            operation="set_content_ref",
                            retryable=False,
                            message="email row missing",
                        )
        except DatabaseOperationError:
            raise
        except psycopg.Error as exc:
            logger.error(
                "Content reference persistence failed: error_type=%s",
                type(exc).__name__,
            )
            raise DatabaseOperationError(
                operation="set_content_ref",
                retryable=isinstance(exc, psycopg.OperationalError),
                message="content reference persistence failed",
            ) from None

    async def set_content_ref_if_absent(
        self,
        email_id: str,
        ref: ContentRef,
    ) -> bool:
        """Atomically claim an empty content_ref slot for concurrent retries."""
        payload = content_ref_to_json(ref)
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE emails_log
                        SET content_ref = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s AND content_ref IS NULL
                        """,
                        (Jsonb(payload), email_id),
                    )
                    return cur.rowcount == 1
        except psycopg.Error as exc:
            logger.error(
                "Content reference claim failed: error_type=%s",
                type(exc).__name__,
            )
            raise DatabaseOperationError(
                operation="set_content_ref_if_absent",
                retryable=isinstance(exc, psycopg.OperationalError),
                message="content reference claim failed",
            ) from None

    async def get_content_ref(self, email_id: str) -> ContentRef | None:
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT content_ref FROM emails_log WHERE id = %s",
                        (email_id,),
                    )
                    row = await cur.fetchone()
        except psycopg.Error as exc:
            logger.error(
                "Content reference read failed: error_type=%s",
                type(exc).__name__,
            )
            raise DatabaseOperationError(
                operation="get_content_ref",
                retryable=isinstance(exc, psycopg.OperationalError),
                message="content reference read failed",
            ) from None

        if row is None or row.get("content_ref") is None:
            return None
        raw_ref = row["content_ref"]
        if isinstance(raw_ref, str):
            try:
                raw_ref = json.loads(raw_ref)
            except json.JSONDecodeError:
                from src.storage import ContentStoreReferenceError

                raise ContentStoreReferenceError("invalid_content_ref") from None
        return content_ref_from_json(raw_ref)

    async def save_draft(self, email_id: str, content: str) -> str:
        if not isinstance(email_id, str) or not email_id or not isinstance(content, str):
            raise DatabaseOperationError(
                operation="save_draft",
                retryable=False,
                message="invalid draft input",
            )
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE emails_log
                        SET draft_content = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                        """,
                        (content, email_id),
                    )
                    if cur.rowcount != 1:
                        raise DatabaseOperationError(
                            operation="save_draft",
                            retryable=False,
                            message="email row missing",
                        )
        except DatabaseOperationError:
            raise
        except psycopg.Error as exc:
            logger.error(
                "Draft persistence failed: error_type=%s",
                type(exc).__name__,
            )
            raise DatabaseOperationError(
                operation="save_draft",
                retryable=isinstance(exc, psycopg.OperationalError),
                message="draft persistence failed",
            ) from None
        return email_id

    async def save_draft_if_status(self, email_id: str, content: str) -> bool:
        """Update a draft only while the email still awaits approval."""
        if (
            not isinstance(email_id, str)
            or not email_id.strip()
            or not isinstance(content, str)
            or not content.strip()
        ):
            raise ValueError("invalid_draft_edit")
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE emails_log
                        SET draft_content = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s AND status = %s
                        """,
                        (content, email_id, "waiting_approval"),
                    )
                    return cur.rowcount == 1
        except psycopg.Error as exc:
            logger.error(
                "Conditional draft persistence failed: error_type=%s",
                type(exc).__name__,
            )
            raise DatabaseOperationError(
                operation="save_draft_if_status",
                retryable=isinstance(exc, psycopg.OperationalError),
                message="conditional draft persistence failed",
            ) from None

    async def load_draft(self, draft_id: str) -> str:
        if not isinstance(draft_id, str) or not draft_id:
            raise DatabaseOperationError(
                operation="load_draft",
                retryable=False,
                message="invalid draft identifier",
            )
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT draft_content FROM emails_log WHERE id = %s",
                        (draft_id,),
                    )
                    row = await cur.fetchone()
        except psycopg.Error as exc:
            logger.error(
                "Draft read failed: error_type=%s",
                type(exc).__name__,
            )
            raise DatabaseOperationError(
                operation="load_draft",
                retryable=isinstance(exc, psycopg.OperationalError),
                message="draft read failed",
            ) from None

        if row is None or not isinstance(row.get("draft_content"), str):
            raise DatabaseOperationError(
                operation="load_draft",
                retryable=False,
                message="draft not found",
            )
        return row["draft_content"]

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
                    if cur.rowcount != 1:
                        raise DatabaseOperationError(
                            operation="update_status",
                            retryable=False,
                            message=f"Email {email_id} was not updated",
                        )

            if status is not None:
                try:
                    from src.observability.metrics import record_email_status
                    record_email_status(status)
                except Exception:
                    pass
        except psycopg.Error as exc:
            logger.error(
                "Failed to update email status: error_type=%s",
                type(exc).__name__,
            )
            raise DatabaseOperationError(
                operation="update_status",
                retryable=isinstance(exc, psycopg.OperationalError),
                message="email status update failed",
            ) from None

    async def compare_and_set_status(
        self,
        email_id: str,
        *,
        expected: frozenset[str],
        target: str,
    ) -> bool:
        """Atomically transition an email when its current status is expected."""
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "UPDATE emails_log SET status=%s, updated_at=CURRENT_TIMESTAMP "
                        "WHERE id=%s AND status=ANY(%s)",
                        (target, email_id, list(expected)),
                    )
                    return cur.rowcount == 1
        except psycopg.Error as exc:
            logger.error(
                "Failed to compare and set email status: error_type=%s",
                type(exc).__name__,
            )
            raise DatabaseOperationError(
                operation="compare_and_set_status",
                retryable=isinstance(exc, psycopg.OperationalError),
                message="email status compare-and-set failed",
            ) from None

    async def claim_self_healing(
        self,
        email_id: str,
        *,
        immediate: frozenset[str],
        stale: frozenset[str],
        stale_after_seconds: int,
    ) -> bool:
        """Atomically claim eligible recovery work without stealing live work."""
        if (
            not isinstance(email_id, str)
            or not email_id.strip()
            or not immediate
            or not stale
            or any(not isinstance(status, str) or not status for status in immediate | stale)
            or isinstance(stale_after_seconds, bool)
            or not isinstance(stale_after_seconds, int)
            or stale_after_seconds <= 0
        ):
            raise ValueError("invalid_self_healing_claim")
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE emails_log
                        SET status = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s AND (
                            status = ANY(%s)
                            OR (
                                status = ANY(%s)
                                AND updated_at < CURRENT_TIMESTAMP
                                    - (%s * INTERVAL '1 second')
                            )
                        )
                        """,
                        (
                            "recovering",
                            email_id,
                            sorted(immediate),
                            sorted(stale),
                            stale_after_seconds,
                        ),
                    )
                    return cur.rowcount == 1
        except psycopg.Error as exc:
            logger.error(
                "Self-healing claim failed: error_type=%s",
                type(exc).__name__,
            )
            raise DatabaseOperationError(
                operation="claim_self_healing",
                retryable=isinstance(exc, psycopg.OperationalError),
                message="self-healing claim failed",
            ) from None

    async def compare_and_set_manual_review(
        self,
        email_id: str,
        *,
        expected: frozenset[str],
        error_code: str,
    ) -> bool:
        """Atomically enter manual review and persist its bounded reason."""
        if (
            not isinstance(email_id, str)
            or not email_id.strip()
            or not isinstance(error_code, str)
            or not error_code
            or len(error_code.encode("utf-8")) > 256
        ):
            raise ValueError("invalid_manual_review_transition")
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE emails_log
                        SET status = %s,
                            error_message = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s AND status = ANY(%s)
                        """,
                        (
                            "manual_review",
                            error_code,
                            email_id,
                            list(expected),
                        ),
                    )
                    return cur.rowcount == 1
        except psycopg.Error as exc:
            logger.error(
                "Manual-review transition failed: error_type=%s",
                type(exc).__name__,
            )
            raise DatabaseOperationError(
                operation="compare_and_set_manual_review",
                retryable=isinstance(exc, psycopg.OperationalError),
                message="manual-review transition failed",
            ) from None

    async def recover_incomplete_approval_states(self) -> int:
        """Fail closed for approval/send transitions left ambiguous at restart."""
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE emails_log
                        SET status = %s,
                            error_message = CASE status
                                WHEN %s THEN %s
                                WHEN %s THEN %s
                                WHEN %s THEN %s
                                WHEN %s THEN %s
                                ELSE error_message
                            END,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE status = ANY(%s)
                        """,
                        (
                            "manual_review",
                            "approved",
                            "approval_handoff_incomplete",
                            "sending",
                            "send_outcome_unknown",
                            "saving_draft",
                            "draft_save_outcome_unknown",
                            "recovering",
                            "self_healing_interrupted",
                            ["approved", "sending", "saving_draft", "recovering"],
                        ),
                    )
                    return cur.rowcount
        except psycopg.Error as exc:
            logger.error(
                "Approval state recovery failed: error_type=%s",
                type(exc).__name__,
            )
            raise DatabaseOperationError(
                operation="recover_incomplete_approval_states",
                retryable=isinstance(exc, psycopg.OperationalError),
                message="approval state recovery failed",
            ) from None

    async def check_email_exists(self, email_id: str) -> bool:
        """Check if an email ID has already been logged/processed."""
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 1 FROM emails_log WHERE id = %s", (email_id,))
                    return await cur.fetchone() is not None
        except psycopg.Error as exc:
            logger.error(
                "Failed to check email existence: error_type=%s",
                type(exc).__name__,
            )
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
        except Exception as exc:
            logger.error(
                "Failed to get records by date: error_type=%s",
                type(exc).__name__,
            )
            return []

    async def close(self):
        if self._pool:
            await self._pool.close()
            logger.info("AsyncDatabaseManager connection pool closed.")
            self._pool = None
