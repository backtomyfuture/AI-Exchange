import os
import logging
import psycopg2
from psycopg2 import OperationalError, IntegrityError, DatabaseError
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Manages PostgreSQL connection and operations for email tracking and audit logs.
    """
    def __init__(self):
        self.host = os.getenv("POSTGRES_HOST", "localhost")
        self.port = os.getenv("POSTGRES_PORT", "5432")
        self.database = os.getenv("POSTGRES_DB", "email_agent")
        self.user = os.getenv("POSTGRES_USER", "user")
        self.password = os.getenv("POSTGRES_PASSWORD", "password")
        self._conn = None

    def get_connection(self):
        if self._conn is None or self._conn.closed:
            try:
                self._conn = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user=self.user,
                    password=self.password
                )
                self._conn.autocommit = True
                self._init_db()
            except OperationalError as e:
                logger.error(f"Failed to connect to PostgreSQL (connection error): {e}")
                raise
            except DatabaseError as e:
                logger.error(f"Database error during connection: {e}")
                raise
        return self._conn

    def _init_db(self):
        """
        Initialize the audit log table and key-value store.
        """
        try:
            with self._conn.cursor() as cur:
                cur.execute("""
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
                
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS app_kv_store (
                        key TEXT PRIMARY KEY,
                        value TEXT,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                
                cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='emails_log';")
                columns = [row[0] for row in cur.fetchall()]
                if 'classification' not in columns:
                    cur.execute("ALTER TABLE emails_log ADD COLUMN classification JSONB;")

                cur.execute("""
                    DO $$ 
                    BEGIN 
                        IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = 'processed_emails') THEN
                            DROP TABLE processed_emails;
                        END IF;
                    END $$;
                """)

                cur.execute("""
                    CREATE OR REPLACE VIEW processed_emails AS 
                    SELECT id, processed_at FROM emails_log;
                """)
        except DatabaseError as e:
            logger.error(f"DB Initialization failed: {e}")

    def log_initial_email(self, email_data: Dict[str, Any]) -> bool:
        """
        Record a new email in the database. 
        Returns True if this is a new record, False if it already exists.
        """
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
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
        except IntegrityError as e:
            logger.warning(f"Duplicate email entry {email_data.get('id')}: {e}")
            return False
        except DatabaseError as e:
            logger.error(f"Failed to log initial email {email_data.get('id')}: {e}")
            return False

    def update_status(self, email_id: str, status: str, **kwargs):
        """
        Update the status and optional fields of an email log.
        """
        import json
        try:
            conn = self.get_connection()
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
            
            with conn.cursor() as cur:
                cur.execute(query, tuple(params))
        except DatabaseError as e:
            logger.error(f"Failed to update status for {email_id}: {e}")

    def check_email_exists(self, email_id: str) -> bool:
        """
        Check if an email ID has already been logged/processed.
        """
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM emails_log WHERE id = %s", (email_id,))
                return cur.fetchone() is not None
        except DatabaseError as e:
            logger.error(f"Failed to check email existence for {email_id}: {e}")
            return False

    def get_processed_count(self) -> int:
        """
        Get total count of processed emails for reporting.
        """
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM emails_log")
                return cur.fetchone()[0]
        except DatabaseError as e:
            logger.error(f"Failed to get processed count: {e}")
            return 0

    def mark_as_processed(self, email_id: str):
        """
        Quick shortcut for dedup, sets status to 'ingested'.
        """
        self.update_status(email_id, "ingested")

    def get_sync_state(self, account_id: str, folder: str = "INBOX") -> Optional[str]:
        """
        Retrieve the last sync state for a given account and folder.
        """
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM app_kv_store WHERE key = %s", (f"sync_state_{account_id}_{folder}",))
                result = cur.fetchone()
                return result[0] if result else None
        except DatabaseError as e:
            logger.error(f"Failed to get sync state for {folder}: {e}")
            return None

    def save_sync_state(self, account_id: str, state: str, folder: str = "INBOX"):
        """
        Save the sync state for a given account and folder.
        """
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO app_kv_store (key, value, updated_at)
                    VALUES (%s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (key) DO UPDATE SET 
                        value = EXCLUDED.value,
                        updated_at = CURRENT_TIMESTAMP;
                """, (f"sync_state_{account_id}_{folder}", state))
        except DatabaseError as e:
            logger.error(f"Failed to save sync state for {folder}: {e}")

    def close(self):
        if self._conn and not self._conn.closed:
            self._conn.close()
