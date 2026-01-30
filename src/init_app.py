import os
import logging
from contextlib import contextmanager
from dotenv import load_dotenv
import psycopg
from psycopg_pool import AsyncConnectionPool
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from src.graph.builder import build_graph
from src.utils.exchange_api import ExchangeClient
from src.utils.email_processor import EmailProcessor
from src.utils.db import DatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

class AppContext:
    def __init__(self):
        self.exchange_client = None
        self.email_processor = None
        self.db_manager = None
        self.graph = None
        self.pool = None

    def initialize(self):
        """
        Initialize all shared components.
        """
        logger.info("Initializing Application Context...")
        
        # 1. Core Components
        self.exchange_client = ExchangeClient()
        self.email_processor = EmailProcessor()
        self.db_manager = DatabaseManager()
        
        # 2. Postgres Connection Pool for LangGraph Checkpointer
        # Use the same connection details as the main DB or specific one
        # Docker internal: host='postgres', but we might need construction from env
        pg_host = os.getenv("POSTGRES_HOST", "localhost")
        pg_user = os.getenv("POSTGRES_USER", "user")
        pg_pass = os.getenv("POSTGRES_PASSWORD", "password")
        pg_db = os.getenv("POSTGRES_DB", "email_agent")
        pg_port = os.getenv("POSTGRES_PORT", "5432")
        
        dsn = f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
        
        # 3. Setup Checkpointer and Graph
        # Hack: Run setup() with a dedicated autocommit connection to allow CREATE INDEX CONCURRENTLY
        # For setup, we can stick to sync (using PostgresSaver) or just assume it's done via migration script.
        # But let's keep the sync setup logic for now as it's cleaner than async setup in __init__.
        try:
            with psycopg.connect(dsn, autocommit=True) as temp_conn:
                from langgraph.checkpoint.postgres import PostgresSaver as SyncPostgresSaver
                temp_cp = SyncPostgresSaver(temp_conn)
                temp_cp.setup()
        except Exception as e:
            logger.warning(f"Checkpointer setup warning (might be already done): {e}")

        self.pool = AsyncConnectionPool(conninfo=dsn, max_size=20, open=False)
        # checkpointer and graph will be initialized in setup_async() to ensure loop exists.
        logger.info("Application Context Initialized (Pool created, Graph deferred).")

    async def setup_async(self):
        """
        Explicitly open the async connection pool and setup graph.
        Must be called from within a running asyncio loop.
        """
        if self.pool:
            await self.pool.open()
            logger.info("AsyncConnectionPool opened.")
        
        if self.graph is None:
            checkpointer = AsyncPostgresSaver(self.pool)
            self.graph = build_graph(checkpointer=checkpointer)
            logger.info("Graph initialized with AsyncPostgresSaver.")

    def close(self):
        if self.db_manager:
            self.db_manager.close()
        # Async pool close is usually async, skipping sync close here for now.
        pass

# Singleton instance
app_context = AppContext()

def get_app_context():
    if app_context.exchange_client is None:
        app_context.initialize()
    return app_context
