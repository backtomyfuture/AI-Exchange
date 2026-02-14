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
from src.utils.db_async import AsyncDatabaseManager
from src.config import get_settings

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
        settings = get_settings()

        # Suppress verbose logs from third-party libraries globally
        logging.getLogger("fontTools").setLevel(logging.ERROR)
        logging.getLogger("weasyprint").setLevel(logging.ERROR)

        # 1. Core Components
        self.exchange_client = ExchangeClient(settings)
        self.email_processor = EmailProcessor()
        # Async DB Manager (initialized in setup_async)
        self.db_manager = AsyncDatabaseManager(settings)
        
        # 2. Postgres Connection Pool for LangGraph Checkpointer
        dsn = f"postgresql://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}@{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}"
        
        # 3. Setup Checkpointer and Graph
        # We skip sync setup logic here assuming DB is initialized or will be by AsyncDatabaseManager
        
        self.pool = AsyncConnectionPool(conninfo=dsn, max_size=20, open=False)
        # checkpointer and graph will be initialized in setup_async() to ensure loop exists.
        logger.info("Application Context Initialized (Pool created, Graph deferred).")

    async def setup_async(self):
        """
        Explicitly open the async connection pool and setup graph.
        Must be called from within a running asyncio loop.
        """
        if self.db_manager:
            await self.db_manager.open()

        if self.pool:
            await self.pool.open()
            logger.info("AsyncConnectionPool opened.")

        # Initialize folder cache and precompute folder policies for webhook routing.
        try:
            await self.exchange_client.get_all_folders()
            settings = get_settings()
            folders_full = {
                folder.strip()
                for folder in settings.EXCHANGE_FOLDERS_FULL.split(",")
                if folder.strip()
            }
            folders_archive = {
                folder.strip()
                for folder in settings.EXCHANGE_FOLDERS_ARCHIVE.split(",")
                if folder.strip()
            }
            self.exchange_client.init_folder_policies(folders_full, folders_archive)
            logger.info("Exchange folder cache and routing policies initialized.")
        except Exception as e:
            logger.warning(
                "Failed to initialize folder cache (will use safe defaults): %s",
                e,
            )
        
        if self.graph is None:
            checkpointer = AsyncPostgresSaver(self.pool)
            await checkpointer.setup() 
            self.graph = build_graph(checkpointer=checkpointer)
            logger.info("Graph initialized with AsyncPostgresSaver.")

    async def close(self):
        if self.db_manager:
            await self.db_manager.close()
        # Async pool close
        if self.pool:
             await self.pool.close()

# Singleton instance
app_context = AppContext()

def get_app_context():
    if app_context.exchange_client is None:
        app_context.initialize()
    return app_context
