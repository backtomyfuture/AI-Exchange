import logging
from psycopg_pool import AsyncConnectionPool
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from src.graph.builder import build_graph
from src.graph.dependencies import GraphDependencies
from src.utils.exchange_api import ExchangeClient
from src.utils.email_processor import EmailProcessor
from src.utils.db_async import AsyncDatabaseManager
from src.config import get_settings, resolve_secret
from src.storage import EncryptedFileContentStore

logger = logging.getLogger(__name__)

class AppContext:
    def __init__(self):
        self.exchange_client = None
        self.email_processor = None
        self.db_manager = None
        self.graph = None
        self.pool = None
        self.content_store = None
        self.graph_dependencies = None

    def initialize(self):
        """
        Initialize all shared components.
        """
        logger.info("Initializing Application Context...")
        settings = get_settings()

        self.content_store = EncryptedFileContentStore(
            root=settings.CONTENT_STORE_ROOT,
            key=resolve_secret(settings.CONTENT_STORE_KEY),
            key_version=settings.CONTENT_STORE_KEY_VERSION,
        )

        # Suppress verbose logs from third-party libraries globally
        logging.getLogger("fontTools").setLevel(logging.ERROR)
        logging.getLogger("weasyprint").setLevel(logging.ERROR)

        # 1. Core Components
        self.exchange_client = ExchangeClient(settings)
        self.email_processor = EmailProcessor()
        # Async DB Manager (initialized in setup_async)
        self.db_manager = AsyncDatabaseManager(settings)
        self.graph_dependencies = GraphDependencies(
            content_store=self.content_store,
            drafts=self.db_manager,
        )
        
        # 2. Postgres Connection Pool for LangGraph Checkpointer
        dsn = settings.database_url
        
        # 3. Setup Checkpointer and Graph
        # We skip sync setup logic here assuming DB is initialized or will be by AsyncDatabaseManager
        
        connection_kwargs = {"autocommit": True, "prepare_threshold": 0}
        self.pool = AsyncConnectionPool(conninfo=dsn, max_size=20, kwargs=connection_kwargs, open=False)
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
        except Exception as exc:
            logger.warning(
                "Failed to initialize folder cache; using safe defaults: error_type=%s",
                type(exc).__name__,
            )
        
        if self.graph is None:
            checkpointer = AsyncPostgresSaver(self.pool)
            self.graph = build_graph(
                checkpointer=checkpointer,
                dependencies=self.graph_dependencies,
            )
            logger.info("Graph initialized with AsyncPostgresSaver.")

    async def close(self):
        if self.exchange_client:
            await self.exchange_client.close()
        if self.db_manager:
            await self.db_manager.close()
        if self.pool:
            await self.pool.close()

# Singleton instance
app_context = AppContext()

def get_app_context():
    if app_context.exchange_client is None:
        app_context.initialize()
    return app_context
