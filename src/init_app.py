import logging
from psycopg_pool import AsyncConnectionPool

from src.db.checkpoint_saver import (
    CheckpointWriteFenceConfigurationError,
    CheckpointWriteGuard,
    FencedAsyncPostgresSaver,
    configure_checkpoint_pool_connection,
)
from src.graph.builder import build_graph
from src.graph.dependencies import GraphDependencies
from src.utils.exchange_api import ExchangeClient
from src.utils.email_processor import EmailProcessor
from src.utils.db_async import AsyncDatabaseManager
from src.config import get_settings, resolve_secret
from src.storage import EncryptedFileContentStore
from src.ingestion.runtime import IngestionRuntime, build_ingestion_runtime

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
        self.ingestion_runtime: IngestionRuntime | None = None
        self._checkpoint_write_guard: CheckpointWriteGuard | None = None
        self._checkpoint_setup_started = False

    def create_ingestion_runtime(self, settings=None) -> IngestionRuntime:
        """Create the one Phase-2 runtime without initializing legacy services."""

        if self.ingestion_runtime is not None:
            raise RuntimeError("ingestion_runtime_already_created")
        self.ingestion_runtime = build_ingestion_runtime(settings or get_settings())
        return self.ingestion_runtime

    def release_ingestion_runtime(self, runtime: IngestionRuntime) -> None:
        """Release only the runtime instance owned by this context."""

        if self.ingestion_runtime is not runtime:
            raise RuntimeError("ingestion_runtime_ownership_mismatch")
        self.ingestion_runtime = None

    def bind_checkpoint_write_guard(self, guard: CheckpointWriteGuard) -> None:
        """Bind the dedicated-session proof exactly once before graph setup."""

        if (
            self.graph is not None
            or self._checkpoint_setup_started
            or self._checkpoint_write_guard is not None
        ):
            raise CheckpointWriteFenceConfigurationError(
                "checkpoint_write_fence_binding_closed"
            )
        if not callable(guard):
            raise CheckpointWriteFenceConfigurationError(
                "checkpoint_write_fence_not_bound"
            )
        self._checkpoint_write_guard = guard

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
        self.pool = AsyncConnectionPool(
            conninfo=dsn,
            max_size=20,
            kwargs=connection_kwargs,
            configure=configure_checkpoint_pool_connection,
            open=False,
        )
        # checkpointer and graph will be initialized in setup_async() to ensure loop exists.
        logger.info("Application Context Initialized (Pool created, Graph deferred).")

    async def setup_async(self):
        """
        Explicitly open the async connection pool and setup graph.
        Must be called from within a running asyncio loop.
        """
        if self._checkpoint_write_guard is None:
            raise CheckpointWriteFenceConfigurationError(
                "checkpoint_write_fence_not_bound"
            )
        self._checkpoint_setup_started = True

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
            checkpointer = FencedAsyncPostgresSaver(
                self.pool,
                write_guard=self._checkpoint_write_guard,
            )
            self.graph = build_graph(
                checkpointer=checkpointer,
                dependencies=self.graph_dependencies,
            )
            logger.info("Graph initialized with FencedAsyncPostgresSaver.")

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


def get_runtime_app_context() -> AppContext:
    """Return the owner without constructing Graph, Exchange or Lark resources."""

    return app_context
