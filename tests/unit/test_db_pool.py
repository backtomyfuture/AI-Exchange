import pytest


@pytest.mark.asyncio
async def test_db_manager_uses_pool():
    """AsyncDatabaseManager should use connection pool instead of single connection."""
    from src.utils.db_async import AsyncDatabaseManager
    from src.config import get_settings

    settings = get_settings()
    db = AsyncDatabaseManager(settings)

    assert hasattr(db, "_pool"), "AsyncDatabaseManager should have _pool attribute"
    assert not hasattr(db, "_conn"), "AsyncDatabaseManager should NOT have _conn single connection"


@pytest.mark.asyncio
async def test_db_manager_get_settings():
    """AsyncDatabaseManager should use get_settings() instead of os.getenv()."""
    from src.utils.db_async import AsyncDatabaseManager
    from src.config import get_settings

    settings = get_settings()
    db = AsyncDatabaseManager(settings)

    assert settings.POSTGRES_HOST in db.dsn
    assert settings.POSTGRES_DB in db.dsn
