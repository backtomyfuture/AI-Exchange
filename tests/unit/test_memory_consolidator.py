"""Tests for the memory consolidation system."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.memory.consolidator import MemoryConsolidator


@pytest.mark.asyncio
async def test_consolidator_skips_with_few_records():
    """Consolidation should skip when not enough records."""
    db = MagicMock()
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchall = AsyncMock(return_value=[{"id": "1", "status": "sent"}])
    cursor_cm = MagicMock()
    cursor_cm.__aenter__ = AsyncMock(return_value=cur)
    cursor_cm.__aexit__ = AsyncMock(return_value=False)
    conn.cursor.return_value = cursor_cm
    conn_cm = MagicMock()
    conn_cm.__aenter__ = AsyncMock(return_value=conn)
    conn_cm.__aexit__ = AsyncMock(return_value=False)
    db.get_connection.return_value = conn_cm

    consolidator = MemoryConsolidator(db_manager=db)
    result = await consolidator.consolidate(days=7, min_records=5)

    assert result["insights_count"] == 0
    assert result["stored"] is False


@pytest.mark.asyncio
async def test_consolidator_returns_insights_structure():
    """Consolidation result should have expected keys."""
    db = AsyncMock()
    consolidator = MemoryConsolidator(db_manager=db)

    with patch.object(consolidator, "_fetch_recent_records", return_value=[]):
        result = await consolidator.consolidate(days=7, min_records=0)

    assert "insights_count" in result
    assert "summary" in result
    assert "stored" in result
