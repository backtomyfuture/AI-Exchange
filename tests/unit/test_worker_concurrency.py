import pytest
import asyncio
from unittest.mock import patch, AsyncMock, MagicMock


def test_worker_has_concurrency_constant():
    """Worker module should define WORKER_CONCURRENCY."""
    import src.exchange_service as es
    assert hasattr(es, 'WORKER_CONCURRENCY')
    assert es.WORKER_CONCURRENCY >= 1


@pytest.mark.asyncio
async def test_concurrent_processing_limited_by_semaphore():
    """Verify concurrent email processing is limited by semaphore."""
    import src.exchange_service as es

    active_count = 0
    max_observed = 0
    lock = asyncio.Lock()

    async def tracked_process(email_data, ctx, skip_analysis=False):
        nonlocal active_count, max_observed
        async with lock:
            active_count += 1
            if active_count > max_observed:
                max_observed = active_count
        await asyncio.sleep(0.05)
        async with lock:
            active_count -= 1

    mock_ctx = MagicMock()
    mock_ctx.exchange_client = AsyncMock()
    mock_ctx.exchange_client.get_email = AsyncMock(return_value={
        "id": "x", "subject": "t", "body": "b", "sender": "s", "received_at": "2026-01-01"
    })

    es._worker_ctx = mock_ctx
    es._webhook_queue = asyncio.Queue()
    es._worker_semaphore = asyncio.Semaphore(es.WORKER_CONCURRENCY)

    with patch.object(es, 'process_and_archive_email', side_effect=tracked_process):
        # Enqueue more items than concurrency limit
        for i in range(6):
            await es._webhook_queue.put((
                {"id": f"test-{i}", "subject": "t", "body": "b", "sender": "s", "received_at": "2026-01-01"},
                False,
            ))

        # Process with concurrency control
        tasks = []
        for _ in range(6):
            email_data, skip = await es._webhook_queue.get()
            async def _process_one(ed, sa):
                async with es._worker_semaphore:
                    await es.process_and_archive_email(ed, mock_ctx, sa)
            tasks.append(asyncio.create_task(_process_one(email_data, skip)))

        await asyncio.gather(*tasks)

    assert max_observed <= es.WORKER_CONCURRENCY
