
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from src.scheduler.polling import run_polling_loop

@pytest.mark.asyncio
async def test_polling_loop_logic():
    # Setup Mocks
    ctx = MagicMock()
    ctx.exchange_client.get_recent_emails = AsyncMock(return_value=[
        {"id": "msg1", "subject": "Test 1"},
        {"id": "msg2", "subject": "Test 2"}
    ])
    
    # Mock process_and_archive_email
    # Since we import it in the module, we need to correct the import path for patching
    # But since we are testing logic, we can just patch it where it is used.
    # However, since run_polling_loop imports process_and_archive_email directly,
    # we should mock it via sys.modules or patch target.
    pass

@pytest.mark.asyncio
async def test_polling_scheduler_flow(mocker):
    # Mock dependencies
    mock_process = mocker.patch("src.scheduler.polling.process_and_archive_email", new_callable=AsyncMock)
    
    ctx = MagicMock()
    # Mock ensure get_recent_emails returns something
    ctx.exchange_client.get_recent_emails = AsyncMock(return_value=[
        {"id": "msg1", "subject": "Test 1"},
    ])
    
    # Run loop for short time then cancel
    task = asyncio.create_task(run_polling_loop(ctx, interval=0.1, startup_delay=0))
    
    await asyncio.sleep(0.2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    
    # Verify calls
    assert ctx.exchange_client.get_recent_emails.called
    assert mock_process.called
    assert mock_process.call_args[0][0]["id"] == "msg1"
    # Ensure skip_analysis was passed as False (we want to re-analyze missed emails)
    assert not mock_process.call_args[1]["skip_analysis"]
