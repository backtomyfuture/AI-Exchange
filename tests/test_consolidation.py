import asyncio
import unittest
from unittest.mock import MagicMock, patch, AsyncMock

# Import src.main. Since we are in the same project, we can import it.
# We might need to ensure src is in path, but usually python -m handles that.
from src.main import main

class TestServiceConsolidation(unittest.IsolatedAsyncioTestCase):
    
    @patch('src.main.lark_app')
    @patch('src.main.exchange_stop_worker', new_callable=AsyncMock)
    @patch('src.main.exchange_start_worker', new_callable=AsyncMock)
    @patch('src.main.get_app_context')
    async def test_main_startup_shutdown(self, mock_get_ctx, mock_start_worker, mock_stop_worker, mock_lark_app):
        """
        Verify that main() initializes components, runs loops, and handles shutdown.
        """
        # 1. Setup Mock Context
        # When get_app_context() is called, return this ctx
        mock_ctx = MagicMock()
        
        # Setup async methods to return awaitables
        # option A: AsyncMock
        mock_ctx.setup_async = AsyncMock()
        mock_ctx.close = AsyncMock()
        
        mock_get_ctx.return_value = mock_ctx
        
        # 2. Execution
        # Run main() as a task
        main_task = asyncio.create_task(main())
        
        # Allow it to start up
        await asyncio.sleep(0.5)
        
        # 4. Verification
        # Check if context was initialized
        mock_get_ctx.assert_called_once()
        mock_ctx.setup_async.assert_called_once()
        
        # Check if Lark was initialized
        mock_lark_app.init_lark_app.assert_called_once()
        mock_lark_app.start_lark_ws.assert_called_once()
        
        # Check if webhook worker was started
        mock_start_worker.assert_awaited_once_with(mock_ctx)
        
        # 4. Shutdown
        main_task.cancel()
        try:
            await main_task
        except asyncio.CancelledError:
            pass
            
        # Verify worker shutdown and context close were called
        mock_stop_worker.assert_awaited_once()
        # Verify close was called
        mock_ctx.close.assert_called_once()

if __name__ == '__main__':
    unittest.main()
