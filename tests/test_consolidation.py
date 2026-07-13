import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

# Import src.main. Since we are in the same project, we can import it.
# We might need to ensure src is in path, but usually python -m handles that.
from src.main import main


class TestServiceConsolidation(unittest.IsolatedAsyncioTestCase):
    @patch("src.main.lark_app")
    @patch("src.main.exchange_stop_worker", new_callable=AsyncMock)
    @patch("src.main.exchange_start_worker", new_callable=AsyncMock)
    @patch("src.main.get_app_context")
    async def test_main_startup_shutdown(
        self,
        mock_get_ctx,
        mock_start_worker,
        mock_stop_worker,
        mock_lark_app,
    ):
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
        mock_ctx.db_manager.recover_incomplete_approval_states = AsyncMock(
            return_value=0
        )

        mock_get_ctx.return_value = mock_ctx
        mock_lark_app.stop_lark_intake = AsyncMock()
        fence = MagicMock()
        fence.__aenter__ = AsyncMock(return_value=fence)
        fence.__aexit__ = AsyncMock(return_value=None)
        fence.assert_held = AsyncMock()

        with (
            patch(
                "src.main.get_settings",
                return_value=SimpleNamespace(database_url="postgresql://test/test"),
            ),
            patch("src.main.validate_runtime_security"),
            patch(
                "src.main._require_runtime_database_boundary",
                new_callable=AsyncMock,
            ) as database_boundary,
            patch(
                "src.main.RuntimeCheckpointMaintenanceFence",
                return_value=fence,
            ) as fence_factory,
        ):
            main_task = asyncio.create_task(main())
            await asyncio.sleep(0)

            mock_get_ctx.assert_called_once()
            mock_ctx.bind_checkpoint_write_guard.assert_called_once_with(
                fence.assert_held
            )
            mock_ctx.setup_async.assert_called_once()
            mock_ctx.db_manager.recover_incomplete_approval_states.assert_awaited_once()
            mock_lark_app.init_lark_app.assert_called_once()
            mock_lark_app.start_lark_ws.assert_called_once()
            mock_start_worker.assert_awaited_once_with(mock_ctx)
            database_boundary.assert_awaited_once()
            fence_factory.assert_called_once()

            main_task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await main_task

        # Verify worker shutdown and context close were called
        mock_stop_worker.assert_awaited_once()
        mock_lark_app.stop_lark_intake.assert_awaited_once()
        mock_ctx.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
