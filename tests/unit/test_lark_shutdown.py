import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from src.utils import lark_app


@pytest.fixture(autouse=True)
def reset_lark_intake(monkeypatch: pytest.MonkeyPatch):
    lark_app.enable_lark_intake()
    monkeypatch.setattr(lark_app, "worker_loop", None)
    yield
    lark_app.enable_lark_intake()


def test_safe_async_run_closes_coroutine_when_intake_is_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def operation() -> None:
        return None

    lark_app.disable_lark_intake()
    coroutine = operation()
    run = Mock(wraps=asyncio.run)
    monkeypatch.setattr(asyncio, "run", run)

    with pytest.raises(RuntimeError, match="lark_intake_disabled"):
        lark_app.safe_async_run(coroutine)

    assert coroutine.cr_frame is None
    run.assert_not_called()


def test_safe_async_wait_closes_coroutine_when_intake_is_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def operation() -> None:
        return None

    lark_app.disable_lark_intake()
    coroutine = operation()
    run = Mock(wraps=asyncio.run)
    monkeypatch.setattr(asyncio, "run", run)

    with pytest.raises(RuntimeError, match="lark_intake_disabled"):
        lark_app.safe_async_wait(coroutine)

    assert coroutine.cr_frame is None
    run.assert_not_called()


@pytest.mark.asyncio
async def test_safe_async_run_tracks_created_task_until_completion() -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def operation() -> None:
        started.set()
        await release.wait()

    task = lark_app.safe_async_run(operation())
    await started.wait()

    assert task in lark_app._lark_background_futures

    release.set()
    await task
    await asyncio.sleep(0)

    assert task not in lark_app._lark_background_futures


@pytest.mark.asyncio
async def test_safe_async_run_tracks_submitted_future_until_completion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(lark_app, "worker_loop", loop)
    release = asyncio.Event()

    async def operation() -> None:
        await release.wait()

    future = lark_app.safe_async_run(operation())

    assert future in lark_app._lark_background_futures

    release.set()
    await asyncio.wrap_future(future)
    await asyncio.sleep(0)

    assert future not in lark_app._lark_background_futures


@pytest.mark.asyncio
async def test_drain_lark_background_tasks_waits_for_created_task() -> None:
    completed = asyncio.Event()

    async def operation() -> None:
        await asyncio.sleep(0)
        completed.set()

    task = lark_app.safe_async_run(operation())

    await lark_app.drain_lark_background_tasks(timeout_seconds=1)

    assert task.done()
    assert completed.is_set()
    assert not lark_app._lark_background_futures


@pytest.mark.asyncio
async def test_drain_lark_background_tasks_waits_for_submitted_future(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(lark_app, "worker_loop", loop)
    completed = asyncio.Event()

    async def operation() -> None:
        await asyncio.sleep(0)
        completed.set()

    future = lark_app.safe_async_run(operation())

    await lark_app.drain_lark_background_tasks(timeout_seconds=1)

    assert future.done()
    assert completed.is_set()
    assert not lark_app._lark_background_futures


@pytest.mark.asyncio
async def test_stop_lark_intake_rejects_new_work_before_draining() -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def existing_operation() -> None:
        started.set()
        await release.wait()

    existing = lark_app.safe_async_run(existing_operation())
    await started.wait()
    stopping = asyncio.create_task(lark_app.stop_lark_intake(timeout_seconds=1))
    await asyncio.sleep(0)

    async def rejected_operation() -> None:
        return None

    rejected = rejected_operation()
    with pytest.raises(RuntimeError, match="lark_intake_disabled"):
        lark_app.safe_async_run(rejected)
    assert rejected.cr_frame is None

    release.set()
    await stopping

    assert existing.done()
    assert not lark_app._lark_background_futures


@pytest.mark.asyncio
async def test_stop_lark_intake_cancels_and_collects_after_timeout() -> None:
    started = asyncio.Event()
    finalized = asyncio.Event()

    async def operation() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            finalized.set()

    task = lark_app.safe_async_run(operation())
    await started.wait()

    await lark_app.stop_lark_intake(timeout_seconds=0)

    assert task.cancelled()
    assert finalized.is_set()
    assert not lark_app._lark_background_futures


@pytest.mark.asyncio
async def test_stop_waits_for_foreign_task_async_finally(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(lark_app, "worker_loop", loop)
    started = asyncio.Event()
    finalized = asyncio.Event()

    async def operation() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0.01)
            finalized.set()

    lark_app.safe_async_run(operation())
    await started.wait()

    await lark_app.stop_lark_intake(timeout_seconds=0)

    assert finalized.is_set()
    assert not lark_app._lark_background_futures
    assert not lark_app._lark_background_completions


@pytest.mark.asyncio
async def test_stop_has_hard_bound_for_cancellation_resistant_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(lark_app, "worker_loop", loop)
    monkeypatch.setattr(lark_app, "_LARK_CANCEL_FINALIZE_SECONDS", 0.01)
    started = asyncio.Event()
    release = asyncio.Event()
    finalized = asyncio.Event()

    async def operation() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await release.wait()
        finally:
            finalized.set()

    lark_app.safe_async_run(operation())
    await started.wait()

    with pytest.raises(RuntimeError, match="lark_background_shutdown_timeout"):
        await lark_app.stop_lark_intake(timeout_seconds=0)

    assert not finalized.is_set()
    release.set()
    await asyncio.wait_for(finalized.wait(), timeout=1)
    await asyncio.sleep(0)
    assert not lark_app._lark_background_futures
    assert not lark_app._lark_background_completions


def test_stop_lark_ws_disconnects_and_joins_sdk_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Client:
        _auto_reconnect = True

        async def _disconnect(self) -> None:
            return None

    client = Client()
    thread = Mock()
    thread.is_alive.return_value = False
    sdk_loop = Mock()
    sdk_loop.is_running.return_value = True
    submitted = Mock()
    submitted.result.return_value = None

    def submit(coro, loop):
        assert loop is sdk_loop
        coro.close()
        return submitted

    monkeypatch.setattr(lark_app, "lark_ws_client", client)
    monkeypatch.setattr(lark_app, "_lark_ws_thread", thread)
    monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", submit)
    from lark_oapi.ws import client as ws_client_module

    monkeypatch.setattr(ws_client_module, "loop", sdk_loop)

    lark_app.stop_lark_ws(timeout_seconds=0.25)

    assert client._auto_reconnect is False
    submitted.result.assert_called_once_with(timeout=0.25)
    sdk_loop.call_soon_threadsafe.assert_called_once_with(sdk_loop.stop)
    thread.join.assert_called_once_with(timeout=0.25)
    assert lark_app.lark_ws_client is None
    assert lark_app._lark_ws_thread is None


@pytest.mark.asyncio
async def test_stop_collects_submitted_coroutine_after_cancelling_future(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(lark_app, "worker_loop", loop)
    started = asyncio.Event()
    finalized = asyncio.Event()

    async def operation() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            finalized.set()

    future = lark_app.safe_async_run(operation())
    await started.wait()

    await lark_app.stop_lark_intake(timeout_seconds=0)

    assert future.cancelled()
    assert finalized.is_set()
    assert not lark_app._lark_background_futures


@pytest.mark.asyncio
async def test_stop_drains_safe_async_wait_accepted_before_disable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(lark_app, "worker_loop", loop)
    started = asyncio.Event()
    completed = asyncio.Event()
    outcomes: list[str] = []

    async def operation() -> str:
        started.set()
        await asyncio.sleep(0.05)
        completed.set()
        return "done"

    runner = threading.Thread(
        target=lambda: outcomes.append(lark_app.safe_async_wait(operation()))
    )
    runner.start()
    await started.wait()

    await lark_app.stop_lark_intake(timeout_seconds=1)
    try:
        assert completed.is_set()
    finally:
        await asyncio.sleep(0.06)
        runner.join(timeout=1)

    assert outcomes == ["done"]
    assert not runner.is_alive()


@pytest.mark.asyncio
async def test_safe_async_wait_timeout_stays_registered_until_owner_finally(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(lark_app, "worker_loop", loop)
    monkeypatch.setattr(lark_app, "ACTION_WAIT_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(lark_app, "_LARK_CANCEL_FINALIZE_SECONDS", 0.01)
    started = asyncio.Event()
    cancellation_seen = asyncio.Event()
    release_operation = asyncio.Event()
    finalized = asyncio.Event()
    outcomes: list[BaseException | str] = []

    async def operation() -> str:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancellation_seen.set()
            await release_operation.wait()
        finally:
            await asyncio.sleep(0)
            finalized.set()
        return "done"

    def run_wait() -> None:
        try:
            outcomes.append(lark_app.safe_async_wait(operation()))
        except BaseException as exc:
            outcomes.append(exc)

    runner = threading.Thread(target=run_wait)
    runner.start()
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        await asyncio.wait_for(cancellation_seen.wait(), timeout=1)
        await asyncio.to_thread(runner.join, 1)

        assert len(outcomes) == 1
        assert isinstance(outcomes[0], RuntimeError)
        assert str(outcomes[0]) == "safe_async_wait_timeout"
        assert not runner.is_alive()
        assert not finalized.is_set()

        with pytest.raises(RuntimeError, match="lark_background_shutdown_timeout"):
            await lark_app.stop_lark_intake(timeout_seconds=0)

        assert not finalized.is_set()
    finally:
        release_operation.set()
        if started.is_set():
            await asyncio.wait_for(finalized.wait(), timeout=1)
        await asyncio.sleep(0)
        await asyncio.to_thread(runner.join, 1)

    assert not runner.is_alive()
    assert not lark_app._lark_background_futures
    assert not lark_app._lark_background_completions


@pytest.mark.asyncio
async def test_stop_lark_intake_is_idempotent() -> None:
    await lark_app.stop_lark_intake(timeout_seconds=0)
    await lark_app.stop_lark_intake(timeout_seconds=0)

    async def operation() -> None:
        return None

    coroutine = operation()
    with pytest.raises(RuntimeError, match="lark_intake_disabled"):
        lark_app.safe_async_wait(coroutine)

    assert coroutine.cr_frame is None


def test_disable_does_not_wait_for_an_accepted_no_loop_fallback() -> None:
    operation_started = threading.Event()
    release_operation = threading.Event()
    disable_returned = threading.Event()

    async def operation() -> None:
        operation_started.set()
        await asyncio.to_thread(release_operation.wait)

    runner = threading.Thread(target=lambda: lark_app.safe_async_run(operation()))
    runner.start()
    assert operation_started.wait(timeout=1)

    def disable() -> None:
        lark_app.disable_lark_intake()
        disable_returned.set()

    stopper = threading.Thread(target=disable)
    stopper.start()
    try:
        assert disable_returned.wait(timeout=0.1)
        assert runner.is_alive()
    finally:
        release_operation.set()
        runner.join(timeout=1)
        stopper.join(timeout=1)

    assert not runner.is_alive()
    assert not stopper.is_alive()
    assert disable_returned.is_set()


def test_stop_tracks_accepted_no_loop_fallback_and_fails_closed_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(lark_app, "_LARK_CANCEL_FINALIZE_SECONDS", 0.01)
    operation_started = threading.Event()
    release_operation = threading.Event()
    stop_returned = threading.Event()
    outcomes: list[BaseException | str] = []

    async def operation() -> None:
        operation_started.set()
        await asyncio.to_thread(release_operation.wait)

    runner = threading.Thread(target=lambda: lark_app.safe_async_run(operation()))
    runner.start()
    assert operation_started.wait(timeout=1)

    def stop() -> None:
        try:
            asyncio.run(lark_app.stop_lark_intake(timeout_seconds=0))
        except BaseException as exc:
            outcomes.append(exc)
        else:
            outcomes.append("returned")
        finally:
            stop_returned.set()

    stopper = threading.Thread(target=stop)
    stopper.start()
    try:
        assert stop_returned.wait(timeout=0.2)
        assert len(outcomes) == 1
        assert isinstance(outcomes[0], RuntimeError)
        assert str(outcomes[0]) == "lark_background_shutdown_timeout"
        assert runner.is_alive()
    finally:
        release_operation.set()
        runner.join(timeout=1)
        stopper.join(timeout=1)

    assert not runner.is_alive()
    assert not stopper.is_alive()
    assert not lark_app._lark_background_futures
    assert not lark_app._lark_background_completions


def test_stop_tracks_accepted_safe_async_wait_no_loop_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(lark_app, "_LARK_CANCEL_FINALIZE_SECONDS", 0.01)
    operation_started = threading.Event()
    release_operation = threading.Event()
    stop_returned = threading.Event()
    stop_outcomes: list[BaseException | str] = []

    async def operation() -> str:
        operation_started.set()
        await asyncio.to_thread(release_operation.wait)
        return "done"

    operation_outcomes: list[str] = []
    runner = threading.Thread(
        target=lambda: operation_outcomes.append(lark_app.safe_async_wait(operation()))
    )
    runner.start()
    assert operation_started.wait(timeout=1)

    def stop() -> None:
        try:
            asyncio.run(lark_app.stop_lark_intake(timeout_seconds=0))
        except BaseException as exc:
            stop_outcomes.append(exc)
        else:
            stop_outcomes.append("returned")
        finally:
            stop_returned.set()

    stopper = threading.Thread(target=stop)
    stopper.start()
    try:
        assert stop_returned.wait(timeout=0.2)
        assert len(stop_outcomes) == 1
        assert isinstance(stop_outcomes[0], RuntimeError)
        assert str(stop_outcomes[0]) == "lark_background_shutdown_timeout"
        assert runner.is_alive()
    finally:
        release_operation.set()
        runner.join(timeout=1)
        stopper.join(timeout=1)

    assert operation_outcomes == ["done"]
    assert not runner.is_alive()
    assert not stopper.is_alive()
    assert not lark_app._lark_background_futures
    assert not lark_app._lark_background_completions


@pytest.mark.asyncio
async def test_init_lark_app_reenables_intake_for_a_new_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = asyncio.get_running_loop()
    settings = SimpleNamespace(
        LARK_APP_ID="",
        LARK_APP_SECRET="",
        LARK_CHAT_ID="",
    )
    monkeypatch.setattr(lark_app, "get_settings", lambda: settings)
    monkeypatch.setattr(lark_app, "init_commands", lambda _db: None)
    monkeypatch.setattr(lark_app, "_register_builtin_commands", lambda: None)
    lark_app.disable_lark_intake()

    lark_app.init_lark_app(None, None, None, worker_loop_arg=loop)

    async def operation() -> str:
        return "accepted"

    future = lark_app.safe_async_run(operation())
    assert await asyncio.wrap_future(future) == "accepted"


@pytest.mark.asyncio
async def test_failed_init_lark_app_leaves_intake_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        lark_app,
        "get_settings",
        Mock(side_effect=RuntimeError("PRIVATE-INIT-FAILURE")),
    )
    lark_app.disable_lark_intake()

    with pytest.raises(RuntimeError, match="PRIVATE-INIT-FAILURE"):
        lark_app.init_lark_app(None, None, None)

    async def operation() -> None:
        return None

    coroutine = operation()
    with pytest.raises(RuntimeError, match="lark_intake_disabled"):
        lark_app.safe_async_run(coroutine)

    assert coroutine.cr_frame is None
