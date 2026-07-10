import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import src.exchange_service as exchange_service
from src.exchange_service import WORKER_CONCURRENCY, WebhookWorker


def _new_mail_payload(mail_id: str) -> dict:
    return {
        "event_type": "NewMailEvent",
        "item_id": {"id": mail_id},
        "parent_folder_id": {"id": "INBOX_FOLDER_ID"},
        "item": {
            "id": mail_id,
            "subject": f"Subject {mail_id}",
            "sender": "sender@example.com",
            "body": "body",
            "received_at": "2026-01-01T00:00:00Z",
        },
    }


async def _wait_until(predicate, timeout: float = 1.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0)


@pytest.fixture
def ctx():
    exchange_client = MagicMock()
    exchange_client._folder_policies = {"INBOX_FOLDER_ID": "full"}
    exchange_client.get_folder_policy.return_value = "full"
    exchange_client.get_folder_name.return_value = "Inbox"

    context = MagicMock()
    context.exchange_client = exchange_client
    return context


@pytest.fixture
def processor():
    started = asyncio.Event()
    released = asyncio.Event()
    released.set()
    mock = AsyncMock()
    mock.active = 0
    mock.completed = 0
    mock.max_active = 0

    async def process(email_data, context, skip_analysis=False):
        mock.active += 1
        mock.max_active = max(mock.max_active, mock.active)
        started.set()
        try:
            await released.wait()
        finally:
            mock.active -= 1
            mock.completed += 1

    def block() -> None:
        started.clear()
        released.clear()

    def release() -> None:
        released.set()

    mock.side_effect = process
    mock.started = started
    mock.block = block
    mock.release = release

    with patch("src.exchange_service.process_and_archive_email", mock):
        yield mock
        released.set()


@pytest.fixture
def worker(ctx, processor):
    return WebhookWorker(ctx)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("concurrency", 0),
        ("concurrency", -1),
        ("concurrency", "3"),
        ("concurrency", 1.5),
        ("concurrency", True),
        ("concurrency", False),
        ("queue_maxsize", 0),
        ("queue_maxsize", -1),
        ("queue_maxsize", "500"),
        ("queue_maxsize", 1.5),
        ("queue_maxsize", True),
        ("queue_maxsize", False),
    ],
)
def test_worker_rejects_non_positive_or_non_integer_limits(ctx, field, value):
    with pytest.raises(ValueError, match=field):
        WebhookWorker(ctx, **{field: value})


@pytest.mark.asyncio
async def test_task_done_happens_after_processing(worker, processor):
    processor.block()
    join_task = None
    await worker.start()
    try:
        result = await worker.enqueue_event(
            _new_mail_payload("mail-1"),
            header_event="NewMailEvent",
        )
        assert result["queued"] is True
        await asyncio.wait_for(processor.started.wait(), timeout=1.0)

        join_task = asyncio.create_task(worker.queue.join())
        await asyncio.sleep(0)

        assert join_task.done() is False

        processor.release()
        await asyncio.wait_for(join_task, timeout=1.0)
    finally:
        processor.release()
        if processor.await_count:
            await _wait_until(lambda: processor.completed >= processor.await_count)
        if join_task is not None and not join_task.done():
            join_task.cancel()
            await asyncio.gather(join_task, return_exceptions=True)
        await worker.stop()


@pytest.mark.asyncio
async def test_worker_creates_only_fixed_consumers(ctx, processor):
    worker = WebhookWorker(ctx, concurrency=WORKER_CONCURRENCY)
    await worker.start()
    try:
        first_tasks = tuple(worker.consumer_tasks)
        await worker.start()

        assert len(first_tasks) == worker.concurrency == WORKER_CONCURRENCY
        assert tuple(worker.consumer_tasks) == first_tasks

        for index in range(100):
            result = await worker.enqueue_event(_new_mail_payload(f"mail-{index}"))
            assert result["queued"] is True

        await asyncio.wait_for(worker.queue.join(), timeout=1.0)

        assert processor.await_count == 100
        assert tuple(worker.consumer_tasks) == first_tasks
        assert all(not task.done() for task in first_tasks)
    finally:
        processor.release()
        await worker.stop()


@pytest.mark.asyncio
async def test_worker_honors_configured_queue_size_and_concurrency(ctx, processor):
    processor.block()
    worker = WebhookWorker(ctx, queue_maxsize=7, concurrency=2)
    await worker.start()
    try:
        assert worker.queue.maxsize == 7
        assert worker.concurrency == 2
        assert len(worker.consumer_tasks) == 2

        for index in range(6):
            result = await worker.enqueue_event(_new_mail_payload(f"mail-{index}"))
            assert result["queued"] is True

        await _wait_until(lambda: processor.await_count == 2)
        assert processor.max_active == 2

        processor.release()
        await asyncio.wait_for(worker.queue.join(), timeout=1.0)
        assert processor.await_count == 6
    finally:
        processor.release()
        await worker.stop()


@pytest.mark.asyncio
async def test_stop_closes_intake_before_draining(worker, processor):
    processor.block()
    await worker.start()
    stop_task = None
    try:
        await worker.enqueue_event(_new_mail_payload("mail-1"))
        await asyncio.wait_for(processor.started.wait(), timeout=1.0)

        stop_task = asyncio.create_task(worker.stop())
        await asyncio.sleep(0)

        with pytest.raises(RuntimeError, match="not accepting"):
            await worker.enqueue_event(_new_mail_payload("mail-2"))

        processor.release()
        await asyncio.wait_for(stop_task, timeout=1.0)
    finally:
        processor.release()
        if stop_task is not None:
            await asyncio.gather(stop_task, return_exceptions=True)
        await worker.stop()


@pytest.mark.asyncio
async def test_stop_timeout_cancels_and_collects_consumers(worker, processor):
    processor.block()
    await worker.start()
    tasks = tuple(worker.consumer_tasks)
    try:
        await worker.enqueue_event(_new_mail_payload("mail-1"))
        await asyncio.wait_for(processor.started.wait(), timeout=1.0)

        await worker.stop(drain_timeout=0.01)

        assert all(task.done() for task in tasks)
        await asyncio.wait_for(worker.queue.join(), timeout=1.0)
        await worker.stop(drain_timeout=0.01)
    finally:
        processor.release()
        await worker.stop()


@pytest.mark.asyncio
async def test_stop_timeout_accounts_for_events_not_taken_by_consumers(
    ctx,
    processor,
    caplog,
):
    processor.block()
    worker = WebhookWorker(ctx, concurrency=2)
    await worker.start()
    try:
        for index in range(5):
            await worker.enqueue_event(_new_mail_payload(f"mail-{index}"))

        await _wait_until(lambda: processor.await_count == 2)
        assert worker.queue.qsize() == 3

        with caplog.at_level(logging.WARNING, logger="ExchangeService"):
            await worker.stop(drain_timeout=0.01)

        assert worker.queue.empty()
        await asyncio.wait_for(worker.queue.join(), timeout=1.0)
        assert worker.shutdown_cancelled_count == 3
        assert "shutdown_cancelled=3" in caplog.text
        assert "sender@example.com" not in caplog.text

        await worker.stop(drain_timeout=0.01)
        assert worker.shutdown_cancelled_count == 3
    finally:
        processor.release()
        while not worker.queue.empty():
            worker.queue.get_nowait()
            worker.queue.task_done()
        await worker.stop()


@pytest.mark.asyncio
async def test_stop_cancellation_finishes_cleanup_before_reraising(ctx, processor):
    processor.block()
    worker = WebhookWorker(ctx, concurrency=2)
    await worker.start()
    tasks = tuple(worker.consumer_tasks)
    stop_task = None
    try:
        for index in range(5):
            await worker.enqueue_event(_new_mail_payload(f"mail-{index}"))
        await _wait_until(lambda: processor.await_count == 2)

        stop_task = asyncio.create_task(worker.stop(drain_timeout=60.0))
        await asyncio.sleep(0)
        assert stop_task.done() is False
        with pytest.raises(RuntimeError, match="not accepting"):
            await worker.enqueue_event(_new_mail_payload("mail-after-stop"))

        stop_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stop_task

        assert all(task.done() for task in tasks)
        assert worker.queue.empty()
        await asyncio.wait_for(worker.queue.join(), timeout=1.0)
        assert worker.shutdown_cancelled_count == 3

        await worker.stop(drain_timeout=0.01)
        assert worker.shutdown_cancelled_count == 3
    finally:
        processor.release()
        if stop_task is not None:
            stop_task.cancel()
            await asyncio.gather(stop_task, return_exceptions=True)
        while not worker.queue.empty():
            worker.queue.get_nowait()
            worker.queue.task_done()
        await worker.stop()


@pytest.mark.asyncio
async def test_cancellation_during_shielded_cleanup_waits_for_cleanup(worker):
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    original_finish_stop = worker._finish_stop
    stop_task = None

    async def delayed_finish_stop(tasks, *, cancel_queued):
        cleanup_started.set()
        await release_cleanup.wait()
        await original_finish_stop(tasks, cancel_queued=cancel_queued)

    await worker.start()
    tasks = tuple(worker.consumer_tasks)
    try:
        with patch.object(worker, "_finish_stop", side_effect=delayed_finish_stop):
            stop_task = asyncio.create_task(worker.stop(drain_timeout=1.0))
            await asyncio.wait_for(cleanup_started.wait(), timeout=1.0)

            stop_task.cancel()
            await asyncio.sleep(0)
            assert stop_task.done() is False

            release_cleanup.set()
            with pytest.raises(asyncio.CancelledError):
                await stop_task

        assert all(task.done() for task in tasks)
        assert worker.queue.empty()
        await asyncio.wait_for(worker.queue.join(), timeout=1.0)
        assert not any(
            task.get_name() == "exchange-webhook-stop-cleanup" and not task.done()
            for task in asyncio.all_tasks()
        )
    finally:
        release_cleanup.set()
        if stop_task is not None:
            stop_task.cancel()
            await asyncio.gather(stop_task, return_exceptions=True)
        await worker.stop()


@pytest.mark.asyncio
async def test_module_stop_worker_clears_globals_when_cancelled(
    ctx,
    processor,
    monkeypatch,
):
    processor.block()
    worker = WebhookWorker(ctx, concurrency=2)
    await worker.start()
    stop_task = None
    monkeypatch.setattr(exchange_service, "_worker", worker)
    monkeypatch.setattr(exchange_service, "_webhook_queue", worker.queue)
    monkeypatch.setattr(exchange_service, "_worker_ctx", ctx)
    monkeypatch.setattr(exchange_service, "_worker_semaphore", worker._semaphore)
    try:
        for index in range(5):
            await worker.enqueue_event(_new_mail_payload(f"mail-{index}"))
        await _wait_until(lambda: processor.await_count == 2)

        stop_task = asyncio.create_task(exchange_service.stop_worker())
        await asyncio.sleep(0)
        assert stop_task.done() is False

        stop_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stop_task

        assert exchange_service._worker is None
        assert exchange_service._webhook_queue is None
        assert exchange_service._worker_ctx is None
        assert exchange_service._worker_semaphore is None
        assert worker.queue.empty()
        await asyncio.wait_for(worker.queue.join(), timeout=1.0)
    finally:
        processor.release()
        if stop_task is not None:
            stop_task.cancel()
            await asyncio.gather(stop_task, return_exceptions=True)
        while not worker.queue.empty():
            worker.queue.get_nowait()
            worker.queue.task_done()
        await worker.stop()


@pytest.mark.asyncio
async def test_start_and_stop_are_idempotent_without_task_leaks(worker):
    await worker.start()
    tasks = tuple(worker.consumer_tasks)

    await worker.start()
    assert tuple(worker.consumer_tasks) == tasks

    await worker.stop(drain_timeout=1.0)
    await worker.stop(drain_timeout=1.0)

    assert all(task.done() for task in tasks)
