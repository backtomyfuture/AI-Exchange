import pytest
from unittest.mock import AsyncMock, MagicMock, patch, ANY
from src.domain.email_state import InitialEmailWriteResult, ProcessingOutcome
from src.domain.errors import DatabaseOperationError
from src.exchange_service import process_and_archive_email
from src.storage import ContentRef


def _content_ref():
    from src.exchange_service import get_settings

    return ContentRef(
        account_id=get_settings().EXCHANGE_ACCOUNT_ID,
        object_id="00000000-0000-4000-8000-000000000027",
        key_version="v1",
        sha256="2" * 64,
    )

@pytest.fixture
def mock_context():
    ctx = MagicMock()
    ctx.db_manager = AsyncMock()
    ctx.db_manager.get_content_ref.return_value = _content_ref()
    ctx.content_store = AsyncMock()
    ctx.content_store.put_email.return_value = _content_ref()
    ctx.email_processor = MagicMock()
    ctx.graph = AsyncMock()
    ctx.exchange_client = AsyncMock()
    return ctx


def configure_completed_graph(mock_context, email_data, *, classification, events=()):
    mock_context.content_store.load_email.return_value = email_data
    mock_state = MagicMock()
    mock_state.values = {}
    final_values = {
        "classification": classification,
        "draft_id": None,
        "context_summaries": [],
        "routing_log": [],
        "active_skills": [],
        "draft_to": [],
        "draft_cc": [],
    }

    async def mock_astream(*args, **kwargs):
        for event in events:
            yield event
        mock_state.values.update(final_values)

    async def update_state(_config, delta, **_kwargs):
        mock_state.values.update(delta)

    mock_context.graph.astream = mock_astream
    mock_context.graph.aupdate_state.side_effect = update_state
    mock_context.graph.aget_state.return_value = mock_state


def fail_status_write(mock_context, status, error):
    async def update_status(email_id, next_status, **kwargs):
        if next_status == status:
            raise error

    mock_context.db_manager.update_status.side_effect = update_status

@pytest.mark.asyncio
async def test_process_flow_new_email(mock_context):
    """Test full processing of a new email that requires reply."""
    
    # Setup
    email_data = {"id": "msg_1", "subject": "Test", "body": "Content"}
    
    # 1. db_manager.log_initial_email returns a typed creation result
    mock_context.db_manager.log_initial_email.return_value = InitialEmailWriteResult.CREATED
    
    # 2. graph.astream yields events
    # We simulate the graph analyzing and drafting
    mock_state = MagicMock()
    mock_state.values = {}
    final_values = {
        "classification": {"need_reply": True},
        "draft_id": "msg_1",
        "context_summaries": [],
        "routing_log": [],
        "active_skills": [],
        "draft_to": [],
        "draft_cc": [],
    }

    async def mock_astream(*args, **kwargs):
        yield {"categorizer": {"classification": {"need_reply": True}}}
        yield {"drafter": {"draft_id": "msg_1"}}
        mock_state.values.update(final_values)

    async def update_state(_config, delta, **_kwargs):
        mock_state.values.update(delta)
    
    mock_context.graph.astream = mock_astream
    mock_context.graph.aupdate_state.side_effect = update_state
    
    # 3. graph.aget_state returns final state
    mock_context.graph.aget_state.return_value = mock_state
    mock_context.content_store.load_email.return_value = email_data
    mock_context.db_manager.load_draft.return_value = "Reply Draft"
    
    # 4. exchange_client.mark_as_read returns True
    mock_context.exchange_client.mark_as_read.return_value = True
    
    # Mock lark_app.send_approval_card globally
    with patch("src.exchange_service.lark_app.send_approval_card") as mock_lark_send, patch(
        "src.exchange_service.lark_app.generate_and_upload_pdf",
        new=AsyncMock(return_value=None),
    ):
        result = await process_and_archive_email(email_data, mock_context)

        assert result is ProcessingOutcome.PROCESSED
        
        # Verify steps
        # Ingestion
        mock_context.email_processor.process_email.assert_called_with(email_data)
        mock_context.db_manager.update_status.assert_any_call(
            "msg_1", "ingested", error_message=None
        )
        
        # Analysis updates
        mock_context.db_manager.update_status.assert_any_call("msg_1", "analyzed", classification=ANY)
        mock_context.db_manager.update_status.assert_any_call("msg_1", "drafted")
        
        # Lark Card
        mock_lark_send.assert_called_once()
        mock_context.db_manager.update_status.assert_any_call("msg_1", "waiting_approval")
        
        # Mark as Read
        mock_context.exchange_client.mark_as_read.assert_called_with("msg_1", is_read=True)

@pytest.mark.asyncio
async def test_process_flow_skip_analysis(mock_context):
    """Test skipping AI analysis."""
    email_data = {"id": "msg_2", "subject": "Archive Me"}
    mock_context.db_manager.log_initial_email.return_value = InitialEmailWriteResult.CREATED
    
    result = await process_and_archive_email(email_data, mock_context, skip_analysis=True)

    assert result is ProcessingOutcome.ARCHIVED
    
    # Should ingest but skip graph
    mock_context.email_processor.process_email.assert_called()
    mock_context.graph.astream.assert_not_called()
    
    # Mark as archived
    mock_context.db_manager.update_status.assert_any_call("msg_2", "archived")
    # Archive-only path should not mark sent/archive items as read
    mock_context.exchange_client.mark_as_read.assert_not_called()

@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status",
    ["waiting_approval", "notified_readonly", "skipped", "sent"],
)
async def test_process_flow_duplicate_email_with_safe_status_marks_read(
    mock_context, status
):
    """Test processing an already logged email."""
    email_data = {"id": "msg_dup"}
    mock_context.db_manager.log_initial_email.return_value = InitialEmailWriteResult.DUPLICATE
    mock_context.db_manager.get_email_status.return_value = status
    
    result = await process_and_archive_email(email_data, mock_context)

    assert result is ProcessingOutcome.DUPLICATE
    
    # Should skip almost everything except maybe mark as read? 
    # Logic: if not is_new: log "already exists".
    # Then it jumps to "Mark as processed (Read) on Server".
    
    mock_context.email_processor.process_email.assert_not_called()
    mock_context.graph.astream.assert_not_called()
    mock_context.exchange_client.mark_as_read.assert_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [None, "pending", "delivery_failed", "error"])
async def test_process_flow_duplicate_email_with_unsafe_status_stays_unread(
    mock_context, status
):
    email_data = {"id": "msg_dup_unsafe"}
    mock_context.db_manager.log_initial_email.return_value = InitialEmailWriteResult.DUPLICATE
    mock_context.db_manager.get_email_status.return_value = status

    result = await process_and_archive_email(email_data, mock_context)

    assert result is ProcessingOutcome.DUPLICATE
    mock_context.exchange_client.mark_as_read.assert_not_awaited()


@pytest.mark.asyncio
async def test_archive_duplicate_never_marks_read_even_with_safe_status(mock_context):
    email_data = {"id": "msg_archive_dup"}
    mock_context.db_manager.log_initial_email.return_value = InitialEmailWriteResult.DUPLICATE
    mock_context.db_manager.get_email_status.return_value = "sent"

    result = await process_and_archive_email(
        email_data, mock_context, skip_analysis=True
    )

    assert result is ProcessingOutcome.DUPLICATE
    mock_context.exchange_client.mark_as_read.assert_not_awaited()


@pytest.mark.asyncio
async def test_database_failure_never_marks_read(mock_context):
    mock_context.db_manager.log_initial_email.side_effect = DatabaseOperationError(
        operation="log_initial_email",
        retryable=True,
        message="database unavailable",
    )

    with pytest.raises(DatabaseOperationError):
        await process_and_archive_email({"id": "mail-3"}, mock_context)

    mock_context.exchange_client.mark_as_read.assert_not_awaited()


@pytest.mark.asyncio
async def test_ingested_status_write_failure_propagates_and_never_marks_read(
    mock_context,
):
    email_data = {"id": "mail-ingested-db-failure"}
    classification = {"need_reply": False, "intent": "垃圾邮件"}
    mock_context.db_manager.log_initial_email.return_value = InitialEmailWriteResult.CREATED
    configure_completed_graph(
        mock_context,
        email_data,
        classification=classification,
    )
    failure = DatabaseOperationError(
        operation="update_status",
        retryable=True,
        message="ingested status write failed",
    )
    fail_status_write(mock_context, "ingested", failure)

    outcome = None
    with pytest.raises(DatabaseOperationError) as caught:
        outcome = await process_and_archive_email(email_data, mock_context)

    assert caught.value is failure
    assert outcome is not ProcessingOutcome.PROCESSED
    mock_context.exchange_client.mark_as_read.assert_not_awaited()


@pytest.mark.asyncio
async def test_analyzed_status_write_failure_propagates_and_never_marks_read(
    mock_context,
):
    email_data = {"id": "mail-analyzed-db-failure"}
    classification = {"need_reply": False, "intent": "垃圾邮件"}
    mock_context.db_manager.log_initial_email.return_value = InitialEmailWriteResult.CREATED
    configure_completed_graph(
        mock_context,
        email_data,
        classification=classification,
        events=({"categorizer": {"classification": classification}},),
    )
    failure = DatabaseOperationError(
        operation="update_status",
        retryable=True,
        message="analyzed status write failed",
    )
    fail_status_write(mock_context, "analyzed", failure)

    outcome = None
    with pytest.raises(DatabaseOperationError) as caught:
        outcome = await process_and_archive_email(email_data, mock_context)

    assert caught.value is failure
    assert outcome is not ProcessingOutcome.PROCESSED
    mock_context.exchange_client.mark_as_read.assert_not_awaited()


@pytest.mark.asyncio
async def test_notification_status_write_failure_propagates_and_never_marks_read(
    mock_context,
):
    email_data = {"id": "mail-notification-db-failure"}
    classification = {"need_reply": True}
    mock_context.db_manager.log_initial_email.return_value = InitialEmailWriteResult.CREATED
    configure_completed_graph(
        mock_context,
        email_data,
        classification=classification,
    )
    failure = DatabaseOperationError(
        operation="update_status",
        retryable=True,
        message="notification status write failed",
    )
    fail_status_write(mock_context, "waiting_approval", failure)

    outcome = None
    with (
        patch(
            "src.exchange_service.lark_app.generate_and_upload_pdf",
            new=AsyncMock(return_value=None),
        ),
        patch("src.exchange_service.lark_app.send_approval_card", return_value=True),
        pytest.raises(DatabaseOperationError) as caught,
    ):
        outcome = await process_and_archive_email(email_data, mock_context)

    assert caught.value is failure
    assert outcome is not ProcessingOutcome.PROCESSED
    mock_context.exchange_client.mark_as_read.assert_not_awaited()
