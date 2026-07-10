import pytest
from unittest.mock import AsyncMock, MagicMock, patch, ANY
from src.domain.email_state import InitialEmailWriteResult, ProcessingOutcome
from src.domain.errors import DatabaseOperationError
from src.exchange_service import process_and_archive_email

@pytest.fixture
def mock_context():
    ctx = MagicMock()
    ctx.db_manager = AsyncMock()
    ctx.email_processor = MagicMock()
    ctx.graph = AsyncMock()
    ctx.exchange_client = AsyncMock()
    return ctx

@pytest.mark.asyncio
async def test_process_flow_new_email(mock_context):
    """Test full processing of a new email that requires reply."""
    
    # Setup
    email_data = {"id": "msg_1", "subject": "Test", "body": "Content"}
    
    # 1. db_manager.log_initial_email returns a typed creation result
    mock_context.db_manager.log_initial_email.return_value = InitialEmailWriteResult.CREATED
    
    # 2. graph.astream yields events
    # We simulate the graph analyzing and drafting
    async def mock_astream(*args, **kwargs):
        yield {"categorizer": {"classification": {"need_reply": True}}}
        yield {"drafter": {"draft": "Reply Draft"}}
    
    mock_context.graph.astream = mock_astream
    
    # 3. graph.aget_state returns final state
    mock_state = MagicMock()
    mock_state.values = {
        "classification": {"need_reply": True},
        "draft": "Reply Draft",
        "context": [],
        "email": email_data
    }
    mock_context.graph.aget_state.return_value = mock_state
    
    # 4. exchange_client.mark_as_read returns True
    mock_context.exchange_client.mark_as_read.return_value = True
    
    # Mock lark_app.send_approval_card globally
    with patch("src.exchange_service.lark_app.send_approval_card") as mock_lark_send:
        result = await process_and_archive_email(email_data, mock_context)

        assert result is ProcessingOutcome.PROCESSED
        
        # Verify steps
        # Ingestion
        mock_context.email_processor.process_email.assert_called_with(email_data)
        mock_context.db_manager.update_status.assert_any_call("msg_1", "ingested")
        
        # Analysis updates
        mock_context.db_manager.update_status.assert_any_call("msg_1", "analyzed", classification=ANY)
        mock_context.db_manager.update_status.assert_any_call("msg_1", "drafted", draft_content="Reply Draft")
        
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
