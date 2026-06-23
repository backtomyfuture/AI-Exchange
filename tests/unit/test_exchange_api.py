import pytest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock
from src.utils.exchange_api import ExchangeClient

@pytest.mark.asyncio
async def test_get_recent_emails_empty(mock_settings):
    """Test getting recent emails when the list is empty."""
    client = ExchangeClient(settings=mock_settings)
    
    mock_http = AsyncMock()
    mock_response_list = MagicMock()
    mock_response_list.status_code = 200
    mock_response_list.json.return_value = {"code": 200, "data": {"items": []}, "message": "OK"}
    mock_http.get.return_value = mock_response_list

    with patch.object(type(client), "http_client", new_callable=PropertyMock, return_value=mock_http):
        emails = await client.get_recent_emails()
        assert emails == []
        mock_http.get.assert_called_once()


@pytest.mark.asyncio
async def test_get_recent_emails_with_items(mock_settings):
    """Test getting recent emails with items, verifying detail fetching behavior."""
    client = ExchangeClient(settings=mock_settings)
    
    mock_http = AsyncMock()

    mock_list_resp = MagicMock()
    mock_list_resp.status_code = 200
    mock_list_resp.json.return_value = {
        "code": 200, 
        "data": {
            "items": [
                {"id": "1", "subject": "Test 1"},
                {"id": "2", "subject": "Test 2", "body": "Load content"}
            ]
        }
    }
    
    mock_detail_resp_1 = MagicMock()
    mock_detail_resp_1.status_code = 200
    mock_detail_resp_1.json.return_value = {"code": 200, "data": {"id": "1", "subject": "Test 1", "body": "Fetched content"}}

    mock_detail_resp_2 = MagicMock()
    mock_detail_resp_2.status_code = 200
    mock_detail_resp_2.json.return_value = {"code": 200, "data": {"id": "2", "subject": "Test 2", "body": "Load content"}}
    
    mock_http.get.side_effect = [mock_list_resp, mock_detail_resp_1, mock_detail_resp_2]

    with patch.object(type(client), "http_client", new_callable=PropertyMock, return_value=mock_http):
        emails = await client.get_recent_emails()
        
        assert len(emails) == 2
        assert emails[0]["body"] == "Fetched content"
        assert emails[1]["body"] == "Load content"
        
        assert mock_http.get.call_count == 3


@pytest.mark.asyncio
async def test_send_email_success(mock_settings):
    """Test sending an email successfully."""
    client = ExchangeClient(settings=mock_settings)
    
    mock_http = AsyncMock()
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_http.post.return_value = mock_resp

    with patch.object(type(client), "http_client", new_callable=PropertyMock, return_value=mock_http):
        result = await client.send_email("test@example.com", "Subj", "Body")
        assert result is True
        
        mock_http.post.assert_called_once()
        args, kwargs = mock_http.post.call_args
        assert kwargs["json"]["to"] == ["test@example.com"]
        assert kwargs["json"]["subject"] == "Subj"


@pytest.mark.asyncio
async def test_create_draft_success(mock_settings):
    """Test creating a draft successfully."""
    client = ExchangeClient(settings=mock_settings)
    
    mock_http = AsyncMock()
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_http.post.return_value = mock_resp

    with patch.object(type(client), "http_client", new_callable=PropertyMock, return_value=mock_http):
        result = await client.create_draft("test@example.com", "Draft Subj", "Draft Body")
        assert result is True
        
        endpoint = mock_http.post.call_args[0][0]
        assert "/drafts" in endpoint

def test_sync_api_removed(mock_settings):
    """sync_emails has been removed after webhook migration."""
    client = ExchangeClient(settings=mock_settings)
    assert not hasattr(client, "sync_emails")


@pytest.mark.asyncio
async def test_close_closes_shared_async_client(mock_settings):
    """Closing the ExchangeClient should close its shared httpx.AsyncClient."""
    client = ExchangeClient(settings=mock_settings)
    http_client = client.http_client

    try:
        await client.close()
    finally:
        if not http_client.is_closed:
            await http_client.aclose()

    assert http_client.is_closed
    assert client._http_client is None
