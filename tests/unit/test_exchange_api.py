import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from src.utils.exchange_api import ExchangeClient

@pytest.mark.asyncio
async def test_get_recent_emails_empty(mock_settings):
    """Test getting recent emails when the list is empty."""
    client = ExchangeClient(settings=mock_settings)
    
    with patch("httpx.AsyncClient") as mock_http_client_cls:
        mock_http_client = AsyncMock()
        mock_http_client_cls.return_value.__aenter__.return_value = mock_http_client
        
        # Mock list response
        mock_response_list = MagicMock()
        mock_response_list.status_code = 200
        mock_response_list.json.return_value = {"code": 200, "data": {"items": []}, "message": "OK"}
        
        mock_http_client.get.return_value = mock_response_list
        
        emails = await client.get_recent_emails()
        assert emails == []
        mock_http_client.get.assert_called_once()


@pytest.mark.asyncio
async def test_get_recent_emails_with_items(mock_settings):
    """Test getting recent emails with items, verifying detail fetching behavior."""
    client = ExchangeClient(settings=mock_settings)
    
    with patch("httpx.AsyncClient") as mock_http_client_cls:
        mock_http_client = AsyncMock()
        mock_http_client_cls.return_value.__aenter__.return_value = mock_http_client
        
        # Scenario: 
        # Item 1: No body in list, needs detail fetch.
        # Item 2: Has body in list, skips detail fetch.
        
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
        
        mock_detail_resp = MagicMock()
        mock_detail_resp.status_code = 200
        mock_detail_resp.json.return_value = {"code": 200, "data": {"id": "1", "subject": "Test 1", "body": "Fetched content"}}
        
        # Sequence of return values for .get calls
        # 1. List call
        # 2. Detail call for ID 1
        mock_http_client.get.side_effect = [mock_list_resp, mock_detail_resp]
        
        emails = await client.get_recent_emails()
        
        assert len(emails) == 2
        assert emails[0]["body"] == "Fetched content" # From detail fetch
        assert emails[1]["body"] == "Load content"    # From list directly
        
        assert mock_http_client.get.call_count == 2


@pytest.mark.asyncio
async def test_send_email_success(mock_settings):
    """Test sending an email successfully."""
    client = ExchangeClient(settings=mock_settings)
    
    with patch("httpx.AsyncClient") as mock_http_client_cls:
        mock_http_client = AsyncMock()
        mock_http_client_cls.return_value.__aenter__.return_value = mock_http_client
        
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_http_client.post.return_value = mock_resp
        
        result = await client.send_email("test@example.com", "Subj", "Body")
        assert result is True
        
        mock_http_client.post.assert_called_once()
        args, kwargs = mock_http_client.post.call_args
        assert kwargs["json"]["to"] == ["test@example.com"]
        assert kwargs["json"]["subject"] == "Subj"


@pytest.mark.asyncio
async def test_create_draft_success(mock_settings):
    """Test creating a draft successfully."""
    client = ExchangeClient(settings=mock_settings)
    
    with patch("httpx.AsyncClient") as mock_http_client_cls:
        mock_http_client = AsyncMock()
        mock_http_client_cls.return_value.__aenter__.return_value = mock_http_client
        
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_http_client.post.return_value = mock_resp
        
        result = await client.create_draft("test@example.com", "Draft Subj", "Draft Body")
        assert result is True
        
        endpoint = mock_http_client.post.call_args[0][0]
        assert "/drafts" in endpoint

@pytest.mark.asyncio
async def test_sync_emails(mock_settings):
    """Test incremental sync."""
    client = ExchangeClient(settings=mock_settings)
    
    with patch("httpx.AsyncClient") as mock_http_client_cls:
        mock_http_client = AsyncMock()
        mock_http_client_cls.return_value.__aenter__.return_value = mock_http_client
        
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "code": 200,
            "data": {
                "sync_state": "new_state",
                "items": [{"change_type": "create", "id": "123"}]
            }
        }
        mock_http_client.post.return_value = mock_resp
        
        result = await client.sync_emails(sync_state="old_state")
        assert result["sync_state"] == "new_state"
        assert len(result["items"]) == 1
