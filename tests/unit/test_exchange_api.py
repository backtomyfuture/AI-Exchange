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
        # Current implementation always fetches detail for each item.
        # Item 1: detail overrides body.
        # Item 2: detail is fetched as well.
        
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
        
        # Sequence of return values for .get calls:
        # 1) list, 2) detail(id=1), 3) detail(id=2)
        mock_http_client.get.side_effect = [mock_list_resp, mock_detail_resp_1, mock_detail_resp_2]
        
        emails = await client.get_recent_emails()
        
        assert len(emails) == 2
        assert emails[0]["body"] == "Fetched content" # From detail fetch
        assert emails[1]["body"] == "Load content"    # From detail fetch
        
        assert mock_http_client.get.call_count == 3


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

def test_sync_api_removed(mock_settings):
    """sync_emails has been removed after webhook migration."""
    client = ExchangeClient(settings=mock_settings)
    assert not hasattr(client, "sync_emails")
