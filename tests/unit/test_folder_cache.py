import httpx
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


MOCK_FOLDERS_RESPONSE = {
    "code": 200,
    "msg": "success",
    "data": {
        "folders": [
            {
                "id": "ROOT_ID",
                "name": "Top of Information Store",
                "parent_id": None,
                "folder_class": "IPF.Note",
                "total_count": 0,
                "unread_count": 0,
                "child_folder_count": 5,
            },
            {
                "id": "INBOX_ID",
                "name": "收件箱",
                "parent_id": "ROOT_ID",
                "folder_class": "IPF.Note",
                "total_count": 1502,
                "unread_count": 5,
                "child_folder_count": 2,
            },
            {
                "id": "VIP_ID",
                "name": "VIP邮件",
                "parent_id": "INBOX_ID",
                "folder_class": "IPF.Note",
                "total_count": 30,
                "unread_count": 1,
                "child_folder_count": 0,
            },
            {
                "id": "DAILY_ID",
                "name": "项目A日报",
                "parent_id": "INBOX_ID",
                "folder_class": "IPF.Note",
                "total_count": 20,
                "unread_count": 0,
                "child_folder_count": 0,
            },
            {
                "id": "SENT_ID",
                "name": "已发送邮件",
                "parent_id": "ROOT_ID",
                "folder_class": "IPF.Note",
                "total_count": 890,
                "unread_count": 0,
                "child_folder_count": 0,
            },
            {
                "id": "DRAFTS_ID",
                "name": "草稿",
                "parent_id": "ROOT_ID",
                "folder_class": "IPF.Note",
                "total_count": 10,
                "unread_count": 0,
                "child_folder_count": 0,
            },
            {
                "id": "CAL_ID",
                "name": "日历",
                "parent_id": "ROOT_ID",
                "folder_class": "IPF.Appointment",
                "total_count": 50,
                "unread_count": 0,
                "child_folder_count": 0,
            },
        ]
    },
}


def _make_client():
    mock_settings = MagicMock()
    mock_settings.EXCHANGE_API_URL = "http://mock/api/v1/exchange/emails"
    mock_settings.EXCHANGE_API_KEY = "test-key"
    mock_settings.EXCHANGE_ACCOUNT_ID = 1
    mock_settings.EXCHANGE_SSL_VERIFY = False
    mock_settings.EXCHANGE_FOLDER_SENTITEMS = "已发送邮件"
    mock_settings.EXCHANGE_FOLDER_DRAFTS = "草稿"

    from src.utils.exchange_api import ExchangeClient

    return ExchangeClient(settings=mock_settings)


def _folder(folder_id: str, name: str) -> dict:
    return {
        "id": folder_id,
        "name": name,
        "parent_id": None,
        "folder_class": "IPF.Note",
    }


def _mock_httpx(response_data):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = response_data

    mock_instance = AsyncMock()
    mock_instance.get = AsyncMock(return_value=mock_response)
    mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
    mock_instance.__aexit__ = AsyncMock(return_value=False)

    return patch("httpx.AsyncClient", return_value=mock_instance), mock_instance


@pytest.mark.asyncio
async def test_get_all_folders_returns_id_name_mapping():
    client = _make_client()
    patcher, _ = _mock_httpx(MOCK_FOLDERS_RESPONSE)

    with patcher:
        result = await client.get_all_folders()

    assert isinstance(result, dict)
    assert result["INBOX_ID"] == "收件箱"
    assert result["SENT_ID"] == "已发送邮件"
    assert result["VIP_ID"] == "VIP邮件"


@pytest.mark.asyncio
async def test_get_all_folders_caches_result():
    client = _make_client()
    patcher, mock_inst = _mock_httpx(MOCK_FOLDERS_RESPONSE)

    with patcher:
        r1 = await client.get_all_folders()
        r2 = await client.get_all_folders()

    assert r1 is r2
    assert mock_inst.get.call_count == 1


@pytest.mark.asyncio
async def test_sentitems_and_drafts_identified_by_name():
    client = _make_client()
    patcher, _ = _mock_httpx(MOCK_FOLDERS_RESPONSE)

    with patcher:
        await client.get_all_folders()

    assert client.sentitems_folder_id == "SENT_ID"
    assert client.drafts_folder_id == "DRAFTS_ID"


@pytest.mark.asyncio
async def test_system_folders_identified_when_config_uses_english_aliases():
    client = _make_client()
    client._sentitems_name = "Sent Items"
    client._drafts_name = "Drafts"
    patcher, _ = _mock_httpx(MOCK_FOLDERS_RESPONSE)

    with patcher:
        await client.get_all_folders()

    assert client.sentitems_folder_id == "SENT_ID"
    assert client.drafts_folder_id == "DRAFTS_ID"


def test_system_folders_identified_by_english_names():
    client = _make_client()

    client._build_folder_cache(
        [
            _folder("SENT_EN", "Sent Items"),
            _folder("DRAFTS_EN", "Drafts"),
        ]
    )

    assert client.sentitems_folder_id == "SENT_EN"
    assert client.drafts_folder_id == "DRAFTS_EN"


def test_configured_names_disambiguate_alias_candidates():
    client = _make_client()
    client._sentitems_name = "Corporate Sent"
    client._drafts_name = "Work In Progress"

    client._build_folder_cache(
        [
            _folder("SENT_ALIAS", "Sent Items"),
            _folder("SENT_CONFIGURED", "corporate-sent"),
            _folder("DRAFTS_ALIAS", "Drafts"),
            _folder("DRAFTS_CONFIGURED", "work_in_progress"),
        ]
    )

    assert client.sentitems_folder_id == "SENT_CONFIGURED"
    assert client.drafts_folder_id == "DRAFTS_CONFIGURED"


@pytest.mark.parametrize("folders", [pytest.param("forward"), pytest.param("reverse")])
def test_duplicate_configured_name_fails_closed_regardless_of_order(folders):
    client = _make_client()
    client._sentitems_name = "Corporate Sent"
    candidates = [
        _folder("SENT_CONFIGURED_1", "Corporate Sent"),
        _folder("SENT_CONFIGURED_2", "corporate-sent"),
        _folder("SENT_ALIAS", "Sent Items"),
        _folder("DRAFTS", "Drafts"),
    ]
    if folders == "reverse":
        candidates.reverse()

    client._build_folder_cache(candidates)

    assert client.sentitems_folder_id is None
    assert client.drafts_folder_id == "DRAFTS"


@pytest.mark.parametrize("folders", [pytest.param("forward"), pytest.param("reverse")])
def test_multiple_aliases_fail_closed_regardless_of_order(folders):
    client = _make_client()
    client._sentitems_name = "Configured Name Not Present"
    candidates = [
        _folder("SENT_ITEMS", "Sent Items"),
        _folder("SENT_SHORT", "Sent"),
        _folder("DRAFTS", "Drafts"),
    ]
    if folders == "reverse":
        candidates.reverse()

    client._build_folder_cache(candidates)

    assert client.sentitems_folder_id is None
    assert client.drafts_folder_id == "DRAFTS"


def test_same_folder_cannot_be_both_sentitems_and_drafts():
    client = _make_client()
    client._sentitems_name = "Shared System Folder"
    client._drafts_name = "shared-system-folder"

    client._build_folder_cache([_folder("SHARED", "Shared_System Folder")])

    assert client.sentitems_folder_id is None
    assert client.drafts_folder_id is None


@pytest.mark.parametrize("folders", [pytest.param("forward"), pytest.param("reverse")])
def test_cross_kind_candidate_fails_closed_even_when_other_kind_is_ambiguous(folders):
    client = _make_client()
    client._sentitems_name = "Drafts"
    client._drafts_name = "Configured Drafts Not Present"
    candidates = [
        _folder("SHARED", "Drafts"),
        _folder("SECOND_DRAFT", "Draft"),
    ]
    if folders == "reverse":
        candidates.reverse()

    client._build_folder_cache(candidates)

    assert client.sentitems_folder_id is None
    assert client.drafts_folder_id is None


@pytest.mark.asyncio
async def test_compute_folder_policies_recursive_inheritance():
    client = _make_client()
    patcher, _ = _mock_httpx(MOCK_FOLDERS_RESPONSE)

    with patcher:
        await client.get_all_folders()

    folders_full = {"收件箱"}
    folders_archive = {"项目A日报"}
    policies = client.compute_folder_policies(folders_full, folders_archive)

    assert policies["INBOX_ID"] == "full"
    assert policies["VIP_ID"] == "full"
    assert policies["DAILY_ID"] == "archive"
    assert policies.get("SENT_ID") == "ignore"
    assert policies.get("CAL_ID") == "ignore"


@pytest.mark.asyncio
async def test_get_folder_policy_after_init():
    client = _make_client()
    patcher, _ = _mock_httpx(MOCK_FOLDERS_RESPONSE)

    with patcher:
        await client.get_all_folders()

    client.init_folder_policies(
        folders_full={"收件箱"},
        folders_archive={"项目A日报"},
    )

    assert client.get_folder_policy("INBOX_ID") == "full"
    assert client.get_folder_policy("VIP_ID") == "full"
    assert client.get_folder_policy("DAILY_ID") == "archive"
    assert client.get_folder_policy("UNKNOWN_ID") == "ignore"


@pytest.mark.asyncio
async def test_get_all_folders_fallback_endpoint_on_404():
    """If /emails/folders/all returns 404, client should fallback to /folders/all."""
    client = _make_client()

    mock_404 = MagicMock()
    mock_404.status_code = 404
    mock_404.text = "not found"

    mock_200 = MagicMock()
    mock_200.status_code = 200
    mock_200.json.return_value = MOCK_FOLDERS_RESPONSE

    mock_instance = AsyncMock()
    mock_instance.get = AsyncMock(side_effect=[mock_404, mock_200])
    mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
    mock_instance.__aexit__ = AsyncMock(return_value=False)

    with patch("httpx.AsyncClient", return_value=mock_instance):
        result = await client.get_all_folders()

    assert result["INBOX_ID"] == "收件箱"
    called_urls = [call.args[0] for call in mock_instance.get.call_args_list]
    assert called_urls[0].endswith("/emails/folders/all")
    assert called_urls[1].endswith("/folders/all")


@pytest.mark.asyncio
async def test_get_all_folders_fallback_endpoint_after_transport_failure():
    client = _make_client()

    mock_200 = MagicMock()
    mock_200.status_code = 200
    mock_200.json.return_value = MOCK_FOLDERS_RESPONSE

    mock_instance = AsyncMock()
    mock_instance.get = AsyncMock(
        side_effect=[httpx.ReadTimeout("first candidate timed out"), mock_200]
    )

    with patch("httpx.AsyncClient", return_value=mock_instance):
        result = await client.get_all_folders()

    assert result["INBOX_ID"] == "收件箱"
    called_urls = [call.args[0] for call in mock_instance.get.call_args_list]
    assert called_urls[0].endswith("/emails/folders/all")
    assert called_urls[1].endswith("/folders/all")
