"""
Lark file operations - extracted from lark_app.py for modularity.
Handles: upload_file_to_drive, delete_file_from_drive
"""
import io
import logging
from typing import Optional

from src.config import get_settings

logger = logging.getLogger(__name__)


def upload_file_to_drive(name: str, content: bytes, size: int,
                         *, lark_api_client=None) -> Optional[dict]:
    if lark_api_client is None:
        from src.utils.lark_app import lark_api_client as _client
        lark_api_client = _client

    if not lark_api_client:
        logger.warning("Lark Client not initialized.")
        return None

    settings = get_settings()
    folder_token = settings.LARK_DRIVE_FOLDER_TOKEN
    if not folder_token:
        logger.warning("Lark Drive Folder Token not configured. Skipping upload.")
        return None

    try:
        from lark_oapi.api.drive.v1 import UploadAllFileRequest, UploadAllFileRequestBody

        request = UploadAllFileRequest.builder() \
            .request_body(UploadAllFileRequestBody.builder()
                .file_name(name)
                .parent_type("explorer")
                .parent_node(folder_token)
                .size(size)
                .file(io.BytesIO(content))
                .build()) \
            .build()

        response = lark_api_client.drive.v1.file.upload_all(request)
        if not response.success():
            logger.error(f"Failed to upload file {name}: {response.code} - {response.msg}")
            return None

        data = response.data
        file_token = data.file_token
        url = getattr(data, "url", "")
        if not url and file_token:
            url = f"https://www.feishu.cn/file/{file_token}"

        logger.info(f"File uploaded. Token: {file_token}, URL: {url}")
        return {"file_token": file_token, "url": url}
    except Exception as e:
        logger.error(f"Exception uploading file {name}: {e}")
        return None


def delete_file_from_drive(file_token: str, *, lark_api_client=None) -> bool:
    if lark_api_client is None:
        from src.utils.lark_app import lark_api_client as _client
        lark_api_client = _client

    if not lark_api_client or not file_token:
        return False

    try:
        from lark_oapi.api.drive.v1 import DeleteFileRequest

        request = DeleteFileRequest.builder() \
            .file_token(file_token) \
            .type("file") \
            .build()

        response = lark_api_client.drive.v1.file.delete(request)
        if not response.success():
            logger.warning(f"Failed to delete file {file_token}: {response.code} - {response.msg}")
            return False

        logger.info(f"File deleted (moved to trash): {file_token}")
        return True
    except Exception as e:
        logger.error(f"Exception deleting file {file_token}: {e}")
        return False
