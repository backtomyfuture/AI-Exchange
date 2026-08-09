import ast
import inspect
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from src.email_feishu_delivery import EmailFeishuDelivery
from src.exchange_service import process_and_archive_email, process_and_archive_email_guarded
from src.ingestion.processing import ProcessingEffectScope, ProcessingPolicyRejected


def _scope(email_id: str, *, account_id: int = 8) -> ProcessingEffectScope:
    return ProcessingEffectScope(
        account_id=account_id,
        inbox_id=str(uuid4()),
        generation=1,
        fencing_token=1,
        attempts=0,
        email_id=str(uuid4()),
        expected_email_version=1,
        event_dedupe_key="a" * 64,
        external_email_id=email_id,
    )


def _function_source(path: str, function_name: str) -> str:
    source = Path(path).read_text(encoding="utf-8")
    tree = ast.parse(source)
    lines = source.splitlines()
    for node in tree.body:
        if isinstance(node, ast.AsyncFunctionDef) and node.name == function_name:
            return "\n".join(lines[node.lineno - 1 : node.end_lineno])
    raise AssertionError(f"Function {function_name} not found")


def test_exchange_entry_stays_small_and_does_not_import_lark_app():
    source = _function_source("src/exchange_service.py", "process_and_archive_email")

    assert len(source.strip().splitlines()) < 60
    assert "lark_app" not in Path("src/exchange_service.py").read_text(encoding="utf-8")


def test_delivery_exposes_one_typed_public_operation():
    signature = inspect.signature(EmailFeishuDelivery.deliver)

    assert list(signature.parameters) == ["self", "request", "effect_boundary"]
    assert signature.parameters["effect_boundary"].default is inspect.Parameter.empty


@pytest.mark.asyncio
async def test_guarded_entry_rejects_cross_account_scope_before_any_io():
    ctx = SimpleNamespace(
        db_manager=SimpleNamespace(log_initial_email=AsyncMock()),
        content_store=SimpleNamespace(put_email=AsyncMock()),
    )
    before = AsyncMock(return_value=None)

    with patch(
        "src.exchange_service.get_settings",
        return_value=SimpleNamespace(EXCHANGE_ACCOUNT_ID=8),
    ), pytest.raises(ProcessingPolicyRejected):
        await process_and_archive_email_guarded(
            {"id": "cross-account", "subject": "s", "sender": "a@example.test"},
            ctx,
            before_external_effect=before,
            effect_scope=_scope("cross-account", account_id=9),
        )

    before.assert_not_awaited()
    ctx.db_manager.log_initial_email.assert_not_awaited()
    ctx.content_store.put_email.assert_not_awaited()


@pytest.mark.asyncio
async def test_unguarded_entry_still_delegates_to_the_email_orchestration():
    ctx = MagicMock()
    ctx.db_manager.log_initial_email = AsyncMock()

    with patch(
        "src.exchange_service._process_email_entry",
        new=AsyncMock(return_value="processed"),
    ) as entry:
        outcome = await process_and_archive_email({"id": "mail-1"}, ctx)

    assert outcome == "processed"
    entry.assert_awaited_once()
