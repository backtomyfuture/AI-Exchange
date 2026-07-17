from __future__ import annotations

import asyncio
from typing import Any
from weakref import WeakValueDictionary

from src.safety.manual_review import normalize_manual_review_code


MAX_ACTION_ID_BYTES = 512
_approval_action_locks: WeakValueDictionary[str, asyncio.Lock] = (
    WeakValueDictionary()
)


def _bounded_action_id(value: object, *, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value.encode("utf-8")) > MAX_ACTION_ID_BYTES
    ):
        raise ValueError(f"invalid_{field}")
    return value


def get_approval_action_lock(email_id: str) -> asyncio.Lock:
    bounded_id = _bounded_action_id(email_id, field="email_id")
    lock = _approval_action_locks.get(bounded_id)
    if lock is None:
        lock = asyncio.Lock()
        _approval_action_locks[bounded_id] = lock
    return lock


async def claim_approval(email_id: str, user_id: str, db_manager: Any) -> bool:
    bounded_id = _bounded_action_id(email_id, field="email_id")
    _bounded_action_id(user_id, field="user_id")
    return await db_manager.compare_and_set_status(
        bounded_id,
        expected=frozenset({"waiting_approval"}),
        target="approved",
    )


async def claim_rejection(email_id: str, user_id: str, db_manager: Any) -> bool:
    bounded_id = _bounded_action_id(email_id, field="email_id")
    _bounded_action_id(user_id, field="user_id")
    return await db_manager.compare_and_set_status(
        bounded_id,
        expected=frozenset({"waiting_approval"}),
        target="rejected",
    )


async def claim_send(email_id: str, db_manager: Any) -> bool:
    bounded_id = _bounded_action_id(email_id, field="email_id")
    return await db_manager.compare_and_set_status(
        bounded_id,
        expected=frozenset({"approved"}),
        target="sending",
    )


async def claim_draft_save(email_id: str, db_manager: Any) -> bool:
    bounded_id = _bounded_action_id(email_id, field="email_id")
    return await db_manager.compare_and_set_status(
        bounded_id,
        expected=frozenset({"waiting_approval"}),
        target="saving_draft",
    )


async def move_to_manual_review(
    email_id: str,
    db_manager: Any,
    *,
    expected: frozenset[str],
    code: object,
) -> bool:
    bounded_id = _bounded_action_id(email_id, field="email_id")
    return await db_manager.compare_and_set_manual_review(
        bounded_id,
        expected=expected,
        error_code=normalize_manual_review_code(code),
    )


async def mark_send_unknown(email_id: str, db_manager: Any, *, code: object) -> bool:
    """Persist a terminal quarantine for an already-started remote send."""
    bounded_id = _bounded_action_id(email_id, field="email_id")
    return await db_manager.compare_and_set_send_unknown(
        bounded_id,
        error_code=normalize_manual_review_code(code),
    )


async def complete_send(email_id: str, db_manager: Any) -> bool:
    bounded_id = _bounded_action_id(email_id, field="email_id")
    return await db_manager.compare_and_set_status(
        bounded_id,
        expected=frozenset({"sending"}),
        target="sent",
    )


async def complete_draft_save(email_id: str, db_manager: Any) -> bool:
    bounded_id = _bounded_action_id(email_id, field="email_id")
    return await db_manager.compare_and_set_status(
        bounded_id,
        expected=frozenset({"saving_draft"}),
        target="draft_saved",
    )
