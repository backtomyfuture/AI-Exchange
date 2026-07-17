from __future__ import annotations

import asyncio
from weakref import WeakValueDictionary


_graph_resource_locks: WeakValueDictionary[str, asyncio.Lock] = (
    WeakValueDictionary()
)


def get_graph_resource_lock(email_id: str) -> asyncio.Lock:
    """Return the single in-process lock for one email's Graph resources."""
    if not isinstance(email_id, str) or not email_id:
        raise ValueError("invalid_graph_resource_lock_id")
    lock = _graph_resource_locks.get(email_id)
    if lock is None:
        lock = asyncio.Lock()
        _graph_resource_locks[email_id] = lock
    return lock
