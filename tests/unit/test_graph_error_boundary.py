import pytest

from src.domain.errors import DatabaseOperationError
from src.graph.builder import _guard_graph_node


@pytest.mark.asyncio
async def test_graph_guard_preserves_database_operation_error_identity():
    database_error = DatabaseOperationError(
        operation="load_draft",
        retryable=True,
        message="bounded database failure",
    )

    async def failing_node(state):
        raise database_error

    guarded = _guard_graph_node("drafter", failing_node)

    with pytest.raises(DatabaseOperationError) as caught:
        await guarded({})

    assert caught.value is database_error


@pytest.mark.asyncio
async def test_graph_guard_with_config_preserves_database_error_identity():
    database_error = DatabaseOperationError(
        operation="compare_and_set_status",
        retryable=True,
        message="bounded database failure",
    )

    async def failing_sender(state, config=None):
        raise database_error

    guarded = _guard_graph_node("sender", failing_sender, pass_config=True)

    with pytest.raises(DatabaseOperationError) as caught:
        await guarded({}, config={"configurable": {"thread_id": "mail"}})

    assert caught.value is database_error
