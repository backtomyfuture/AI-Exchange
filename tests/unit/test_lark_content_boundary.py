from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.graph.dependencies import GraphDependencies
from src.graph.state_factory import build_initial_graph_state
from src.storage import ContentRef
from src.utils import lark_app


def _lark_settings(*, debug: bool) -> SimpleNamespace:
    return SimpleNamespace(
        DEBUG=debug,
        EXTERNAL_URL="https://example.test",
        LARK_ALLOWED_OPEN_IDS="user-1",
    )


class FakeContentStore:
    def __init__(self, email):
        self.email = deepcopy(email)
        self.loads = []

    async def load_email(self, ref, *, include_attachments=False):
        self.loads.append((ref, include_attachments))
        return deepcopy(self.email)


class FakeDraftStore:
    def __init__(self, values=None, *, status_getter=None):
        self.values = dict(values or {})
        self.saves = []
        self.loads = []
        self.status_getter = status_getter or (lambda: "waiting_approval")

    async def save_draft(self, email_id, content):
        self.saves.append((email_id, content))
        self.values[email_id] = content
        return email_id

    async def save_draft_if_status(self, email_id, content):
        if self.status_getter() != "waiting_approval":
            return False
        self.saves.append((email_id, content))
        self.values[email_id] = content
        return True

    async def load_draft(self, draft_id):
        self.loads.append(draft_id)
        return self.values[draft_id]


def _ref():
    return ContentRef(
        account_id=8,
        object_id="00000000-0000-4000-8000-000000000067",
        key_version="v1",
        sha256="6" * 64,
    )


def _slim_state():
    state = build_initial_graph_state(
        {
            "id": "mail-1",
            "subject": "small subject",
            "sender": "sender@example.com",
            "draft_to": ["sender@example.com"],
        },
        _ref(),
    )
    state["draft_id"] = "mail-1"
    state["classification"] = {"need_reply": True}
    state["context_summaries"] = [{"id": "old-1", "snippet": "small context"}]
    return SimpleNamespace(values=state)


def _event(
    action,
    *,
    email_id="mail-1",
    options=None,
    option=None,
    form_value=None,
):
    action_obj = SimpleNamespace(
        value={"action": action, "id": email_id},
        options=options,
        option=option,
        form_value=form_value or {},
    )
    return SimpleNamespace(
        event=SimpleNamespace(
            action=action_obj,
            operator=SimpleNamespace(open_id="user-1"),
            context=SimpleNamespace(open_message_id="message-1"),
        )
    )


@pytest.fixture
def production_lark_boundary(monkeypatch):
    monkeypatch.setattr(
        "src.graph.state_factory.get_settings",
        lambda: SimpleNamespace(EXCHANGE_ACCOUNT_ID=8),
    )
    monkeypatch.setattr(lark_app, "get_settings", lambda: _lark_settings(debug=False))
    state = _slim_state()
    graph = SimpleNamespace(
        aget_state=AsyncMock(return_value=state),
        aupdate_state=AsyncMock(),
        ainvoke=AsyncMock(),
    )

    async def update_state(_config, delta):
        state.values.update(delta)

    graph.aupdate_state.side_effect = update_state
    content_store = FakeContentStore(
        {
            "id": "mail-1",
            "subject": "complete subject",
            "sender": "sender@example.com",
            "body": "COMPLETE-BODY-SENTINEL",
            "attachments": [{"name": "a.txt"}],
        }
    )
    drafts = FakeDraftStore({"mail-1": "COMPLETE-DRAFT-SENTINEL"})
    dependencies = GraphDependencies(content_store=content_store, drafts=drafts)
    database = SimpleNamespace(status="waiting_approval")

    async def compare_and_set_status(_email_id, *, expected, target):
        if database.status not in expected:
            return False
        database.status = target
        return True

    async def get_email_status(_email_id):
        return database.status

    async def update_status(_email_id, status, **_kwargs):
        if status is not None:
            database.status = status

    database.compare_and_set_status = AsyncMock(
        side_effect=compare_and_set_status
    )
    database.get_email_status = AsyncMock(side_effect=get_email_status)
    database.update_status = AsyncMock(side_effect=update_status)
    drafts.status_getter = lambda: database.status
    monkeypatch.setattr(lark_app, "graph", graph)
    monkeypatch.setattr(lark_app, "graph_dependencies", dependencies)
    monkeypatch.setattr(lark_app, "db_manager", database)
    monkeypatch.setattr(lark_app, "card_builder", MagicMock())
    lark_app.card_builder.build_approval_card.return_value = {"card": "ok"}
    monkeypatch.setattr(lark_app, "worker_loop", None)
    return state, graph, dependencies


def _seed_test_card(email_id: str) -> SimpleNamespace:
    state = SimpleNamespace(
        values={
            "email": {
                "id": email_id,
                "subject": "Explicit test card",
                "sender": "sender@example.com",
                "to": ["recipient@example.com"],
                "cc": [],
                "draft_to": ["recipient@example.com"],
                "draft_cc": [],
                "body": "TEST-CARD-BODY",
                "attachments": [],
            },
            "draft": "TEST-CARD-DRAFT",
            "context": [],
            "classification": {"need_reply": True, "reasoning": "test"},
            "attachment_tokens": [],
            "pdf_token": None,
            "recipient_candidates": {
                "to": [
                    {
                        "open_id": "mock-user-1",
                        "search_text": "alice mock-user-1",
                    }
                ],
                "cc": [],
            },
        }
    )
    lark_app._mock_store[email_id] = state
    return state


def test_edit_card_hydrates_email_and_draft_without_graph_fallback(
    production_lark_boundary,
):
    _state, _graph, dependencies = production_lark_boundary

    result = lark_app.handle_card_action(_event("edit_draft"))

    call = lark_app.card_builder.build_approval_card.call_args
    assert call.args[1] == "COMPLETE-DRAFT-SENTINEL"
    assert call.args[2] == [{"id": "old-1", "snippet": "small context"}]
    assert call.args[3]["body"] == "COMPLETE-BODY-SENTINEL"
    assert dependencies.content_store.loads == [(_ref(), False)]
    assert result["toast"]["content"] == "编辑正文"


def test_test_push_card_is_resolved_before_graph_or_content_stores(
    production_lark_boundary,
    monkeypatch,
):
    _state, graph, dependencies = production_lark_boundary
    email_id = "test_push_graph_boundary"
    _seed_test_card(email_id)
    monkeypatch.setattr(
        lark_app,
        "get_settings",
        lambda: _lark_settings(debug=True),
    )
    graph.aget_state.reset_mock()
    try:
        result = lark_app.handle_card_action(
            _event("edit_draft", email_id=email_id)
        )

        graph.aget_state.assert_not_awaited()
        assert dependencies.content_store.loads == []
        assert dependencies.drafts.loads == []
        assert result["toast"]["content"] == "编辑正文"
    finally:
        lark_app._mock_store.pop(email_id, None)


def test_test_push_prefix_without_explicit_mock_uses_production_boundary(
    production_lark_boundary,
    monkeypatch,
):
    _state, graph, dependencies = production_lark_boundary
    email_id = "test_push_not_seeded"
    lark_app._mock_store.pop(email_id, None)
    monkeypatch.setattr(
        lark_app,
        "get_settings",
        lambda: _lark_settings(debug=True),
    )

    result = lark_app.handle_card_action(_event("edit_draft", email_id=email_id))

    graph.aget_state.assert_awaited_once()
    assert dependencies.content_store.loads == [(_ref(), False)]
    assert dependencies.drafts.loads == ["mail-1"]
    assert result["toast"]["content"] == "编辑正文"


def test_seeded_mock_with_debug_disabled_uses_production_boundary(
    production_lark_boundary,
    monkeypatch,
):
    _state, graph, dependencies = production_lark_boundary
    email_id = "test_push_debug_disabled"
    _seed_test_card(email_id)
    monkeypatch.setattr(
        lark_app,
        "get_settings",
        lambda: _lark_settings(debug=False),
    )
    try:
        result = lark_app.handle_card_action(
            _event("edit_draft", email_id=email_id)
        )

        graph.aget_state.assert_awaited_once()
        assert dependencies.content_store.loads == [(_ref(), False)]
        assert dependencies.drafts.loads == ["mail-1"]
        assert result["toast"]["content"] == "编辑正文"
    finally:
        lark_app._mock_store.pop(email_id, None)


def test_debug_true_non_test_seed_uses_production_action_boundary(
    production_lark_boundary,
    monkeypatch,
):
    _state, graph, dependencies = production_lark_boundary
    email_id = "REAL-EXCHANGE-ID"
    _seed_test_card(email_id)
    monkeypatch.setattr(
        lark_app,
        "get_settings",
        lambda: _lark_settings(debug=True),
    )
    try:
        result = lark_app.handle_card_action(
            _event("edit_draft", email_id=email_id)
        )

        graph.aget_state.assert_awaited_once()
        assert dependencies.content_store.loads == [(_ref(), False)]
        assert dependencies.drafts.loads == ["mail-1"]
        assert result["toast"]["content"] == "编辑正文"
    finally:
        lark_app._mock_store.pop(email_id, None)


@pytest.mark.parametrize(
    ("action", "option", "form_value", "expected_status"),
    [
        ("approve", None, None, "approved"),
        ("reject", None, None, "rejected"),
        ("reject_with_reason", "tone_wrong", None, "rejected"),
        ("mark_read", None, None, "read"),
        ("save_draft_only", None, None, "draft_saved"),
        ("save_draft", None, {"draft_input": "EDITED TEST DRAFT"}, "modified"),
        (
            "save_modification",
            None,
            {"draft_input": "LEGACY EDITED TEST DRAFT"},
            "modified",
        ),
    ],
)
def test_explicit_test_actions_are_in_memory_only(
    production_lark_boundary,
    monkeypatch,
    action,
    option,
    form_value,
    expected_status,
):
    _state, graph, dependencies = production_lark_boundary
    email_id = f"test_push_action_{action}"
    mock_state = _seed_test_card(email_id)
    monkeypatch.setattr(
        lark_app,
        "get_settings",
        lambda: _lark_settings(debug=True),
    )
    exchange = SimpleNamespace(create_draft=AsyncMock())
    monkeypatch.setattr(lark_app, "exchange_client", exchange)
    graph.aget_state.reset_mock()
    try:
        result = lark_app.handle_card_action(
            _event(
                action,
                email_id=email_id,
                option=option,
                form_value=form_value,
            )
        )

        assert isinstance(result, dict)
        assert mock_state.values["status"] == expected_status
        graph.aget_state.assert_not_awaited()
        graph.aupdate_state.assert_not_awaited()
        graph.ainvoke.assert_not_awaited()
        lark_app.db_manager.update_status.assert_not_awaited()
        assert dependencies.content_store.loads == []
        assert dependencies.drafts.loads == []
        assert dependencies.drafts.saves == []
        exchange.create_draft.assert_not_awaited()
        lark_app.card_builder.build_approval_card.assert_not_called()
    finally:
        lark_app._mock_store.pop(email_id, None)


def test_explicit_test_pdf_uses_dedicated_in_memory_flow(
    production_lark_boundary,
    monkeypatch,
):
    _state, graph, dependencies = production_lark_boundary
    email_id = "test_push_pdf_action"
    mock_state = _seed_test_card(email_id)
    monkeypatch.setattr(
        lark_app,
        "get_settings",
        lambda: _lark_settings(debug=True),
    )
    production_pdf = AsyncMock()
    test_pdf = AsyncMock()
    monkeypatch.setattr(lark_app, "process_pdf_generation_and_reply", production_pdf)
    monkeypatch.setattr(
        lark_app,
        "_process_test_card_pdf_generation_and_reply",
        test_pdf,
        raising=False,
    )
    graph.aget_state.reset_mock()
    try:
        result = lark_app.handle_card_action(
            _event("view_original_pdf", email_id=email_id)
        )

        assert result["toast"]["type"] == "info"
        test_pdf.assert_awaited_once_with(email_id, mock_state, "message-1")
        production_pdf.assert_not_awaited()
        graph.aget_state.assert_not_awaited()
        graph.aupdate_state.assert_not_awaited()
        assert dependencies.content_store.loads == []
        assert dependencies.drafts.loads == []
        lark_app.card_builder.build_approval_card.assert_not_called()
        lark_app.db_manager.update_status.assert_not_awaited()
    finally:
        lark_app._mock_store.pop(email_id, None)


def test_explicit_test_recipient_search_uses_seeded_candidates_only(
    production_lark_boundary,
    monkeypatch,
):
    _state, graph, dependencies = production_lark_boundary
    email_id = "test_push_recipient_search"
    mock_state = _seed_test_card(email_id)
    monkeypatch.setattr(
        lark_app,
        "get_settings",
        lambda: _lark_settings(debug=True),
    )
    lark_app.card_builder.search_person_picker_candidates.return_value = [
        "production-user"
    ]
    monkeypatch.setattr(lark_app, "update_card_ui", MagicMock())
    graph.aget_state.reset_mock()
    try:
        result = lark_app.handle_card_action(
            _event(
                "search_to",
                email_id=email_id,
                form_value={"to_search_keyword": "alice"},
            )
        )

        assert result["toast"]["type"] == "info"
        assert mock_state.values["email"]["draft_to_options"] == ["mock-user-1"]
        lark_app.card_builder.search_person_picker_candidates.assert_not_called()
        graph.aget_state.assert_not_awaited()
        graph.aupdate_state.assert_not_awaited()
        assert dependencies.content_store.loads == []
        assert dependencies.drafts.loads == []
        lark_app.card_builder.build_approval_card.assert_not_called()
    finally:
        lark_app._mock_store.pop(email_id, None)


@pytest.mark.parametrize(
    ("action", "options", "form_value"),
    [
        ("view_original", None, None),
        ("edit_to", None, None),
        ("edit_cc", None, None),
        ("edit_draft", None, None),
        ("select_to", ["mock-user-1"], None),
        ("select_cc", ["mock-user-1"], None),
        (
            "save_to",
            None,
            {
                "to_existing": ["mock-user-1"],
                "to_new": [],
                "to_external_input": "external@example.com",
            },
        ),
        (
            "save_cc",
            None,
            {
                "cc_existing": [],
                "cc_new": ["mock-user-1"],
                "cc_external_input": "",
            },
        ),
        ("cancel_edit", None, None),
        ("modify", None, None),
        ("cancel_modification", None, None),
    ],
)
def test_explicit_test_ui_and_recipient_actions_never_touch_production_state(
    production_lark_boundary,
    monkeypatch,
    action,
    options,
    form_value,
):
    _state, graph, dependencies = production_lark_boundary
    email_id = f"test_push_ui_{action}"
    _seed_test_card(email_id)
    monkeypatch.setattr(
        lark_app,
        "get_settings",
        lambda: _lark_settings(debug=True),
    )
    monkeypatch.setattr(
        lark_app,
        "lark_api_client",
        SimpleNamespace(
            im=SimpleNamespace(
                v1=SimpleNamespace(message=SimpleNamespace(reply=MagicMock()))
            )
        ),
    )
    exchange = SimpleNamespace(create_draft=AsyncMock(), resolve_contact=AsyncMock())
    monkeypatch.setattr(lark_app, "exchange_client", exchange)
    graph.aget_state.reset_mock()
    try:
        result = lark_app.handle_card_action(
            _event(
                action,
                email_id=email_id,
                options=options,
                form_value=form_value,
            )
        )

        assert isinstance(result, dict)
        graph.aget_state.assert_not_awaited()
        graph.aupdate_state.assert_not_awaited()
        graph.ainvoke.assert_not_awaited()
        lark_app.db_manager.update_status.assert_not_awaited()
        assert dependencies.content_store.loads == []
        assert dependencies.drafts.loads == []
        assert dependencies.drafts.saves == []
        exchange.create_draft.assert_not_awaited()
        exchange.resolve_contact.assert_not_awaited()
        lark_app.card_builder.build_approval_card.assert_not_called()
    finally:
        lark_app._mock_store.pop(email_id, None)


@pytest.mark.asyncio
async def test_dedicated_test_pdf_flow_uses_only_seeded_email_and_lark(
    production_lark_boundary,
    monkeypatch,
):
    _state, graph, dependencies = production_lark_boundary
    email_id = "test_push_pdf_helper"
    mock_state = _seed_test_card(email_id)
    monkeypatch.setattr(
        lark_app,
        "get_settings",
        lambda: _lark_settings(debug=True),
    )
    monkeypatch.setattr(
        lark_app,
        "_render_test_card_pdf",
        AsyncMock(return_value=b"TEST-PDF"),
    )
    monkeypatch.setattr(
        lark_app,
        "upload_file_to_drive",
        MagicMock(
            return_value={
                "url": "https://example.test/test.pdf",
                "file_token": "test-pdf-token",
            }
        ),
    )
    reply = MagicMock()
    monkeypatch.setattr(
        lark_app,
        "lark_api_client",
        SimpleNamespace(
            im=SimpleNamespace(v1=SimpleNamespace(message=SimpleNamespace(reply=reply)))
        ),
    )
    graph.aget_state.reset_mock()
    try:
        await lark_app._process_test_card_pdf_generation_and_reply(
            email_id,
            mock_state,
            "message-1",
        )

        assert mock_state.values["pdf_token"] == "test-pdf-token"
        reply.assert_called_once()
        graph.aget_state.assert_not_awaited()
        graph.aupdate_state.assert_not_awaited()
        graph.ainvoke.assert_not_awaited()
        lark_app.db_manager.update_status.assert_not_awaited()
        assert dependencies.content_store.loads == []
        assert dependencies.drafts.loads == []
        assert dependencies.drafts.saves == []
    finally:
        lark_app._mock_store.pop(email_id, None)


def test_draft_edit_saves_full_text_and_updates_graph_with_id_only(
    production_lark_boundary,
):
    state, graph, dependencies = production_lark_boundary

    lark_app.process_modification("mail-1", "EDITED-DRAFT-SENTINEL")

    assert dependencies.drafts.saves == [("mail-1", "EDITED-DRAFT-SENTINEL")]
    update = graph.aupdate_state.await_args.args[1]
    assert update == {
        "draft_id": "mail-1",
    }
    assert "EDITED-DRAFT-SENTINEL" not in str(update)
    assert "draft" not in state.values


def test_recipient_edit_updates_bounded_top_level_field_not_email_copy(
    production_lark_boundary,
):
    _state, graph, _dependencies = production_lark_boundary

    result = lark_app.handle_card_action(
        _event("select_to", options=["open-id-1", "open-id-2"])
    )

    update = graph.aupdate_state.await_args.args[1]
    assert update == {
        "draft_to": ["open_id=open-id-1", "open_id=open-id-2"],
    }
    assert "email" not in update
    assert result["toast"]["type"] == "success"


def test_recipient_update_rejects_eleven_people_instead_of_silent_truncation(
    production_lark_boundary,
):
    _state, graph, _dependencies = production_lark_boundary

    result = lark_app.handle_card_action(
        _event("select_to", options=[f"open-id-{index}" for index in range(11)])
    )

    graph.aupdate_state.assert_not_awaited()
    assert result.toast.content == "操作失败，请稍后重试"


def test_cleanup_keeps_twenty_attachment_tokens_and_pdf_token(
    production_lark_boundary,
):
    state, _graph, _dependencies = production_lark_boundary
    tokens = [f"file-token-{index}" for index in range(20)]
    state.values["attachment_tokens"] = tokens
    state.values["pdf_token"] = "pdf-token"

    collected = set(lark_app._collect_cleanup_tokens(state))

    assert collected == {*tokens, "pdf-token"}


@pytest.mark.asyncio
async def test_action_cleanup_removes_only_confirmed_deleted_handles(
    production_lark_boundary,
    monkeypatch,
):
    state, _graph, _dependencies = production_lark_boundary
    state.values["attachment_tokens"] = ["DELETE-ME", "KEEP-ME"]
    state.values["pdf_token"] = "DELETE-PDF"
    delete = MagicMock(side_effect=lambda token: token != "KEEP-ME")
    monkeypatch.setattr(lark_app, "delete_file_from_drive", delete)

    await lark_app._cleanup_action_drive_tokens("mail-1", state)

    assert state.values["attachment_tokens"] == ["KEEP-ME"]
    assert state.values["pdf_token"] is None
    assert {call.args[0] for call in delete.call_args_list} == {
        "DELETE-ME",
        "KEEP-ME",
        "DELETE-PDF",
    }


def test_recipient_search_persists_only_bounded_ui_state(
    production_lark_boundary,
):
    state, graph, _dependencies = production_lark_boundary
    lark_app.card_builder.search_person_picker_candidates.side_effect = [
        ["candidate-1"],
        ["candidate-2"],
    ]

    lark_app.handle_card_action(
        _event(
            "search_to",
            form_value={
                "to_search_keyword": "first",
                "to_external_input": "external@example.com",
            },
        )
    )
    lark_app.handle_card_action(
        _event(
            "search_to",
            form_value={"to_search_keyword": "second"},
        )
    )

    ui = state.values["recipient_ui"]["to"]
    assert ui["options"] == ["candidate-1", "candidate-2"]
    assert ui["external_input"] == "external@example.com"
    assert "email" not in graph.aupdate_state.await_args.args[1]


def test_late_recipient_search_result_is_dropped_after_approval(
    production_lark_boundary,
    monkeypatch,
):
    _state_value, graph, _dependencies = production_lark_boundary

    def approve_while_searching(_keyword):
        lark_app.db_manager.status = "approved"
        return ["late-candidate"]

    lark_app.card_builder.search_person_picker_candidates.side_effect = (
        approve_while_searching
    )
    patch_card = MagicMock()
    monkeypatch.setattr(lark_app, "update_card_ui", patch_card)

    result = lark_app.handle_card_action(
        _event(
            "search_to",
            form_value={"to_search_keyword": "late"},
        )
    )

    assert result["toast"]["type"] == "info"
    graph.aupdate_state.assert_not_awaited()
    patch_card.assert_not_called()


def _close_scheduled(coroutine):
    coroutine.close()


def test_approval_loads_final_draft_but_graph_update_stays_small(
    production_lark_boundary,
):
    _state, graph, dependencies = production_lark_boundary

    with patch("src.utils.lark_app.safe_async_run", side_effect=_close_scheduled):
        lark_app.process_approval("mail-1", "approver-1")

    assert dependencies.drafts.loads == ["mail-1"]
    lark_app.db_manager.update_status.assert_awaited_once_with(
        "mail-1",
        None,
        approver_user_id="approver-1",
        final_draft="COMPLETE-DRAFT-SENTINEL",
    )
    lark_app.db_manager.compare_and_set_status.assert_awaited_once_with(
        "mail-1",
        expected=frozenset({"waiting_approval"}),
        target="approved",
    )
    update = graph.aupdate_state.await_args.args[1]
    assert update == {"approval_status": "approved"}
    assert "COMPLETE-DRAFT-SENTINEL" not in str(update)


def test_rejection_updates_only_small_status_and_reason_stays_in_database(
    production_lark_boundary,
):
    _state, graph, _dependencies = production_lark_boundary

    with patch("src.utils.lark_app.safe_async_run", side_effect=_close_scheduled):
        lark_app.process_rejection("mail-1", "approver-1", reason="语气不当")

    update = graph.aupdate_state.await_args.args[1]
    assert update == {"approval_status": "rejected"}
    assert "语气不当" not in str(update)
    lark_app.db_manager.update_status.assert_awaited_once_with(
        "mail-1",
        None,
        approver_user_id="approver-1",
        rejection_reason="语气不当",
    )
    lark_app.db_manager.compare_and_set_status.assert_awaited_once_with(
        "mail-1",
        expected=frozenset({"waiting_approval"}),
        target="rejected",
    )


@pytest.mark.asyncio
async def test_save_exchange_draft_hydrates_store_without_graph_fulltext_write(
    production_lark_boundary,
):
    state, graph, _dependencies = production_lark_boundary
    lark_app.exchange_client = SimpleNamespace(create_draft=AsyncMock())

    await lark_app.process_save_draft("mail-1", state)

    args = lark_app.exchange_client.create_draft.await_args
    assert args.args[0] == ["sender@example.com"]
    assert args.args[1] == "Re: complete subject"
    assert "COMPLETE-DRAFT-SENTINEL" in args.args[2]
    assert graph.aupdate_state.await_count == 0
    assert "draft" not in state.values
    assert "body" not in state.values["email"]
