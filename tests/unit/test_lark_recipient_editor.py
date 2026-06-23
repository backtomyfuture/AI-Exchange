"""Unit tests for ``src.utils.lark_recipient_editor``."""

from types import SimpleNamespace

from src.utils.lark_recipient_editor import (
    build_recipient_field,
    clear_recipient_edit_temp,
    extract_external_emails_from_recipients,
    merge_keep_and_add,
    merge_unique,
    normalize_email_list,
    normalize_uid_list,
    read_selected_open_ids,
)


# ---------------------------------------------------------------------------
# merge_unique
# ---------------------------------------------------------------------------

def test_merge_unique_strips_and_dedupes_preserving_order():
    assert merge_unique([" a", "b", " a ", "c", "b"]) == ["a", "b", "c"]


def test_merge_unique_drops_empty_strings():
    # Note: str(None) -> "None"; legacy semantics preserved.
    assert merge_unique(["", "x", " "]) == ["x"]


# ---------------------------------------------------------------------------
# read_selected_open_ids
# ---------------------------------------------------------------------------

def test_read_selected_open_ids_handles_options_list():
    action = SimpleNamespace(options=["u1", " u2 ", ""], option=None)
    assert read_selected_open_ids(action) == ["u1", "u2"]


def test_read_selected_open_ids_handles_single_option():
    action = SimpleNamespace(options=None, option="u3")
    assert read_selected_open_ids(action) == ["u3"]


def test_read_selected_open_ids_dedupes_across_fields():
    action = SimpleNamespace(options=["u1", "u2"], option="u1")
    assert read_selected_open_ids(action) == ["u1", "u2"]


# ---------------------------------------------------------------------------
# normalize_uid_list
# ---------------------------------------------------------------------------

def test_normalize_uid_list_accepts_list():
    assert normalize_uid_list(["a", "b", "a"]) == ["a", "b"]


def test_normalize_uid_list_parses_json_string():
    assert normalize_uid_list('["a","b","b"]') == ["a", "b"]


def test_normalize_uid_list_falls_back_to_csv():
    assert normalize_uid_list("a, b ,c,b") == ["a", "b", "c"]


def test_normalize_uid_list_returns_empty_for_none_or_blank():
    assert normalize_uid_list(None) == []
    assert normalize_uid_list("") == []


# ---------------------------------------------------------------------------
# normalize_email_list
# ---------------------------------------------------------------------------

def test_normalize_email_list_extracts_from_freetext():
    raw = "John <john@example.com>; Jane <jane@example.com>"
    assert normalize_email_list(raw) == ["john@example.com", "jane@example.com"]


def test_normalize_email_list_handles_chinese_separators():
    raw = "a@x.com，b@x.com；c@x.com"
    assert normalize_email_list(raw) == ["a@x.com", "b@x.com", "c@x.com"]


def test_normalize_email_list_drops_invalid():
    raw = "valid@x.com, not-an-email, also@bad"
    assert normalize_email_list(raw) == ["valid@x.com"]


def test_normalize_email_list_handles_iterable_input():
    assert normalize_email_list(["a@x.com", "b@x.com"]) == ["a@x.com", "b@x.com"]


# ---------------------------------------------------------------------------
# extract_external_emails_from_recipients
# ---------------------------------------------------------------------------

def test_extract_external_emails_skips_open_ids():
    items = ["open_id=u1", "ext@example.com"]
    assert extract_external_emails_from_recipients(items) == ["ext@example.com"]


def test_extract_external_emails_handles_legacy_dump():
    items = ["name='Alice', email_address='alice@example.com'"]
    assert extract_external_emails_from_recipients(items) == ["alice@example.com"]


def test_extract_external_emails_handles_angle_bracket():
    items = ["Bob <bob@example.com>"]
    assert extract_external_emails_from_recipients(items) == ["bob@example.com"]


def test_extract_external_emails_dedupes():
    items = ["a@x.com", "a@x.com", "b@x.com"]
    assert extract_external_emails_from_recipients(items) == ["a@x.com", "b@x.com"]


# ---------------------------------------------------------------------------
# clear_recipient_edit_temp
# ---------------------------------------------------------------------------

def test_clear_recipient_edit_temp_removes_only_temp_keys():
    payload = {
        "draft_to": ["a"],
        "draft_to_options": ["x"],
        "draft_to_search_hint": "h",
        "draft_to_new_selected": ["y"],
        "draft_to_external_input": "ext",
        "other": "keep",
    }
    clear_recipient_edit_temp(payload, "to")
    assert payload == {"draft_to": ["a"], "other": "keep"}


# ---------------------------------------------------------------------------
# merge_keep_and_add / build_recipient_field
# ---------------------------------------------------------------------------

def test_merge_keep_and_add_dedupes_and_preserves_order():
    assert merge_keep_and_add(["a", "b"], ["b", "c"]) == ["a", "b", "c"]


def test_build_recipient_field_orders_uids_then_externals():
    result = build_recipient_field(["u1", "u2"], ["e@x.com"])
    assert result == ["open_id=u1", "open_id=u2", "e@x.com"]


def test_build_recipient_field_dedupes_across_groups():
    result = build_recipient_field(["u1", "u1"], ["e@x.com", "e@x.com"])
    assert result == ["open_id=u1", "e@x.com"]
