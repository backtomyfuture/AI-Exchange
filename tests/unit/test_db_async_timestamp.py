from src.utils.db_async import normalize_timestamp_input


def test_normalize_timestamp_input_empty_string_to_none():
    assert normalize_timestamp_input("") is None
    assert normalize_timestamp_input("   ") is None


def test_normalize_timestamp_input_keeps_valid_values():
    assert normalize_timestamp_input("2026-02-12T15:00:00") == "2026-02-12T15:00:00"
    assert normalize_timestamp_input(None) is None
