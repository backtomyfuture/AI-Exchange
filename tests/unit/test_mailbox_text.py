from src.utils.mailbox_text import parse_serialized_mailbox


def test_parses_typical_serialized_mailbox() -> None:
    parsed = parse_serialized_mailbox(
        "Mailbox(name='武珉（Annie）', email_address='m.wu@tianjin-air.com', "
        "routing_type='SMTP', mailbox_type='Mailbox')"
    )

    assert parsed is not None
    assert parsed.name == "武珉（Annie）"
    assert parsed.address == "m.wu@tianjin-air.com"


def test_apostrophe_in_name_uses_double_quoted_repr() -> None:
    parsed = parse_serialized_mailbox(
        "Mailbox(name=\"O'Connor\", email_address='o@example.com')"
    )

    assert parsed is not None
    assert parsed.name == "O'Connor"
    assert parsed.address == "o@example.com"


def test_escaped_quotes_inside_single_quoted_name() -> None:
    parsed = parse_serialized_mailbox(
        r"Mailbox(name='O\'Neil \"Sonny\"', email_address='o@example.com')"
    )

    assert parsed is not None
    assert parsed.name == "O'Neil \"Sonny\""


def test_field_order_does_not_matter() -> None:
    parsed = parse_serialized_mailbox(
        "Mailbox(email_address='o@example.com', name='张三')"
    )

    assert parsed is not None
    assert parsed.name == "张三"
    assert parsed.address == "o@example.com"


def test_empty_name_keeps_address_for_caller_fallback() -> None:
    parsed = parse_serialized_mailbox(
        "Mailbox(name='', email_address='zhang-xia@tianjin-air.com', "
        "routing_type='SMTP', mailbox_type='Mailbox')"
    )

    assert parsed is not None
    assert parsed.name == ""
    assert parsed.address == "zhang-xia@tianjin-air.com"


def test_plain_addresses_and_display_formats_are_not_serialized_mailboxes() -> None:
    assert parse_serialized_mailbox("sender@example.test") is None
    assert parse_serialized_mailbox("第二位发件人 <second@example.com>") is None


def test_malformed_or_empty_values_return_none() -> None:
    assert parse_serialized_mailbox(None) is None
    assert parse_serialized_mailbox(123) is None
    assert parse_serialized_mailbox("") is None
    assert parse_serialized_mailbox("   ") is None
    assert parse_serialized_mailbox("Mailbox(name=未加引号)") is None
