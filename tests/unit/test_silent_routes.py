from src.observability.silent_routes import count_silent, silent_share_alert


def test_silent_share_alert_requires_enough_volume_and_drift():
    assert silent_share_alert(
        today_silent=8,
        today_total=10,
        baseline_silent=10,
        baseline_total=100,
    )
    assert not silent_share_alert(
        today_silent=2,
        today_total=10,
        baseline_silent=10,
        baseline_total=100,
    )


def test_count_silent_uses_low_cardinality_statuses():
    silent, total = count_silent(["no_action", "sent", "skipped", "waiting_approval"])
    assert (silent, total) == (2, 4)
