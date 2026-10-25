from datetime import UTC, datetime, timedelta

import pytest

from formula_1.bronze.openf1_stream import build_message, delays, parse_time, replay_events


def test_parse_time_returns_none_for_empty_value():
    assert parse_time(None) is  None
    assert parse_time("") is None

def test_parse_time_adds_utc_when_timezone_is_missing():
    result = parse_time("2025-12-07T13:05:00")

    assert result == datetime(2025, 12, 7, 13, 5, tzinfo=UTC)

def test_build_message_takes_lap_time_from_date_start():
    record = {"date_start": "2025-12-07T13:05:00+00:00", "lap_number": 5}

    message = build_message("replay-1", "laps", 9839, record)

    assert message["event_time"] == "2025-12-07T13:05:00+00:00"


def test_build_message_takes_pit_time_from_date():
    record = {"date": "2025-12-07T13:20:00+00:00", "lap_number": 18}

    message = build_message("replay-1", "pit", 9839, record)

    assert message["event_time"] == "2025-12-07T13:20:00+00:00"
    assert message["payload"] == record


def test_build_message_rejects_unknown_endpoint():
    with pytest.raises(ValueError):
        build_message("replay-1", "weather", 9839, {})

def test_replay_events_mixes_endpoints_in_time_order():
    records = {
        "laps": [{"date_start": "2025-12-07T13:10:00+00:00"}],
        "pit": [{"date": "2025-12-07T13:05:00+00:00"}],
        "race_control": [{"date": "2025-12-07T13:15:00+00:00"}],
    }

    events = replay_events(records, "replay-1", 9839)

    assert [message["endpoint"] for _, message in events] == ["pit", "laps", "race_control"]


def test_replay_events_puts_events_without_time_first():
    records = {
        "pit": [{"date": "2025-12-07T13:05:00+00:00"}, {"date": None}],
    }

    events = replay_events(records, "replay-1", 9839)

    first_time, first_message = events[0]
    assert first_time is None
    assert first_message["payload"] == {"date": None}

def test_delays_divide_gap_by_speed_and_respect_cap():
    start = datetime(2025, 12, 7, 13, 0, tzinfo=UTC)
    events = [
        (start, {}),
        (start + timedelta(seconds=60), {}),
        (start + timedelta(seconds=1000), {}),
    ]

    assert delays(events, speed=60, max_sleep_sec=5) == [0.0, 1.0, 5.0]


def test_delays_skip_events_without_time():
    start = datetime(2025, 12, 7, 13, 0, tzinfo=UTC)
    events = [
        (None, {}),
        (start, {}),
        (None, {}),
        (start + timedelta(seconds=120), {}),
    ]

    assert delays(events, speed=60, max_sleep_sec=5) == [0.0, 0.0, 0.0, 2.0]


def test_delays_reject_non_positive_speed():
    with pytest.raises(ValueError):
        delays([], speed=0, max_sleep_sec=5)