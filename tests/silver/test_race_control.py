from datetime import datetime

import pytest
from pyspark.sql import functions as F

from formula_1.silver.transforms.race_control import (
    event_type,
    race_control_deduped,
    race_control_enriched,
    track_status,
    track_status_intervals,
)

RAW_SCHEMA = (
    "session_key string, date string, category string, flag string, scope string, "
    "driver_number string, lap_number string, message string, _load_id string, _ingested_at timestamp"
)
EVENTS_SCHEMA = "openf1_session_key int, event_ts timestamp, event_type string, ingested_at timestamp"
SESSION_END_SCHEMA = "openf1_session_key int, session_end_utc timestamp"
INTERVALS_SCHEMA = (
    "openf1_session_key int, interval_start_utc timestamp, interval_end_utc timestamp, severity_rank int"
)
LAPS_SCHEMA = "openf1_session_key int, lap_number int, lap_start_utc timestamp, lap_end_utc timestamp"
L1 = "20251201T090000Z"
L2 = "20251208T090000Z"
INGESTED = datetime(2025, 12, 8, 9, 0)


def at(hour, minute):
    return datetime(2025, 12, 7, hour, minute)


SESSION_END = at(15, 0)

def raw_event(message, load_id=L1, category="SafetyCar", flag=None, date="2025-12-07T13:05:00+00:00"):
    return ("9839", date, category, flag, "Track", None, "12", message, load_id, INGESTED)


@pytest.mark.parametrize(
    ("category", "flag", "message", "expected"),
    [
        ("SafetyCar", None, "VIRTUAL SAFETY CAR DEPLOYED", "VSC_DEPLOYED"),
        ("SafetyCar", None, "VSC DEPLOYED", "VSC_DEPLOYED"),
        ("SafetyCar", None, "virtual safety car deployed", "VSC_DEPLOYED"),
        ("SafetyCar", None, "VIRTUAL SAFETY CAR ENDING", "VSC_ENDING"),
        ("SafetyCar", None, "SAFETY CAR DEPLOYED", "SAFETY_CAR_DEPLOYED"),
        ("SafetyCar", None, "SAFETY CAR IN THIS LAP", "SAFETY_CAR_IN"),
        ("Flag", "RED", "RED FLAG", "RED_FLAG"),
        ("Flag", "red", "RED FLAG", "RED_FLAG"),
        ("Other", None, "TRACK CLEAR", "TRACK_CLEAR"),
        ("Other", None, "CAR 44 (HAM) TIME PENALTY SERVED", "PENALTY_SERVED"),
        ("Flag", "YELLOW", "YELLOW IN TRACK SECTOR 5", "OTHER"),
        ("Other", None, "GREEN LIGHT - PIT EXIT OPEN", "OTHER"),
    ],
)
def test_event_type(spark, category, flag, message, expected):
    df = spark.createDataFrame([(category, flag, message)], "category string, flag string, message string")

    result = df.select(event_type(F.col("category"), F.col("flag"), F.col("message")).alias("t")).first()

    assert result["t"] == expected


def test_race_control_deduped_merges_same_event_and_keeps_different_ones(spark):
    df = spark.createDataFrame(
        [
            raw_event("SAFETY CAR DEPLOYED", load_id=L1),
            raw_event("SAFETY CAR DEPLOYED", load_id=L2),
            raw_event("YELLOW IN TRACK SECTOR 5", category="Flag", flag="YELLOW"),
        ],
        RAW_SCHEMA,
    )

    result = race_control_deduped(df)

    rows = {row["message"]: row["_load_id"] for row in result.collect()}
    assert rows == {"SAFETY CAR DEPLOYED": L2, "YELLOW IN TRACK SECTOR 5": L1}


def test_race_control_enriched_builds_full_row(spark):
    df = spark.createDataFrame([raw_event(" SAFETY CAR DEPLOYED ", flag=" yellow ")], RAW_SCHEMA)

    row = race_control_enriched(race_control_deduped(df)).first().asDict()
    event_hash = row.pop("event_hash")

    assert len(event_hash) == 64
    assert row == {
        "openf1_session_key": 9839,
        "event_ts": at(13, 5),
        "category": "SafetyCar",
        "flag": "YELLOW",
        "scope": "Track",
        "message": "SAFETY CAR DEPLOYED",
        "driver_number": None,
        "lap_number": 12,
        "event_type": "SAFETY_CAR_DEPLOYED",
        "ingested_at": INGESTED,
        "_load_id": L1,
    }


def intervals_for(spark, events, session_end=SESSION_END):
    race_control = spark.createDataFrame(events, EVENTS_SCHEMA)
    session = spark.createDataFrame([(9839, session_end)], SESSION_END_SCHEMA)
    result = track_status_intervals(race_control, session)
    return sorted(
        (row["interval_start_utc"], row["interval_end_utc"], row["track_status_name"], row["severity_rank"])
        for row in result.collect()
    )


def test_track_status_intervals_close_at_first_closing_event(spark):
    result = intervals_for(
        spark,
        [
            (9839, at(13, 5), "SAFETY_CAR_DEPLOYED", INGESTED),
            (9839, at(13, 12), "SAFETY_CAR_IN", INGESTED),
            (9839, at(13, 30), "TRACK_CLEAR", INGESTED),
        ],
    )

    assert result == [(at(13, 5), at(13, 12), "Safety Car", 2)]


def test_track_status_intervals_run_to_session_end_without_closing_event(spark):
    result = intervals_for(spark, [(9839, at(14, 40), "RED_FLAG", INGESTED)])

    assert result == [(at(14, 40), at(15, 0), "Red Flag", 3)]


def test_track_status_intervals_ignore_closing_events_before_opening_or_in_other_session(spark):
    result = intervals_for(
        spark,
        [
            (9839, at(13, 0), "VSC_ENDING", INGESTED),
            (9839, at(13, 5), "VSC_DEPLOYED", INGESTED),
            (1111, at(13, 8), "VSC_ENDING", INGESTED),
        ],
    )

    assert result == [(at(13, 5), at(15, 0), "Virtual Safety Car", 1)]


def test_track_status_intervals_skip_other_events(spark):
    result = intervals_for(
        spark,
        [
            (9839, at(13, 5), "OTHER", INGESTED),
            (9839, at(13, 6), "PENALTY_SERVED", INGESTED),
        ],
    )

    assert result == []


def lap_statuses(spark, laps, intervals):
    laps_df = spark.createDataFrame(laps, LAPS_SCHEMA)
    intervals_df = spark.createDataFrame(intervals, INTERVALS_SCHEMA)
    result = track_status(
        laps_df, intervals_df, ["openf1_session_key", "lap_number"], F.col("lap_start_utc"), F.col("lap_end_utc")
    )
    return {row["lap_number"]: row["track_status_name"] for row in result.collect()}


def test_track_status_takes_most_severe_overlapping_interval(spark):
    result = lap_statuses(
        spark,
        [
            (9839, 10, at(13, 0), at(13, 2)),
            (9839, 11, at(13, 2), at(13, 4)),
            (9839, 12, at(13, 4), at(13, 6)),
        ],
        [
            (9839, at(13, 3), at(13, 10), 1),
            (9839, at(13, 5), at(13, 20), 3),
        ],
    )

    assert result == {10: "Green", 11: "Virtual Safety Car", 12: "Red Flag"}


def test_track_status_ignores_intervals_of_other_sessions(spark):
    result = lap_statuses(
        spark,
        [(9839, 10, at(13, 0), at(13, 2))],
        [(1111, at(12, 0), at(14, 0), 2)],
    )

    assert result == {10: "Green"}


def test_track_status_counts_interval_that_touches_lap_boundary(spark):
    result = lap_statuses(
        spark,
        [(9839, 10, at(13, 0), at(13, 2))],
        [(9839, at(13, 2), at(13, 10), 2)],
    )

    assert result == {10: "Safety Car"}


def test_track_status_keeps_lap_without_end_time_green(spark):
    result = lap_statuses(
        spark,
        [(9839, 58, at(14, 50), None)],
        [(9839, at(14, 0), at(15, 0), 2)],
    )

    assert result == {58: "Green"}