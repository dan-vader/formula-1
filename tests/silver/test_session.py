from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from formula_1.silver.transforms.session import (
    _matched_calendar,
    session_deduped,
    session_driver_deduped,
    session_driver_filtered,
    session_filtered,
    session_id,
)

SESSION_RAW_SCHEMA = "session_key string, session_name string, _load_id string, _ingested_at timestamp"
DRIVER_RAW_SCHEMA = "session_key string, driver_number string, name_acronym string, _load_id string, _ingested_at timestamp"
L1 = "20251201T090000Z"
L2 = "20251208T090000Z"
INGESTED = datetime(2025, 12, 8, 9, 0)
SESSIONS_SCHEMA = "openf1_session_key int, year int, session_type string, session_start_utc timestamp"
SCHEDULE_SCHEMA = "sched_season int, sched_round int, sched_type string, sched_date date"


@pytest.mark.parametrize(
    ("season", "round_number", "session_type", "expected"),
    [
        (2026, 4, "Race", "2026-04-R"),
        (2025, 12, "Race", "2025-12-R"),
        (2025, 4, "Sprint", "2025-04-S"),
    ],
)

def test_session_id(spark, season, round_number, session_type, expected):
    df = spark.createDataFrame([(season, round_number, session_type)], "season int, round int, session_type string")

    result = df.select(session_id(F.col("season"), F.col("round"), F.col("session_type")).alias("id")).first()

    assert result["id"] == expected


def match(spark, sessions, schedule):
    result = _matched_calendar(
        spark.createDataFrame(sessions, SESSIONS_SCHEMA),
        spark.createDataFrame(schedule, SCHEDULE_SCHEMA),
    )
    return {row["openf1_session_key"]: (row["season"], row["round"], row["_chk_schedule_matches"]) for row in result.collect()}


def test_session_id_is_null_when_round_is_unknown(spark):
    df = spark.createDataFrame([(2026, None, "Race")], "season int, round int, session_type string")

    result = df.select(session_id(F.col("season"), F.col("round"), F.col("session_type")).alias("id")).first()

    assert result["id"] is None


def test_session_filtered_keeps_only_race_and_sprint_that_are_not_cancelled(spark):
    df = spark.createDataFrame(
        [
            ("1", "Race", "false"),
            ("2", "Sprint", None),
            ("3", "Qualifying", "false"),
            ("4", "Race", "True"),
            ("5", "Practice 1", None),
        ],
        "session_key string, session_name string, is_cancelled string",
    )

    result = session_filtered(df)

    assert sorted(row["session_key"] for row in result.collect()) == ["1", "2"]


def test_session_deduped_keeps_newest_load_per_session(spark):
    df = spark.createDataFrame(
        [
            ("9839", "Race", L1, INGESTED),
            ("9839", "Race", L2, INGESTED),
            ("9840", "Sprint", L1, INGESTED),
        ],
        SESSION_RAW_SCHEMA,
    )

    result = session_deduped(df)

    rows = {row["session_key"]: row["_load_id"] for row in result.collect()}
    assert rows == {"9839": L2, "9840": L1}


def test_session_driver_filtered_keeps_drivers_of_known_sessions_only(spark):
    drivers = spark.createDataFrame(
        [
            ("1", "44", "HAM", L1, INGESTED),
            ("2", "44", "HAM", L1, INGESTED),
            ("abc", "44", "HAM", L1, INGESTED),
        ],
        DRIVER_RAW_SCHEMA,
    )
    session = spark.createDataFrame([(1,), (1,)], "openf1_session_key int")

    result = session_driver_filtered(drivers, session)

    assert [row["session_key"] for row in result.collect()] == ["1"]


def test_session_driver_deduped_keeps_one_row_per_driver_in_session(spark):
    df = spark.createDataFrame(
        [
            ("1", "44", "HAM", L1, INGESTED),
            ("1", "44", "HAM", L2, INGESTED),
            ("2", "44", "HAM", L1, INGESTED),
            ("1", "16", "LEC", L1, INGESTED),
        ],
        DRIVER_RAW_SCHEMA,
    )

    result = session_driver_deduped(df)

    rows = {(row["session_key"], row["driver_number"]): row["_load_id"] for row in result.collect()}
    assert rows == {("1", "44"): L2, ("2", "44"): L1, ("1", "16"): L1}

def test_matched_calendar_finds_round_for_session_on_same_day(spark):
    result = match(
        spark,
        [(9839, 2025, "Race", datetime(2025, 12, 7, 13, 0))],
        [(2025, 24, "Race", date(2025, 12, 7)), (2025, 23, "Race", date(2025, 11, 30))],
    )

    assert result == {9839: (2025, 24, True)}


def test_matched_calendar_accepts_one_day_difference(spark):
    result = match(
        spark,
        [(9840, 2025, "Race", datetime(2025, 11, 23, 4, 0))],
        [(2025, 22, "Race", date(2025, 11, 22))],
    )

    assert result == {9840: (2025, 22, True)}


def test_matched_calendar_leaves_round_empty_when_no_match(spark):
    result = match(
        spark,
        [
            (1, 2025, "Race", datetime(2025, 12, 10, 13, 0)),
            (2, 2025, "Sprint", datetime(2025, 12, 7, 13, 0)),
        ],
        [(2025, 24, "Race", date(2025, 12, 7))],
    )

    assert result == {1: (None, None, False), 2: (None, None, False)}


def test_matched_calendar_leaves_round_empty_when_match_is_ambiguous(spark):
    result = match(
        spark,
        [(9839, 2025, "Race", datetime(2025, 12, 7, 13, 0))],
        [(2025, 23, "Race", date(2025, 12, 6)), (2025, 24, "Race", date(2025, 12, 7))],
    )

    assert result == {9839: (None, None, False)}
