from datetime import date, datetime
from decimal import Decimal

import pytest

from formula_1.silver.transforms.results import (
    duration_ms,
    jolpica_pit_deduped,
    jolpica_pit_enriched,
    race_result_deduped,
    race_result_enriched,
)

RESULT_SCHEMA = "session_id string, driver_id string, snapshot_ts timestamp, result_hash string, _ingested_at timestamp"
INGESTED = datetime(2025, 12, 20, 9, 0)
L1 = "20251208T090000Z"
RESULT_ROW_SCHEMA = (
    "session_id string, driver_id string, snapshot_ts timestamp, constructor_id string, car_number int, "
    "grid_position int, position_text string, classified_position int, points decimal(5,2), status string, "
    "laps_completed int, result_hash string, _ingested_at timestamp, _chk_grid_valid boolean, _load_id string, "
    "_is_first boolean"
)
SESSION_DATES_SCHEMA = "session_id string, openf1_session_key int, session_date_local date"
FINISH_SCHEMA = "status_id int, status string"
RACE_DAY = date(2025, 12, 7)
L2 = "20251215T090000Z"
PIT_SCHEMA = (
    "session_id string, driver_id string, stop_number int, lap_number int, _local_time string, "
    "duration_ms bigint, _ingested_at timestamp, _load_id string"
)
PIT_SESSION_SCHEMA = "session_id string, session_date_local date, gmt_offset string"

def result_row(snapshot_ts=datetime(2025, 12, 7, 15, 0), status="Finished", is_first=True, session_id="2025-24-R"):
    return (
        session_id, "hamilton", snapshot_ts, "ferrari", 44, 3, "1", 1, Decimal("25.00"), status, 58, "A",
        INGESTED, True, L1, is_first,
    )


def enrich(spark, rows):
    df = spark.createDataFrame(rows, RESULT_ROW_SCHEMA)
    session = spark.createDataFrame([("2025-24-R", 9839, RACE_DAY)], SESSION_DATES_SCHEMA)
    finish_status = spark.createDataFrame([(1, "Finished"), (5, "Engine")], FINISH_SCHEMA)
    return race_result_enriched(df, session, finish_status).collect()


def version(day, result_hash, driver_id="hamilton", ingested=INGESTED):
    return ("2025-24-R", driver_id, datetime(2025, 12, day, 9, 0), result_hash, ingested)


def kept_versions(result):
    return sorted((row["driver_id"], row["snapshot_ts"].day, row["result_hash"]) for row in result.collect())


@pytest.mark.parametrize(
    ("duration", "expected"),
    [
        ("22.5", 22500),
        ("2.4", 2400),
        ("22.5234", 22523),
        ("1:05.3", 65300),
        ("25:11.123", 1511123),
        ("abc", None),
        ("", None),
        (None, None),
    ],
)
def test_duration_ms(spark, duration, expected):
    df = spark.createDataFrame([(duration,)], "duration string")

    result = df.select(duration_ms("duration").alias("ms")).first()

    assert result["ms"] == expected


def test_race_result_deduped_keeps_first_and_changed_versions(spark):
       df = spark.createDataFrame([version(7, "A"), version(8, "A"), version(15, "B"), version(16, "B")], RESULT_SCHEMA)

       result = race_result_deduped(df)

       assert kept_versions(result) == [("hamilton", 7, "A"), ("hamilton", 15, "B")]
       flags = {row["snapshot_ts"].day: row["_is_first"] for row in result.collect()}
       assert flags == {7: True, 15: False}


def test_race_result_deduped_keeps_change_back_to_previous_result(spark):
    df = spark.createDataFrame([version(7, "A"), version(15, "B"), version(20, "A")], RESULT_SCHEMA)

    result = race_result_deduped(df)

    assert kept_versions(result) == [("hamilton", 7, "A"), ("hamilton", 15, "B"), ("hamilton", 20, "A")]


def test_race_result_deduped_compares_versions_per_driver(spark):
    df = spark.createDataFrame(
        [version(7, "A", driver_id="hamilton"), version(8, "A", driver_id="russell")],
        RESULT_SCHEMA,
    )

    result = race_result_deduped(df)

    assert kept_versions(result) == [("hamilton", 7, "A"), ("russell", 8, "A")]


def test_race_result_deduped_keeps_latest_ingest_of_same_snapshot(spark):
    df = spark.createDataFrame(
        [
            version(7, "A", ingested=datetime(2025, 12, 7, 10, 0)),
            version(7, "B", ingested=datetime(2025, 12, 7, 11, 0)),
        ],
        RESULT_SCHEMA,
    )

    result = race_result_deduped(df)

    assert kept_versions(result) == [("hamilton", 7, "B")]


def test_race_result_enriched_adds_session_key_and_status_id(spark):
    row = enrich(spark, [result_row()])[0].asDict()

    assert row == {
        "session_id": "2025-24-R",
        "driver_id": "hamilton",
        "snapshot_ts": datetime(2025, 12, 7, 15, 0),
        "openf1_session_key": 9839,
        "constructor_id": "ferrari",
        "car_number": 44,
        "grid_position": 3,
        "position_text": "1",
        "classified_position": 1,
        "points": Decimal("25.00"),
        "status_id": 1,
        "laps_completed": 58,
        "result_hash": "A",
        "is_late_change": False,
        "ingested_at": INGESTED,
        "_chk_grid_valid": True,
        "_load_id": L1,
    }


@pytest.mark.parametrize(
    ("snapshot_day", "is_first", "expected"),
    [
        (date(2025, 12, 21), False, False),
        (date(2025, 12, 22), False, True),
        (date(2026, 1, 20), True, False),
    ],
)
def test_race_result_enriched_marks_late_change(spark, snapshot_day, is_first, expected):
    snapshot_ts = datetime(snapshot_day.year, snapshot_day.month, snapshot_day.day, 9, 0)

    row = enrich(spark, [result_row(snapshot_ts=snapshot_ts, is_first=is_first)])[0]

    assert row["is_late_change"] is expected


def test_race_result_enriched_keeps_row_with_unknown_status_and_session(spark):
    rows = enrich(
        spark,
        [result_row(status="Hydraulics", session_id="2025-99-R", is_first=False, snapshot_ts=datetime(2026, 3, 1, 9, 0))],
    )

    assert len(rows) == 1
    assert rows[0]["status_id"] is None
    assert rows[0]["openf1_session_key"] is None
    assert rows[0]["is_late_change"] is False


def test_jolpica_pit_deduped_keeps_newest_load_per_stop(spark):
    df = spark.createDataFrame(
        [
            ("2025-24-R", "hamilton", 1, 20, "17:28:31", 22500, INGESTED, L1),
            ("2025-24-R", "hamilton", 1, 20, "17:28:31", 22400, INGESTED, L2),
            ("2025-24-R", "hamilton", 2, 41, "18:01:10", 23100, INGESTED, L1),
        ],
        PIT_SCHEMA,
    )

    result = jolpica_pit_deduped(df)

    assert {row["stop_number"]: row["_load_id"] for row in result.collect()} == {1: L2, 2: L1}


def test_jolpica_pit_enriched_converts_local_time_to_utc(spark):
    df = spark.createDataFrame([("2025-22-R", "hamilton", 1, 20, "20:45:12", 22500, INGESTED, L1)], PIT_SCHEMA)
    session = spark.createDataFrame([("2025-22-R", date(2025, 11, 22), "-08:00:00")], PIT_SESSION_SCHEMA)

    row = jolpica_pit_enriched(df, session).first().asDict()

    assert row == {
        "session_id": "2025-22-R",
        "driver_id": "hamilton",
        "stop_number": 1,
        "lap_number": 20,
        "pit_time_utc": datetime(2025, 11, 23, 4, 45, 12),
        "duration_ms": 22500,
        "ingested_at": INGESTED,
        "_load_id": L1,
    }


def test_jolpica_pit_enriched_handles_positive_offset(spark):
    df = spark.createDataFrame([("2025-24-R", "hamilton", 1, 20, "17:28:31", 22500, INGESTED, L1)], PIT_SCHEMA)
    session = spark.createDataFrame([("2025-24-R", date(2025, 12, 7), "04:00:00")], PIT_SESSION_SCHEMA)

    row = jolpica_pit_enriched(df, session).first()

    assert row["pit_time_utc"] == datetime(2025, 12, 7, 13, 28, 31)


def test_jolpica_pit_enriched_keeps_stop_of_unknown_session(spark):
    df = spark.createDataFrame([("2025-99-R", "hamilton", 1, 20, "17:28:31", 22500, INGESTED, L1)], PIT_SCHEMA)
    session = spark.createDataFrame([], PIT_SESSION_SCHEMA)

    rows = jolpica_pit_enriched(df, session).collect()

    assert len(rows) == 1
    assert rows[0]["pit_time_utc"] is None