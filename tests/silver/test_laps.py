from datetime import datetime

import pytest

from formula_1.silver.transforms.laps import (
    _lap_stint,
    _lap_times,
    _position_at_lap_end,
    lap_deduped,
    lap_enriched,
    lap_filtered,
    stint_deduped,
    stint_enriched,
    stint_filtered,
)

RAW_STINT_SCHEMA = (
    "session_key string, driver_number string, stint_number string, lap_start string, lap_end string, "
    "compound string, tyre_age_at_start string, _load_id string, _ingested_at timestamp"
)
RAW_LAP_SCHEMA = (
    "session_key string, driver_number string, lap_number string, date_start string, lap_duration string, "
    "is_pit_out_lap string, _load_id string, _ingested_at timestamp"
)
STINTS_SCHEMA = (
    "openf1_session_key int, driver_number int, stint_number int, lap_start int, lap_end int, "
    "compound string, tyre_age_at_start int"
)
LAP_KEYS_SCHEMA = "openf1_session_key int, driver_number int, lap_number int"
LAP_ENDS_SCHEMA = "openf1_session_key int, driver_number int, lap_number int, lap_end_utc timestamp"
POSITIONS_SCHEMA = "openf1_session_key int, driver_number int, ts timestamp, pos int"
SESSION_DRIVERS_SCHEMA = "openf1_session_key int, driver_number int, driver_id string, constructor_id string"
INTERVALS_SCHEMA = (
    "openf1_session_key int, interval_start_utc timestamp, interval_end_utc timestamp, severity_rank int"
)
L1 = "20251201T090000Z"
L2 = "20251208T090000Z"
INGESTED = datetime(2025, 12, 8, 9, 0)


def at(hour, minute, second=0, microsecond=0):
    return datetime(2025, 12, 7, hour, minute, second, microsecond)


def iso(hour, minute, second=0):
    return f"2025-12-07T{hour:02d}:{minute:02d}:{second:02d}+00:00"


def raw_stint(stint="1", compound="SOFT", load_id=L1):
    return ("9839", "44", stint, "1", "20", compound, "0", load_id, INGESTED)


def raw_lap(driver="44", lap="1", start=None, duration=None, pit_out=None, load_id=L1):
    return ("9839", driver, lap, start, duration, pit_out, load_id, INGESTED)


@pytest.mark.parametrize("filtered", [stint_filtered, lap_filtered], ids=lambda f: f.__name__)
def test_filtered_keeps_rows_of_known_sessions_only(spark, filtered):
    df = spark.createDataFrame([("1",), ("2",), ("abc",)], "session_key string")
    sessions = spark.createDataFrame([(1,), (1,)], "openf1_session_key int")

    result = filtered(df, sessions)

    assert [row["session_key"] for row in result.collect()] == ["1"]


def test_stint_deduped_keeps_newest_load_per_stint(spark):
    df = spark.createDataFrame(
        [raw_stint("1", load_id=L1), raw_stint("1", load_id=L2), raw_stint("2", load_id=L1)],
        RAW_STINT_SCHEMA,
    )

    result = stint_deduped(df)

    assert {row["stint_number"]: row["_load_id"] for row in result.collect()} == {"1": L2, "2": L1}


def test_lap_deduped_keeps_newest_load_per_lap(spark):
    df = spark.createDataFrame(
        [raw_lap(lap="1", load_id=L1), raw_lap(lap="1", load_id=L2), raw_lap(lap="2", load_id=L1)],
        RAW_LAP_SCHEMA,
    ).selectExpr("*", "true AS _is_batch")

    result = lap_deduped(df)

    assert {row["lap_number"]: row["_load_id"] for row in result.collect()} == {"1": L2, "2": L1}



def test_stint_enriched_builds_full_row(spark):
    df = spark.createDataFrame([("9839", "44", "2", "21", "40", " soft ", "3", L1, INGESTED)], RAW_STINT_SCHEMA)

    row = stint_enriched(df).first().asDict()

    assert row == {
        "openf1_session_key": 9839,
        "driver_number": 44,
        "stint_number": 2,
        "lap_start": 21,
        "lap_end": 40,
        "compound": "SOFT",
        "_chk_compound_ok": True,
        "tyre_age_at_start": 3,
        "ingested_at": INGESTED,
        "_load_id": L1,
    }


@pytest.mark.parametrize(
    ("compound", "expected", "ok"),
    [
        ("SOFT", "SOFT", True),
        (" medium ", "MEDIUM", True),
        ("Intermediate", "INTERMEDIATE", True),
        ("UNKNOWN", None, False),
        ("HYPERSOFT", None, False),
        (None, None, True),
    ],
)
def test_stint_enriched_accepts_only_known_compounds(spark, compound, expected, ok):
    df = spark.createDataFrame([raw_stint(compound=compound)], RAW_STINT_SCHEMA)

    row = stint_enriched(df).first()

    assert (row["compound"], row["_chk_compound_ok"]) == (expected, ok)


def test_lap_times_take_end_from_duration_or_next_lap_start(spark):
    df = spark.createDataFrame(
        [
            raw_lap(driver="44", lap="1", start=iso(13, 0), duration="90.5", pit_out="false"),
            raw_lap(driver="44", lap="2", start=iso(13, 1, 31), duration=None, pit_out="True"),
            raw_lap(driver="44", lap="3", start=iso(13, 3), duration=None),
            raw_lap(driver="1", lap="3", start=iso(13, 2), duration=None),
        ],
        RAW_LAP_SCHEMA,
    )

    rows = {
        (row["driver_number"], row["lap_number"]): (row["lap_end_utc"], row["lap_duration_ms"], row["is_pit_out_lap"])
        for row in _lap_times(df).collect()
    }

    assert rows[(44, 1)] == (at(13, 1, 30, 500000), 90500, False)
    assert rows[(44, 2)] == (at(13, 3), None, True)
    assert rows[(44, 3)] == (None, None, False)


def stint_rows(spark, laps, stints):
    laps_df = spark.createDataFrame(laps, LAP_KEYS_SCHEMA)
    stints_df = spark.createDataFrame(stints, STINTS_SCHEMA)
    return {
        row["lap_number"]: (row["stint_number"], row["compound"], row["tyre_age_laps"])
        for row in _lap_stint(laps_df, stints_df).collect()
    }


def test_lap_stint_adds_compound_and_tyre_age(spark):
    result = stint_rows(
        spark,
        [(9839, 44, 5), (9839, 44, 25)],
        [(9839, 44, 1, 1, 20, "SOFT", 3), (9839, 44, 2, 21, 40, "HARD", 0)],
    )

    assert result == {5: (1, "SOFT", 7), 25: (2, "HARD", 4)}


def test_lap_stint_takes_lower_stint_when_stints_share_a_lap(spark):
    result = stint_rows(
        spark,
        [(9839, 44, 20)],
        [(9839, 44, 1, 1, 20, "SOFT", 0), (9839, 44, 2, 20, 40, "HARD", 0)],
    )

    assert result == {20: (1, "SOFT", 19)}


def test_lap_stint_keeps_lap_without_stint(spark):
    result = stint_rows(spark, [(9839, 44, 5)], [(9839, 16, 1, 1, 20, "SOFT", 0)])

    assert result == {5: (None, None, None)}


def test_position_at_lap_end_takes_last_known_position(spark):
    laps = spark.createDataFrame(
        [
            (9839, 44, 1, at(12, 59)),
            (9839, 44, 2, at(13, 2)),
            (9839, 44, 3, at(13, 3)),
            (9839, 44, 4, at(13, 5)),
        ],
        LAP_ENDS_SCHEMA,
    )
    positions = spark.createDataFrame(
        [
            (9839, 44, at(13, 0), 5),
            (9839, 44, at(13, 3), 4),
            (9839, 16, at(13, 4), 1),
        ],
        POSITIONS_SCHEMA,
    )

    result = _position_at_lap_end(laps, positions)

    assert {row["lap_number"]: row["position_at_lap_end"] for row in result.collect()} == {
        1: None,
        2: 5,
        3: 4,
        4: 4,
    }


def test_lap_enriched_builds_full_row(spark):
    laps = spark.createDataFrame(
        [raw_lap(lap="12", start=iso(13, 0), duration="90.5", pit_out="false")], RAW_LAP_SCHEMA
    )
    stints = spark.createDataFrame([(9839, 44, 1, 1, 20, "MEDIUM", 2)], STINTS_SCHEMA)
    session_driver = spark.createDataFrame(
        [(9839, 44, "hamilton", "ferrari"), (9839, 16, "leclerc", "ferrari")], SESSION_DRIVERS_SCHEMA
    )
    intervals = spark.createDataFrame([(9839, at(13, 1), at(13, 10), 2)], INTERVALS_SCHEMA)
    pit_in = spark.createDataFrame([(9839, 44, 12)], LAP_KEYS_SCHEMA)
    positions = spark.createDataFrame([(9839, 44, at(12, 50), 3)], POSITIONS_SCHEMA)

    row = lap_enriched(laps, stints, session_driver, intervals, pit_in, positions).first().asDict()

    assert row == {
        "openf1_session_key": 9839,
        "driver_number": 44,
        "lap_number": 12,
        "driver_id": "hamilton",
        "constructor_id": "ferrari",
        "lap_start_utc": at(13, 0),
        "lap_end_utc": at(13, 1, 30, 500000),
        "lap_duration_ms": 90500,
        "is_pit_in_lap": True,
        "is_pit_out_lap": False,
        "compound": "MEDIUM",
        "stint_number": 1,
        "tyre_age_laps": 13,
        "track_status_name": "Safety Car",
        "position_at_lap_end": 3,
        "ingested_at": INGESTED,
        "_chk_n_drivers": 2,
        "_load_id": L1,
    }


