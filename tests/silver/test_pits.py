from datetime import datetime

import pytest

from formula_1.silver.transforms.pits import (
    _penalty_visits,
    _pit_positions,
    _pit_times,
    _pit_tyres,
    overtake_deduped,
    overtake_enriched,
    overtake_filtered,
    pit_deduped,
    pit_enriched,
    pit_filtered,
)

RAW_PIT_SCHEMA = (
    "session_key string, driver_number string, lap_number string, date string, lane_duration string, "
    "pit_duration string, stop_duration string, _load_id string, _ingested_at timestamp"
)
RAW_OVERTAKE_SCHEMA = (
    "session_key string, date string, overtaking_driver_number string, overtaken_driver_number string, "
    "position string, _load_id string, _ingested_at timestamp"
)
PIT_KEYS_SCHEMA = "openf1_session_key int, driver_number int, lap_number int"
VISITS_SCHEMA = "openf1_session_key int, driver_number int, lap_number int, pit_entry_utc timestamp"
STINTS_SCHEMA = (
    "openf1_session_key int, driver_number int, stint_number int, lap_start int, lap_end int, "
    "compound string, tyre_age_at_start int"
)
RACE_CONTROL_SCHEMA = (
    "openf1_session_key int, event_ts timestamp, event_type string, driver_number int, message string"
)
LAP_POSITIONS_SCHEMA = "openf1_session_key int, driver_number int, lap_number int, position_at_lap_end int"
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


def raw_pit(lap="20", lane="20.5", pit=None, stop="2.4", load_id=L1):
    return ("9839", "44", lap, iso(13, 20), lane, pit, stop, load_id, INGESTED)


def raw_overtake(overtaken="16", load_id=L1):
    return ("9839", iso(13, 30), "44", overtaken, "3", load_id, INGESTED)


@pytest.mark.parametrize("filtered", [pit_filtered, overtake_filtered], ids=lambda f: f.__name__)
def test_filtered_keeps_rows_of_known_sessions_only(spark, filtered):
    df = spark.createDataFrame([("1",), ("2",), ("abc",)], "session_key string")
    sessions = spark.createDataFrame([(1,), (1,)], "openf1_session_key int")

    result = filtered(df, sessions)

    assert [row["session_key"] for row in result.collect()] == ["1"]


def test_pit_deduped_keeps_newest_load_per_lap(spark):
    df = spark.createDataFrame(
        [raw_pit(lap="20", load_id=L1), raw_pit(lap="20", load_id=L2), raw_pit(lap="41", load_id=L1)],
        RAW_PIT_SCHEMA,
    ).selectExpr("*", "true AS _is_batch")

    result = pit_deduped(df)

    assert {row["lap_number"]: row["_load_id"] for row in result.collect()} == {"20": L2, "41": L1}


def test_overtake_deduped_keeps_newest_load_per_overtake(spark):
    df = spark.createDataFrame(
        [raw_overtake("16", load_id=L1), raw_overtake("16", load_id=L2), raw_overtake("1", load_id=L1)],
        RAW_OVERTAKE_SCHEMA,
    )

    result = overtake_deduped(df)

    assert {row["overtaken_driver_number"]: row["_load_id"] for row in result.collect()} == {"16": L2, "1": L1}


def test_overtake_enriched_builds_full_row(spark):
    df = spark.createDataFrame([raw_overtake()], RAW_OVERTAKE_SCHEMA)

    row = overtake_enriched(df).first().asDict()

    assert row == {
        "openf1_session_key": 9839,
        "overtake_ts": at(13, 30),
        "overtaking_driver_number": 44,
        "overtaken_driver_number": 16,
        "position": 3,
        "ingested_at": INGESTED,
        "_load_id": L1,
    }


def test_pit_times_convert_durations_to_ms(spark):
    df = spark.createDataFrame(
        [
            raw_pit(lap="10", lane="20.5", pit=None, stop="2.4"),
            raw_pit(lap="20", lane=None, pit="21.34", stop=None),
            raw_pit(lap="30", lane=None, pit=None, stop=None),
        ],
        RAW_PIT_SCHEMA,
    )

    rows = {
        row["lap_number"]: (row["pit_entry_utc"], row["pit_lane_duration_ms"], row["stop_duration_ms"])
        for row in _pit_times(df).collect()
    }

    assert rows == {
        10: (at(13, 20), 20500, 2400),
        20: (at(13, 20), 21340, None),
        30: (at(13, 20), None, None),
    }

def tyres_for(spark, pits, stints):
    result = _pit_tyres(spark.createDataFrame(pits, PIT_KEYS_SCHEMA), spark.createDataFrame(stints, STINTS_SCHEMA))
    return [(row["lap_number"], row["compound_removed"], row["compound_fitted"], row["is_tyre_change"]) for row in result.collect()]


def test_pit_tyres_find_new_stint_on_next_lap(spark):
    result = tyres_for(
        spark,
        [(9839, 44, 20)],
        [(9839, 44, 1, 1, 20, "SOFT", 0), (9839, 44, 2, 21, 40, "HARD", 0)],
    )

    assert result == [(20, "SOFT", "HARD", True)]


def test_pit_tyres_find_new_stint_on_same_lap(spark):
    result = tyres_for(
        spark,
        [(9839, 44, 20)],
        [(9839, 44, 1, 1, 20, "SOFT", 0), (9839, 44, 2, 20, 40, "MEDIUM", 0)],
    )

    assert result == [(20, "SOFT", "MEDIUM", True)]


def test_pit_tyres_without_new_stint_change_nothing(spark):
    result = tyres_for(spark, [(9839, 44, 20)], [(9839, 44, 1, 1, 40, "SOFT", 0)])

    assert result == [(20, None, None, False)]


def test_pit_tyres_skip_empty_stint_but_remove_its_tyre(spark):
    result = tyres_for(
        spark,
        [(9839, 44, 20)],
        [
            (9839, 44, 1, 1, 20, "SOFT", 0),
            (9839, 44, 2, 21, 20, "MEDIUM", 0),
            (9839, 44, 3, 21, 40, "HARD", 0),
        ],
    )

    assert result == [(20, "MEDIUM", "HARD", True)]


def penalty_laps(spark, events, visits):
    result = _penalty_visits(
        spark.createDataFrame(events, RACE_CONTROL_SCHEMA), spark.createDataFrame(visits, VISITS_SCHEMA)
    )
    return sorted(row["lap_number"] for row in result.collect())


VISITS = [
    (9839, 44, 10, at(13, 0)),
    (9839, 44, 18, at(13, 15)),
    (9839, 44, 30, at(13, 40)),
]


def test_penalty_goes_to_last_visit_before_message(spark):
    result = penalty_laps(spark, [(9839, at(13, 20), "PENALTY_SERVED", 44, "CAR 44 PENALTY SERVED")], VISITS)

    assert result == [18]


def test_penalty_driver_number_is_read_from_message(spark):
    events = [(9839, at(13, 20), "PENALTY_SERVED", None, "TIME PENALTY FOR CAR 44 (HAM) - PENALTY SERVED")]

    assert penalty_laps(spark, events, VISITS) == [18]


def test_penalty_without_earlier_visit_or_driver_is_ignored(spark):
    events = [
        (9839, at(12, 50), "PENALTY_SERVED", 44, "CAR 44 PENALTY SERVED"),
        (9839, at(13, 20), "PENALTY_SERVED", None, "PENALTY SERVED"),
        (9839, at(13, 20), "OTHER", 44, "CAR 44 TRACK LIMITS"),
    ]

    assert penalty_laps(spark, events, VISITS) == []


def test_pit_positions_take_lap_before_and_after_visit(spark):
    pits = spark.createDataFrame([(9839, 44, 20), (9839, 44, 2)], PIT_KEYS_SCHEMA)
    laps = spark.createDataFrame(
        [(9839, 44, 19, 5), (9839, 44, 20, 5), (9839, 44, 21, 8), (9839, 44, 2, 3)],
        LAP_POSITIONS_SCHEMA,
    )

    result = _pit_positions(pits, laps)

    rows = {row["lap_number"]: (row["position_before"], row["position_after"]) for row in result.collect()}
    assert rows == {20: (5, 8), 2: (None, None)} 


def test_pit_enriched_builds_full_row(spark):
    df = spark.createDataFrame([raw_pit(lap="20", lane="20.5", stop="2.4")], RAW_PIT_SCHEMA)
    stints = spark.createDataFrame(
        [(9839, 44, 1, 1, 20, "SOFT", 0), (9839, 44, 2, 21, 40, "HARD", 0)], STINTS_SCHEMA
    )
    laps = spark.createDataFrame([(9839, 44, 19, 5), (9839, 44, 21, 8)], LAP_POSITIONS_SCHEMA)
    race_control = spark.createDataFrame([], RACE_CONTROL_SCHEMA)
    session_driver = spark.createDataFrame([(9839, 44, "hamilton", "ferrari")], SESSION_DRIVERS_SCHEMA)
    intervals = spark.createDataFrame([(9839, at(13, 20, 10), at(13, 25), 1)], INTERVALS_SCHEMA)

    row = pit_enriched(df, stints, laps, race_control, session_driver, intervals).first().asDict()

    assert row == {
        "openf1_session_key": 9839,
        "driver_number": 44,
        "lap_number": 20,
        "driver_id": "hamilton",
        "constructor_id": "ferrari",
        "pit_entry_utc": at(13, 20),
        "pit_lane_duration_ms": 20500,
        "stop_duration_ms": 2400,
        "compound_removed": "SOFT",
        "compound_fitted": "HARD",
        "is_tyre_change": True,
        "is_penalty_served": False,
        "track_status_name": "Virtual Safety Car",
        "position_before": 5,
        "position_after": 8,
        "positions_lost": 3,
        "ingested_at": INGESTED,
        "_load_id": L1,
    }