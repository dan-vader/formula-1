from datetime import date, datetime

import pytest
from chispa import assert_df_equality
from pyspark.sql import functions as F

from formula_1.silver.transforms import session as session_module
from formula_1.silver.transforms.session import (
    _casted_drivers,
    _matched_calendar,
    _unique_codes,
    session_deduped,
    session_driver_deduped,
    session_driver_enriched,
    session_driver_filtered,
    session_enriched,
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
OPENF1_DRIVER_SCHEMA = (
    "session_key string, driver_number string, name_acronym string, full_name string, first_name string, "
    "last_name string, team_name string, team_colour string, _ingested_at timestamp, _load_id string"
)
SESSION_IDS_SCHEMA = "openf1_session_key int, session_id string"
CODES_SCHEMA = "driver_id string, jolpica_code string"
OPENF1_SESSION_SCHEMA = (
    "session_key string, session_name string, date_start string, date_end string, meeting_key string, "
    "circuit_short_name string, gmt_offset string, year string, _ingested_at timestamp, _load_id string"
)
SCHEDULE_FULL_SCHEMA = (
    "sched_season int, sched_round int, sched_race_name string, sched_circuit_id string, "
    "sched_type string, sched_date date"
)
MEETINGS_SCHEMA = "meeting_key int, meeting_name string, circuit_type string"
CAR_NUMBERS_SCHEMA = "session_id string, driver_number int, number_driver_id string, constructor_id string"


def assert_rows(spark, result, rows, schema):
    expected = spark.createDataFrame(rows, schema)
    assert_df_equality(result, expected, ignore_row_order=True, ignore_nullable=True)


def use_calendar(spark, monkeypatch, schedule, meetings):
    schedule_df = spark.createDataFrame(schedule, SCHEDULE_FULL_SCHEMA)
    meetings_df = spark.createDataFrame(meetings, MEETINGS_SCHEMA)
    monkeypatch.setattr(session_module, "_schedule_sessions", lambda: schedule_df)
    monkeypatch.setattr(session_module, "_meetings_clean", lambda: meetings_df)

def openf1_driver(
    session_key="9839",
    number="44",
    acronym="HAM",
    full_name="Lewis HAMILTON",
    first_name="Lewis",
    last_name="Hamilton",
    colour="E8002D",
):
    return (session_key, number, acronym, full_name, first_name, last_name, "Ferrari", colour, INGESTED, L1)


def match(spark, sessions, schedule):
    result = _matched_calendar(
        spark.createDataFrame(sessions, SESSIONS_SCHEMA),
        spark.createDataFrame(schedule, SCHEDULE_SCHEMA),
    )
    return {row["openf1_session_key"]: (row["season"], row["round"], row["_chk_schedule_matches"]) for row in result.collect()}


def use_jolpica_drivers(spark, monkeypatch, car_numbers, codes):
    car_numbers_df = spark.createDataFrame(car_numbers, CAR_NUMBERS_SCHEMA)
    codes_df = spark.createDataFrame(codes, CODES_SCHEMA)
    monkeypatch.setattr(session_module, "_car_numbers", lambda: car_numbers_df)
    monkeypatch.setattr(session_module, "_driver_codes", lambda: codes_df)


def race_session(spark):
    return spark.createDataFrame([(9839, "2025-24-R")], SESSION_IDS_SCHEMA)


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

    assert_rows(spark, result, [("1", "Race", "false"), ("2", "Sprint", None)], "session_key string, session_name string, is_cancelled string")


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

    assert_rows(spark, result, [("1", "44", "HAM", L1, INGESTED)], DRIVER_RAW_SCHEMA)


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


def test_casted_drivers_casts_fields_and_adds_session_id(spark):
    drivers = spark.createDataFrame([openf1_driver(acronym=" ham ", colour="e8002d")], OPENF1_DRIVER_SCHEMA)
    session = spark.createDataFrame([(9839, "2025-24-R")], SESSION_IDS_SCHEMA)

    result = _casted_drivers(drivers, session).first().asDict()

    assert result == {
        "openf1_session_key": 9839,
        "driver_number": 44,
        "name_acronym": "HAM",
        "full_name": "Lewis HAMILTON",
        "team_name_openf1": "Ferrari",
        "team_colour": "E8002D",
        "ingested_at": INGESTED,
        "_load_id": L1,
        "session_id": "2025-24-R",
    }

@pytest.mark.parametrize(
    ("colour", "expected"),
    [
        ("E8002D", "E8002D"),
        ("e8002d", "E8002D"),
        ("#E8002D", None),
        ("E800", None),
        ("GGGGGG", None),
        (None, None),
    ],
)
def test_casted_drivers_accepts_only_six_hex_digits_as_colour(spark, colour, expected):
    drivers = spark.createDataFrame([openf1_driver(colour=colour)], OPENF1_DRIVER_SCHEMA)
    session = spark.createDataFrame([], SESSION_IDS_SCHEMA)

    result = _casted_drivers(drivers, session).first()

    assert result["team_colour"] == expected


def test_casted_drivers_builds_full_name_and_keeps_driver_without_session(spark):
    drivers = spark.createDataFrame([openf1_driver(full_name=None)], OPENF1_DRIVER_SCHEMA)
    session = spark.createDataFrame([], SESSION_IDS_SCHEMA)

    result = _casted_drivers(drivers, session).collect()

    assert len(result) == 1
    assert result[0]["full_name"] == "Lewis Hamilton"
    assert result[0]["session_id"] is None

def test_unique_codes_keeps_only_codes_of_exactly_one_driver(spark):
    codes = spark.createDataFrame(
        [
            ("max_verstappen", "VER"),
            ("verstappen", "VER"),
            ("hamilton", "HAM"),
            ("fangio", None),
        ],
        CODES_SCHEMA,
    )

    result = _unique_codes(codes)

    assert {row["name_acronym"]: row["code_driver_id"] for row in result.collect()} == {"HAM": "hamilton"}


def test_session_enriched_builds_full_row_with_local_date(spark, monkeypatch):
    use_calendar(
        spark,
        monkeypatch,
        [(2025, 22, "Las Vegas Grand Prix", "vegas", "Race", date(2025, 11, 22))],
        [(1270, "Las Vegas Grand Prix", "Temporary – Street")],
    )
    df = spark.createDataFrame(
        [
            (
                "9840", "Race", "2025-11-23T04:00:00+00:00", "2025-11-23T06:00:00+00:00", "1270",
                "Las Vegas", "-08:00:00", "2025", INGESTED, L1,
            )
        ],
        OPENF1_SESSION_SCHEMA,
    )

    result = session_enriched(df).first().asDict()

    assert result == {
        "session_id": "2025-22-R",
        "openf1_session_key": 9840,
        "meeting_key": 1270,
        "season": 2025,
        "round": 22,
        "session_type": "Race",
        "event_name": "Las Vegas Grand Prix",
        "circuit_name": "Las Vegas",
        "circuit_type": "Temporary – Street",
        "regulation_era": "Pre-2026",
        "session_start_utc": datetime(2025, 11, 23, 4, 0),
        "session_end_utc": datetime(2025, 11, 23, 6, 0),
        "gmt_offset": "-08:00:00",
        "session_date_local": date(2025, 11, 22),
        "jolpica_circuit_id": "vegas",
        "ingested_at": INGESTED,
        "_chk_schedule_matches": True,
        "_load_id": L1,
    }


def test_session_enriched_uses_calendar_name_and_2026_era(spark, monkeypatch):
    use_calendar(
        spark,
        monkeypatch,
        [(2026, 4, "Bahrain Grand Prix", "bahrain", "Race", date(2026, 4, 12))],
        [],
    )
    df = spark.createDataFrame(
        [
            (
                "11000", "Race", "2026-04-12T15:00:00+00:00", "2026-04-12T17:00:00+00:00", "1300",
                "Sakhir", "03:00:00", "2026", INGESTED, L1,
            )
        ],
        OPENF1_SESSION_SCHEMA,
    )

    result = session_enriched(df).first()

    assert result["session_id"] == "2026-04-R"
    assert result["event_name"] == "Bahrain Grand Prix"
    assert result["regulation_era"] == "2026 regulations"
    assert result["session_date_local"] == date(2026, 4, 12)

def test_session_driver_enriched_matches_by_car_number(spark, monkeypatch):
    use_jolpica_drivers(spark, monkeypatch, [("2025-24-R", 44, "hamilton", "ferrari")], [("hamilton", "HAM")])
    df = spark.createDataFrame([openf1_driver()], OPENF1_DRIVER_SCHEMA)

    result = session_driver_enriched(df, race_session(spark)).first().asDict()

    assert result == {
        "openf1_session_key": 9839,
        "driver_number": 44,
        "session_id": "2025-24-R",
        "driver_id": "hamilton",
        "constructor_id": "ferrari",
        "name_acronym": "HAM",
        "full_name": "Lewis HAMILTON",
        "team_name_openf1": "Ferrari",
        "team_colour": "E8002D",
        "map_method": "car_number",
        "acronym_match": True,
        "ingested_at": INGESTED,
        "_load_id": L1,
    }

def test_session_driver_enriched_falls_back_to_unique_code(spark, monkeypatch):
    use_jolpica_drivers(
        spark,
        monkeypatch,
        [],
        [("hamilton", "HAM"), ("max_verstappen", "VER"), ("verstappen", "VER")],
    )
    df = spark.createDataFrame(
        [openf1_driver(number="44", acronym="HAM"), openf1_driver(number="1", acronym="VER")],
        OPENF1_DRIVER_SCHEMA,
    )

    result = session_driver_enriched(df, race_session(spark))

    rows = {
        row["driver_number"]: (row["driver_id"], row["map_method"], row["constructor_id"])
        for row in result.collect()
    }
    assert rows == {44: ("hamilton", "name_acronym", None), 1: (None, "unmatched", None)}

def test_session_driver_enriched_prefers_car_number_and_flags_acronym_mismatch(spark, monkeypatch):
    use_jolpica_drivers(
        spark,
        monkeypatch,
        [("2025-24-R", 44, "russell", "mercedes")],
        [("hamilton", "HAM"), ("russell", "RUS")],
    )
    df = spark.createDataFrame([openf1_driver(number="44", acronym="HAM")], OPENF1_DRIVER_SCHEMA)

    result = session_driver_enriched(df, race_session(spark)).first()

    assert result["driver_id"] == "russell"
    assert result["map_method"] == "car_number"
    assert result["acronym_match"] is False