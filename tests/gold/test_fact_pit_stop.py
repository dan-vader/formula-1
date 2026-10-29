from datetime import date

from pyspark.sql import functions as F

from formula_1.gold.surrogate import UNKNOWN_SK, sk
from formula_1.gold.transforms import fact_pit_stop as fact_pit_stop_module
from formula_1.gold.transforms.fact_pit_stop import fact_pit_stop

SESSION_SCHEMA = "openf1_session_key int, session_id string, session_date_local date"
SESSIONS = [(9839, "2025-24-R", date(2025, 12, 7))]

DIM_TEAM_SCHEMA = "team_sk bigint, constructor_id string, effective_from date, effective_to date"
TEAMS = [(102, "mclaren", date(2025, 1, 1), date(9999, 12, 31))]

PIT_SCHEMA = (
    "openf1_session_key int, driver_id string, constructor_id string, compound_removed string, "
    "compound_fitted string, track_status_name string, is_tyre_change boolean, is_penalty_served boolean, "
    "pit_lane_duration_ms bigint, stop_duration_ms bigint, positions_lost int, lap_number int"
)


def pit(session_key=9839, driver_id="norris", removed="MEDIUM", fitted="HARD", tyre_change=True, penalty=False):
    return (session_key, driver_id, "mclaren", removed, fitted, "Safety Car", tyre_change, penalty, 21500, 2300, 0, 31)


def key(spark, column):
    return spark.range(1).select(column.alias("k")).first()["k"]


def build(spark, use_silver, pits):
    use_silver(fact_pit_stop_module, {
        "silver_session": spark.createDataFrame(SESSIONS, SESSION_SCHEMA),
        "silver_pit_stop": spark.createDataFrame(pits, PIT_SCHEMA),
    })
    return fact_pit_stop(spark.createDataFrame(TEAMS, DIM_TEAM_SCHEMA))


def test_pit_stop_gets_all_keys_and_keeps_its_measures(spark, use_silver):
    row = build(spark, use_silver, [pit()]).first().asDict()

    assert row == {
        "session_sk": 2025242,
        "driver_sk": key(spark, sk(F.lit("norris"))),
        "team_sk": 102,
        "session_date_sk": 20251207,
        "tyre_compound_removed_sk": key(spark, sk(F.lit("MEDIUM"))),
        "tyre_compound_fitted_sk": key(spark, sk(F.lit("HARD"))),
        "track_status_sk": key(spark, sk(F.lit("Safety Car"))),
        "pit_stop_flags_sk": key(spark, sk(F.lit(True), F.lit(False))),
        "pit_lane_duration_ms": 21500,
        "stop_duration_ms": 2300,
        "positions_lost": 0,
        "lap_number": 31,
    }


def test_penalty_drive_through_has_unknown_tyres_and_penalty_flag(spark, use_silver):
    row = build(
        spark, use_silver, [pit(removed=None, fitted=None, tyre_change=False, penalty=True)]
    ).first()

    assert row["tyre_compound_removed_sk"] == UNKNOWN_SK
    assert row["tyre_compound_fitted_sk"] == UNKNOWN_SK
    assert row["pit_stop_flags_sk"] == key(spark, sk(F.lit(False), F.lit(True)))


def test_pit_stop_without_driver_is_dropped(spark, use_silver):
    result = build(spark, use_silver, [pit(), pit(driver_id=None)])

    assert result.count() == 1


def test_pit_stop_of_unknown_session_is_kept_with_unknown_keys(spark, use_silver):
    row = build(spark, use_silver, [pit(session_key=1)]).first()

    assert row["session_sk"] == UNKNOWN_SK
    assert row["session_date_sk"] == UNKNOWN_SK
    assert row["team_sk"] == UNKNOWN_SK