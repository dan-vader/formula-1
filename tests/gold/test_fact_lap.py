from datetime import date

from pyspark.sql import functions as F

from formula_1.gold.surrogate import UNKNOWN_SK, sk
from formula_1.gold.transforms import fact_lap as fact_lap_module
from formula_1.gold.transforms.fact_lap import fact_lap

SESSION_SCHEMA = "openf1_session_key int, session_id string, session_date_local date"
SESSIONS = [(9839, "2025-24-R", date(2025, 12, 7))]

DIM_TEAM_SCHEMA = "team_sk bigint, constructor_id string, effective_from date, effective_to date"
TEAMS = [
    (101, "mclaren", date(2024, 1, 1), date(2024, 12, 31)),
    (102, "mclaren", date(2025, 1, 1), date(9999, 12, 31)),
]

LAP_SCHEMA = (
    "openf1_session_key int, driver_id string, constructor_id string, track_status_name string, "
    "is_pit_in_lap boolean, is_pit_out_lap boolean, compound string, lap_duration_ms bigint, "
    "tyre_age_laps int, position_at_lap_end int, lap_number int, stint_number int"
)


def lap(session_key=9839, driver_id="norris", compound="MEDIUM"):
    return (session_key, driver_id, "mclaren", "Green", False, True, compound, 84512, 3, 1, 17, 2)


def key(spark, column):
    return spark.range(1).select(column.alias("k")).first()["k"]


def build(spark, use_silver, laps):
    use_silver(fact_lap_module, {
        "silver_session": spark.createDataFrame(SESSIONS, SESSION_SCHEMA),
        "silver_lap": spark.createDataFrame(laps, LAP_SCHEMA),
    })
    return fact_lap(spark.createDataFrame(TEAMS, DIM_TEAM_SCHEMA))


def test_lap_gets_all_keys_and_keeps_its_measures(spark, use_silver):
    row = build(spark, use_silver, [lap()]).first().asDict()

    assert row == {
        "session_sk": 2025242,
        "driver_sk": key(spark, sk(F.lit("norris"))),
        "team_sk": 102,
        "session_date_sk": 20251207,
        "track_status_sk": key(spark, sk(F.lit("Green"))),
        "lap_flags_sk": key(spark, sk(F.lit(False), F.lit(True))),
        "tyre_compound_sk": key(spark, sk(F.lit("MEDIUM"))),
        "lap_duration_ms": 84512,
        "tyre_age_laps": 3,
        "position_at_lap_end": 1,
        "lap_number": 17,
        "stint_number": 2,
    }


def test_lap_without_driver_is_dropped(spark, use_silver):
    result = build(spark, use_silver, [lap(), lap(driver_id=None)])

    assert result.count() == 1


def test_lap_of_unknown_session_is_kept_with_unknown_keys(spark, use_silver):
    row = build(spark, use_silver, [lap(session_key=1)]).first()

    assert row["session_sk"] == UNKNOWN_SK
    assert row["session_date_sk"] == UNKNOWN_SK
    assert row["team_sk"] == UNKNOWN_SK


def test_lap_without_compound_points_to_unknown_tyre(spark, use_silver):
    row = build(spark, use_silver, [lap(compound=None)]).first()

    assert row["tyre_compound_sk"] == UNKNOWN_SK