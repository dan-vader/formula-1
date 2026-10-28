from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import functions as F

from formula_1.gold.surrogate import sk
from formula_1.gold.transforms import fact_race_classification as module
from formula_1.gold.transforms.fact_race_classification import fact_race_classification

RACE_DAY = datetime(2025, 12, 7, 16, 0)
NEXT_DAY = datetime(2025, 12, 8, 10, 0)
LATER = datetime(2025, 12, 10, 10, 0)

SESSION_SCHEMA = "session_id string, session_date_local date"
SESSIONS = [("2025-24-R", date(2025, 12, 7)), ("2025-23-R", date(2025, 11, 30))]

SESSION_DRIVER_SCHEMA = "openf1_session_key int, driver_number int, session_id string, driver_id string"
SESSION_DRIVERS = [
    (9839, 4, "2025-24-R", "norris"),
    (9839, 1, "2025-24-R", "verstappen"),
    (9838, 4, "2025-23-R", "norris"),
]

OVERTAKE_SCHEMA = "openf1_session_key int, overtaking_driver_number int"
OVERTAKES = [(9839, 4), (9839, 4)]

DIM_TEAM_SCHEMA = "team_sk bigint, constructor_id string, effective_from date, effective_to date"
TEAMS = [
    (102, "mclaren", date(2025, 1, 1), date(9999, 12, 31)),
    (201, "red_bull", date(2025, 1, 1), date(9999, 12, 31)),
]

RESULT_SCHEMA = (
    "session_id string, driver_id string, snapshot_ts timestamp, status_id int, "
    "constructor_id string, grid_position int, classified_position int, points decimal(5,2)"
)


def result(driver_id="norris", snapshot=RACE_DAY, points="25", position=1,
           session_id="2025-24-R", constructor="mclaren"):
    return (session_id, driver_id, snapshot, 1, constructor, 2, position, Decimal(points))


def key(spark, column):
    return spark.range(1).select(column.alias("k")).first()["k"]


def build(spark, use_silver, results):
    use_silver(module, {
        "silver_race_result": spark.createDataFrame(results, RESULT_SCHEMA),
        "silver_session": spark.createDataFrame(SESSIONS, SESSION_SCHEMA),
        "silver_session_driver": spark.createDataFrame(SESSION_DRIVERS, SESSION_DRIVER_SCHEMA),
        "silver_overtake": spark.createDataFrame(OVERTAKES, OVERTAKE_SCHEMA),
    })
    return fact_race_classification(spark.createDataFrame(TEAMS, DIM_TEAM_SCHEMA))


def versions_of(spark, fact, driver_id):
    driver_sk = key(spark, sk(F.lit(driver_id)))
    rows = fact.where(F.col("driver_sk") == driver_sk).collect()
    return sorted(rows, key=lambda row: row["classification_version"])


def test_first_result_gets_all_keys_and_full_points_as_change(spark, use_silver):
    row = build(spark, use_silver, [result(snapshot=NEXT_DAY)]).first().asDict()

    assert row == {
        "session_sk": 2025242,
        "driver_sk": key(spark, sk(F.lit("norris"))),
        "classification_version": 1,
        "session_date_sk": 20251207,
        "classification_date_sk": 20251208,
        "finish_status_sk": key(spark, sk(F.lit(1))),
        "team_sk": 102,
        "grid_position": 2,
        "classified_position": 1,
        "points_change": Decimal(25),
        "points": Decimal(25),
        "overtakes_made": 2,
    }


def test_penalty_after_race_creates_second_version_with_negative_change(spark, use_silver):
    results = [
        result(snapshot=NEXT_DAY, points="18", position=2),
        result(snapshot=RACE_DAY, points="25", position=1),
    ]

    rows = versions_of(spark, build(spark, use_silver, results), "norris")

    summary = [(r["classification_version"], r["classified_position"], r["points"], r["points_change"]) for r in rows]
    assert summary == [
        (1, 1, Decimal(25), Decimal(25)),
        (2, 2, Decimal(18), Decimal(-7)),
    ]


def test_sum_of_points_changes_equals_final_points(spark, use_silver):
    results = [
        result(snapshot=RACE_DAY, points="25"),
        result(snapshot=NEXT_DAY, points="18"),
        result(snapshot=LATER, points="19"),
    ]

    rows = versions_of(spark, build(spark, use_silver, results), "norris")

    assert sum(r["points_change"] for r in rows) == rows[-1]["points"] == Decimal(19)


def test_versions_are_counted_per_driver_in_each_race(spark, use_silver):
    results = [result("norris"), result("verstappen", constructor="red_bull", points="18", position=2)]

    fact = build(spark, use_silver, results)

    assert [r["classification_version"] for r in fact.collect()] == [1, 1]


def test_overtakes_are_zero_for_driver_without_them_and_empty_without_data(spark, use_silver):
    results = [
        result("norris"),
        result("verstappen", constructor="red_bull", points="18", position=2),
        result("norris", session_id="2025-23-R"),
    ]

    fact = build(spark, use_silver, results)

    norris, verstappen = key(spark, sk(F.lit("norris"))), key(spark, sk(F.lit("verstappen")))
    overtakes = {(r["session_sk"], r["driver_sk"]): r["overtakes_made"] for r in fact.collect()}
    assert overtakes == {
        (2025242, norris): 2,
        (2025242, verstappen): 0,
        (2025232, norris): None,
    }


def test_race_driver_version_is_unique(spark, use_silver):
    results = [
        result(snapshot=RACE_DAY),
        result(snapshot=NEXT_DAY, points="18"),
        result("verstappen", constructor="red_bull"),
        result("norris", session_id="2025-23-R"),
    ]

    fact = build(spark, use_silver, results)
    grain = fact.select("session_sk", "driver_sk", "classification_version")

    assert grain.distinct().count() == fact.count()