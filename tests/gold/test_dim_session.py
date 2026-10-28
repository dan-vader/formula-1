from datetime import datetime

import pytest
from pyspark.sql import functions as F

from formula_1.gold.surrogate import UNKNOWN_SK, session_sk
from formula_1.gold.transforms import dim_session as dim_session_module
from formula_1.gold.transforms.dim_session import dim_session

SESSION_SCHEMA = (
    "session_id string, openf1_session_key int, season int, round int, session_type string, "
    "session_start_utc timestamp, event_name string, circuit_name string, circuit_type string, "
    "regulation_era string"
)
RESULT_SCHEMA = "session_id string, driver_id string, snapshot_ts timestamp"
T1 = datetime(2025, 12, 7, 15, 0)
ABU_DHABI = (
    "2025-24-R", 9839, 2025, 24, "Race", datetime(2025, 12, 7, 13, 0),
    "Abu Dhabi Grand Prix", "Yas Marina", "permanent", "ground_effect",
)


def build(spark, use_silver, sessions, results):
    use_silver(dim_session_module, {
        "silver_session": spark.createDataFrame(sessions, SESSION_SCHEMA),
        "silver_race_result": spark.createDataFrame(results, RESULT_SCHEMA),
    })
    return dim_session()


def by_key(dim):
    rows = dim.where(F.col("session_sk") != UNKNOWN_SK).collect()
    return {row["session_sk"]: row.asDict() for row in rows}


def test_known_session_keeps_its_details(spark, use_silver):
    row = by_key(build(spark, use_silver, [ABU_DHABI], []))[2025242]

    assert (row["season"], row["round"], row["session_type"]) == (2025, 24, "Race")
    assert row["event_name"] == "Abu Dhabi Grand Prix"
    assert row["is_inferred"] is False


@pytest.mark.parametrize(
    ("session_id", "key", "season", "round_number", "session_type"),
    [
        ("2025-23-S", 2025231, 2025, 23, "Sprint"),
        ("2024-05-R", 2024052, 2024, 5, "Race"),
    ],
)
def test_session_only_in_results_is_inferred_from_its_id(
    spark, use_silver, session_id, key, season, round_number, session_type
):
    dim = build(spark, use_silver, [ABU_DHABI], [(session_id, "hamilton", T1)])

    row = by_key(dim)[key]

    assert (row["season"], row["round"], row["session_type"]) == (season, round_number, session_type)
    assert row["event_name"] == "Unknown"
    assert row["is_inferred"] is True
    assert row["inferred_at"] == T1


def test_session_id_is_replaced_by_session_sk(spark, use_silver):
    dim = build(spark, use_silver, [ABU_DHABI], [])

    assert "session_sk" in dim.columns
    assert "session_id" not in dim.columns


def test_every_session_in_results_finds_row_in_dim_session(spark, use_silver):
    results = [("2025-24-R", "hamilton", T1), ("2025-23-S", "hamilton", T1)]
    dim = build(spark, use_silver, [ABU_DHABI], results)

    fact_keys = spark.createDataFrame(results, RESULT_SCHEMA).select(
        session_sk("session_id").alias("session_sk")
    )

    assert fact_keys.join(dim, "session_sk", "left_anti").count() == 0


def test_session_keys_are_unique(spark, use_silver):
    dim = build(spark, use_silver, [ABU_DHABI], [("2025-23-S", "hamilton", T1)])

    assert dim.select("session_sk").distinct().count() == dim.count()