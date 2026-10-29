from datetime import datetime

from pyspark.sql import functions as F

from formula_1.gold.surrogate import UNKNOWN_SK, sk
from formula_1.gold.transforms import dim_driver as dim_driver_module
from formula_1.gold.transforms.dim_driver import dim_driver

DRIVER_SCHEMA = "driver_id string, full_name string, name_acronym string"
RESULT_SCHEMA = "session_id string, driver_id string, snapshot_ts timestamp"
T1 = datetime(2025, 12, 7, 15, 0)
T2 = datetime(2025, 12, 8, 9, 0)
HAMILTON = ("hamilton", "Lewis Hamilton", "HAM")


def build(spark, use_silver, drivers, results):
    use_silver(dim_driver_module, {
        "silver_driver": spark.createDataFrame(drivers, DRIVER_SCHEMA),
        "silver_race_result": spark.createDataFrame(results, RESULT_SCHEMA),
    })
    return dim_driver()


def by_driver(dim):
    rows = dim.where(F.col("driver_sk") != UNKNOWN_SK).collect()
    return {row["driver_id"]: row.asDict() for row in rows}


def test_known_driver_is_kept_as_is(spark, use_silver):
    dim = build(spark, use_silver, [HAMILTON], [])

    row = by_driver(dim)["hamilton"]

    assert (row["full_name"], row["name_acronym"]) == ("Lewis Hamilton", "HAM")
    assert row["is_inferred"] is False
    assert row["inferred_at"] is None


def test_driver_only_in_results_is_added_as_inferred_at_first_sighting(spark, use_silver):
    results = [("2025-24-R", "colapinto", T2), ("2025-23-R", "colapinto", T1)]

    row = by_driver(build(spark, use_silver, [HAMILTON], results))["colapinto"]

    assert row["is_inferred"] is True
    assert row["inferred_at"] == T1
    assert row["full_name"] == "Unknown"


def test_known_driver_in_results_is_not_duplicated(spark, use_silver):
    dim = build(spark, use_silver, [HAMILTON], [("2025-24-R", "hamilton", T1)])

    assert dim.count() == 2
    assert by_driver(dim)["hamilton"]["is_inferred"] is False


def test_result_without_driver_adds_nothing(spark, use_silver):
    dim = build(spark, use_silver, [HAMILTON], [("2025-24-R", None, T1)])

    assert dim.count() == 2


def test_every_driver_in_results_finds_row_in_dim_driver(spark, use_silver):
    results = [("2025-24-R", "hamilton", T1), ("2025-24-R", "colapinto", T1)]
    dim = build(spark, use_silver, [HAMILTON], results)

    fact_keys = spark.createDataFrame(results, RESULT_SCHEMA).select(sk("driver_id").alias("driver_sk"))

    assert fact_keys.join(dim, "driver_sk", "left_anti").count() == 0


def test_driver_keys_are_unique_and_unknown_is_not_inferred(spark, use_silver):
    dim = build(spark, use_silver, [HAMILTON], [("2025-24-R", "colapinto", T1)])

    unknown = dim.where(F.col("driver_sk") == UNKNOWN_SK).first()

    assert dim.select("driver_sk").distinct().count() == dim.count()
    assert unknown["is_inferred"] is False