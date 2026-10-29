from pyspark.sql import functions as F

from formula_1.gold.surrogate import UNKNOWN_SK, sk
from formula_1.gold.transforms import dim_finish_status as dim_finish_status_module
from formula_1.gold.transforms.dim_finish_status import dim_finish_status

STATUS_SCHEMA = "status_id int, status string, status_category string"
STATUSES = [(1, "Finished", "finished"), (3, "Accident", "dnf"), (11, "+1 Lap", "lapped")]


def build(spark, use_silver):
    use_silver(dim_finish_status_module, {
        "silver_finish_status": spark.createDataFrame(STATUSES, STATUS_SCHEMA),
    })
    return dim_finish_status()


def test_finish_status_has_one_row_per_status_plus_unknown(spark, use_silver):
    dim = build(spark, use_silver)

    known = dim.where(F.col("finish_status_sk") != UNKNOWN_SK).collect()

    assert {row["status_id"]: row["status"] for row in known} == {1: "Finished", 3: "Accident", 11: "+1 Lap"}
    assert dim.count() == 4


def test_result_status_finds_its_row_in_dim_finish_status(spark, use_silver):
    dim = build(spark, use_silver)
    results = spark.createDataFrame([(1,), (3,), (11,)], "status_id int")

    fact_keys = results.select(sk("status_id").alias("finish_status_sk"))

    assert fact_keys.join(dim, "finish_status_sk", "left_anti").count() == 0


def test_finish_status_keys_are_unique(spark, use_silver):
    dim = build(spark, use_silver)

    assert dim.select("finish_status_sk").distinct().count() == dim.count()