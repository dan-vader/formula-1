from pyspark.sql import DataFrame, functions as F

from formula_1.gold.config import silver
from formula_1.gold.surrogate import sk, with_inferred_members, with_unknown_member


def dim_driver() -> DataFrame:
    known = silver("silver_driver").select(
        "driver_id",
        "full_name",
        "name_acronym",
        F.lit(False).alias("is_inferred"),
        F.lit(None).cast("timestamp").alias("inferred_at"),
    )
    referenced = silver("silver_race_result").groupBy("driver_id").agg(
        F.min("snapshot_ts").alias("inferred_at")
    )
    dim = with_inferred_members(known, referenced, "driver_id")
    return with_unknown_member(dim.select(sk("driver_id").alias("driver_sk"), "*"), "driver_sk")