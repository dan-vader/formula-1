from pyspark.sql import DataFrame, functions as F

from formula_1.gold.config import silver
from formula_1.gold.surrogate import UNKNOWN_LABEL, sk, with_unknown_member


def _known() -> DataFrame:
    return silver("silver_driver").select(
        "driver_id",
        "full_name",
        "name_acronym",
        F.lit(False).alias("is_inferred"),
        F.lit(None).cast("timestamp").alias("inferred_at"),
    )


def _inferred(known: DataFrame) -> DataFrame:
    """Drivers referenced by a race result before silver_driver has them."""
    return (
        silver("silver_race_result")
        .where(F.col("driver_id").isNotNull())
        .groupBy("driver_id")
        .agg(F.min("snapshot_ts").alias("inferred_at"))
        .join(known, "driver_id", "left_anti")
        .select(
            "driver_id",
            F.lit(UNKNOWN_LABEL).alias("full_name"),
            F.lit(UNKNOWN_LABEL).alias("name_acronym"),
            F.lit(True).alias("is_inferred"),
            "inferred_at",
        )
    )


def dim_driver() -> DataFrame:
    known = _known()
    dim = known.unionByName(_inferred(known)).select(sk("driver_id").alias("driver_sk"), "*")
    return with_unknown_member(dim, "driver_sk")