from pyspark.sql import DataFrame

from formula_1.gold.config import silver
from formula_1.gold.surrogate import sk, with_unknown_member


def dim_finish_status() -> DataFrame:
    dim = silver("silver_finish_status").select(
        sk("status_id").alias("finish_status_sk"),
        "status_id",
        "status",
        "status_category",
    )
    return with_unknown_member(dim, "finish_status_sk")