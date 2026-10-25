from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window


def dedup_latest(df, keys, order_by=("_load_id", "_ingested_at")) -> DataFrame:
    w = Window.partitionBy(*keys).orderBy(*[F.col(col).desc_nulls_last() for col in order_by])
    return (
        df.withColumn("_rn", F.row_number().over(w))
            .where("_rn = 1")
            .drop("_rn")
    )


def dedup_batch_over_stream(df, keys) -> DataFrame:
    w = Window.partitionBy(*keys).orderBy(
        F.col("_is_batch").desc(),
        F.col("_load_id").desc_nulls_last(),
        F.col("_ingested_at").desc_nulls_last()
    )
    return (
        df.withColumn("_rn", F.row_number().over(w))
            .where("_rn = 1")
            .drop("_rn")
    )

