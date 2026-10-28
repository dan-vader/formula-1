from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from formula_1.silver import config
from formula_1.silver.transforms import dq

SOURCE = "openf1_stream_raw"


def stream_checked() -> DataFrame:
    stream = config.bronze_stream()
    return dq.apply(stream, SOURCE)


def stream_quarantine(checked) -> DataFrame:
    failed_rules = F.transform("_errors", lambda x: x["name"])
    record_key = F.coalesce(
        F.col("replay_id"),
        F.concat_ws("-", F.lit("kafka"),
                    F.col("_kafka_partition").cast("string"),
                    F.col("_kafka_offset").cast("string"))
    )
    quarantine_id = F.sha2(
        F.concat_ws("|", F.lit(SOURCE), record_key, F.array_join(failed_rules, ",")),
        256
    )
    bad = checked.where(F.col("_errors").isNotNull())

    return bad.select(
        quarantine_id.alias("quarantine_id"),
        F.lit(SOURCE).alias("source_table"),
        record_key.alias("record_key"),
        failed_rules.alias("failed_rules"),
        F.coalesce("_raw_message", "payload").alias("payload"),
        F.col("replay_id").alias("source_load_id"),
        F.col("_ingested_at").alias("source_ingested_at"),
    )
