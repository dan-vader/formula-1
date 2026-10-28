from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from formula_1.silver import config

SOURCE = "openf1_stream_raw"


def stream_quarantine() -> DataFrame:
    stream = config.bronze_stream()
    envelope_ok = F.coalesce(
        F.col("endpoint").isNotNull()
        & F.col("session_key").isNotNull()
        & F.col("event_time").isNotNull(),
        F.lit(False)
    )
    # Kafka partition and offset are the key when replay_id is missing
    record_key = F.coalesce(
        F.col("replay_id"),
        F.concat_ws("-", F.lit("kafka"),
                    F.col("_kafka_partition").cast("string"),
                    F.col("_kafka_offset").cast("string"))
    )
    failed_rules = F.array(F.lit("R-STR-01"))
    quarantine_id = F.sha2(
        F.concat_ws("|", F.lit(SOURCE), record_key, F.array_join(failed_rules, ",")),
        256
    )

    return stream.where(~envelope_ok).select(
        quarantine_id.alias("quarantine_id"),
        F.lit(SOURCE).alias("source_table"),
        record_key.alias("record_key"),
        failed_rules.alias("failed_rules"),
        F.coalesce("_raw_message", "payload").alias("payload"),
        F.col("replay_id").alias("source_load_id"),
        F.col("_ingested_at").alias("source_ingested_at"),
    )
