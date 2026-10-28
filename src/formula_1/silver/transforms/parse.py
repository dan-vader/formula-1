from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from formula_1.silver import config
from formula_1.silver.schemas import jolpica as J


def _try_cast(expr, dtype):
    return F.expr(f"try_cast({expr} AS {dtype})")


def explode_openf1(df, record_ddl) -> DataFrame:
    schema = f"array<struct<{record_ddl}>>"
    parsed = F.from_json("payload", schema)

    with_records = df.select(
        *config.OPENF1_TECH,
        F.explode(parsed).alias("records")
    )
    return with_records.select("records.*", *config.OPENF1_TECH)


def explode_results(df, list_field="Results") -> DataFrame:
    parsed = F.from_json("payload", J.RACE_RESULTS)
    races = parsed["MRData"]["RaceTable"]["Races"]

    one_row_per_race = df.select(
        *config.JOLPICA_TECH,
        F.explode(races).alias("Race")
    )
    return one_row_per_race.select(
        *config.JOLPICA_TECH,
        "Race",
        F.explode(f"Race.{list_field}").alias("Result")
    )


def stream_records(endpoint, record_ddl) -> DataFrame:
    stream = config.bronze_stream()
    stream_good = stream.where(
        (F.col("endpoint") == endpoint)
        & F.col("session_key").isNotNull()
        & F.col("event_time").isNotNull()
    )

    parsed = stream_good.select(
        F.from_json("payload", f"struct<{record_ddl}>").alias("records"),
        F.col("replay_id").alias("_load_id"),
        F.col("_ingested_at"),
        F.year("event_time").alias("_year"),
        F.col("session_key").cast("int").alias("_session_key")
    )
    return parsed.select("records.*", *config.OPENF1_TECH)


def with_stream(batch, endpoint, record_ddl) -> DataFrame:
    stream = stream_records(endpoint, record_ddl)
    return (
        batch.withColumn("_is_batch", F.lit(True))
            .unionByName(stream.withColumn("_is_batch", F.lit(False)))
    )
