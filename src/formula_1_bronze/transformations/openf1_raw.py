from pyspark import pipelines as dp
from pyspark.sql import functions as F

from formula_1.bronze import openf1

LANDING_PATH = spark.conf.get("formula_1.landing_path")


def define_bronze_table(folder, table):
    @dp.table(
        name=table,
        comment=f"Raw OpenF1 responses from '{folder}', one row per JSON file",
        # Append-only history: a full refresh would wipe and rebuild it
        table_properties={"pipelines.reset.allowed": "false"},
    )
    def bronze():
        # Directory listing: the managed volume has no external location with file events
        raw = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "text")
            .option("wholetext", "true")
            .option("cloudFiles.partitionColumns", "")
            .load(f"{LANDING_PATH}/openf1/{folder}/")
        )
        return (
            raw.select(
                F.col("value").alias("payload"),
                F.col("_metadata.file_path").alias("_source_file_name"),
            )
            .withColumn("_source", F.lit("openf1"))
            .withColumn("_endpoint", F.lit(folder))
            .withColumn("_load_id", F.regexp_extract("_source_file_name", r"load_id=([^/]+)", 1))
            .withColumn(
                "_year",
                F.expr("try_cast(regexp_extract(_source_file_name, 'year=([0-9]+)', 1) as int)"),
            )
            .withColumn(
                "_session_key",  # -1 for endpoints that are not per session (sessions, meetings)
                F.expr("coalesce(try_cast(regexp_extract(_source_file_name, 'session_key=(-?[0-9]+)', 1) as int), -1)"),
            )
            .withColumn("_ingested_at", F.current_timestamp())
        )


for folder, table in openf1.landing_tables().items():
    define_bronze_table(folder, table)
