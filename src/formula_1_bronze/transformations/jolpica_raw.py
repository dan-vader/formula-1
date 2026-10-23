from pyspark import pipelines as dp
from pyspark.sql import functions as F

from formula_1 import jolpica

BRONZE_SCHEMA = spark.conf.get("formula_1.bronze_schema")
LANDING_PATH = spark.conf.get("formula_1.landing_path")


def define_bronze_table(folder, table):
    @dp.table(
        name=f"{BRONZE_SCHEMA}.{table}",
        comment=f"Raw Jolpica pages from '{folder}', one row per JSON file",
    )
    def bronze():
        # Directory listing: the managed volume has no external location with file events
        raw = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "text")
            .option("wholetext", "true")
            .option("cloudFiles.partitionColumns", "")
            .load(f"{LANDING_PATH}/jolpica/{folder}/")
        )
        return (
            raw.select(
                F.col("value").alias("payload"),
                F.col("_metadata.file_path").alias("_source_file_name"),
            )
            .withColumn("_source", F.lit("jolpica"))
            .withColumn("_resource", F.lit(folder))
            .withColumn("_load_id", F.regexp_extract("_source_file_name", r"load_id=([^/]+)", 1))
            .withColumn("_season", F.regexp_extract("_source_file_name", r"season=([^/]+)", 1))
            .withColumn("_round", F.regexp_extract("_source_file_name", r"round=([^/]+)", 1))
            .withColumn(
                "_page_offset",
                F.expr("try_cast(regexp_extract(_source_file_name, 'offset=([0-9]+)[.]json', 1) as int)"),
            )
            .withColumn("_ingested_at", F.current_timestamp())
        )


for folder, table in jolpica.landing_tables().items():
    define_bronze_table(folder, table)
