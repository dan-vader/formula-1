from pyspark.sql import DataFrame, Window, functions as F

from formula_1.gold.config import silver
from formula_1.gold.surrogate import date_sk, session_sk, sk
from formula_1.gold.transforms.dim_team import lookup_team_sk


def _overtakes() -> DataFrame:
    overtakes = silver("silver_overtake")
    covered = overtakes.select("openf1_session_key").distinct()
    per_driver = overtakes.groupBy(
        "openf1_session_key", F.col("overtaking_driver_number").alias("driver_number")
    ).agg(F.count(F.lit(1)).alias("_overtakes"))
    return (
        silver("silver_session_driver")
        .where(F.col("driver_id").isNotNull())
        .join(covered, "openf1_session_key")
        .join(per_driver, ["openf1_session_key", "driver_number"], "left")
        .groupBy("session_id", "driver_id")
        .agg(F.sum(F.coalesce("_overtakes", F.lit(0))).cast("int").alias("overtakes_made"))
    )


def fact_race_classification(dim_team: DataFrame) -> DataFrame:
    sessions = silver("silver_session").select("session_id", "session_date_local")
    by_version = Window.partitionBy("session_id", "driver_id").orderBy("snapshot_ts")
    previous_points = F.lag("points").over(by_version)
    versions = (
        silver("silver_race_result")
        .join(sessions, "session_id", "left")
        .join(_overtakes(), ["session_id", "driver_id"], "left")
        .withColumn("classification_version", F.row_number().over(by_version))
        .withColumn("points_change", F.coalesce(F.col("points") - previous_points, F.col("points")))
    )
    return lookup_team_sk(versions, dim_team, "session_date_local").select(
        session_sk("session_id").alias("session_sk"),
        sk("driver_id").alias("driver_sk"),
        "classification_version",
        date_sk("session_date_local").alias("session_date_sk"),
        date_sk("snapshot_ts").alias("classification_date_sk"),
        sk("status_id").alias("finish_status_sk"),
        "team_sk",
        "grid_position",
        "classified_position",
        F.col("points_change").cast("decimal(5,2)").alias("points_change"),
        "points",
        "overtakes_made",
    )