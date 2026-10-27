from pyspark.sql import DataFrame, Window, functions as F

from formula_1.gold.config import silver
from formula_1.gold.surrogate import date_sk, session_sk, sk
from formula_1.gold.transforms.dim_team import lookup_team_sk


def fact_race_classification(dim_team: DataFrame) -> DataFrame:
    sessions = silver("silver_session").select("session_id", "session_date_local")
    by_version = Window.partitionBy("session_id", "driver_id").orderBy("snapshot_ts")
    previous_points = F.lag("points").over(by_version)
    versions = (
        silver("silver_race_result")
        .join(sessions, "session_id", "left")
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
        F.lit(None).cast("int").alias("overtakes_made"),
    )