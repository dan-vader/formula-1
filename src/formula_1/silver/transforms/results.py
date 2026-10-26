from pyspark.sql import DataFrame, Window, functions as F

from formula_1.silver import config
from formula_1.silver.schemas import jolpica as J
from formula_1.silver.transforms import dedup, snapshot
from formula_1.silver.transforms.parse import _try_cast
from formula_1.silver.transforms.session import session_id

LOAD_ID_FORMAT = "yyyyMMdd'T'HHmmss'Z'"
LATE_CHANGE_DAYS = 14


# One row per result record of every complete snapshot
def _rows(table: str, list_field: str, session_type: str) -> DataFrame:
    snap = snapshot.all_complete_jolpica(config.bronze(table))
    p = F.from_json("payload", J.RACE_RESULTS)
    races = snap.select(*config.JOLPICA_TECH, F.explode(p["MRData"]["RaceTable"]["Races"]).alias("race"))
    r = races.select(*config.JOLPICA_TECH, "race", F.explode(f"race.{list_field}").alias("res"))

    grid = _try_cast("res.grid", "int")
    return r.select(
        session_id(F.col("race.season"), F.col("race.round"), F.lit(session_type)).alias("session_id"),
        F.lower(F.trim("res.Driver.driverId")).alias("driver_id"),
        F.to_timestamp("_load_id", LOAD_ID_FORMAT).alias("snapshot_ts"),
        F.lower(F.trim("res.Constructor.constructorId")).alias("constructor_id"),
        _try_cast("res.number", "int").alias("car_number"),
        F.when(grid == 0, F.lit(None)).otherwise(grid).alias("grid_position"),  # 0 = pit lane start
        grid.isNotNull().alias("_chk_grid_valid"),
        F.col("res.positionText").alias("position_text"),
        F.when(F.col("res.positionText").rlike("^[0-9]+$"),
               _try_cast("res.position", "int")).alias("classified_position"),
        _try_cast("res.points", "decimal(5,2)").alias("points"),
        F.trim("res.status").alias("status"),
        _try_cast("res.laps", "int").alias("laps_completed"),
        F.sha2(F.concat_ws("|", "res.position", "res.positionText", "res.points", "res.grid", "res.laps",
                "res.status", "res.Constructor.constructorId"), 256).alias("result_hash"),
        "_ingested_at",
        "_load_id"
    )


# Every complete snapshot is a version candidate
def race_result_parsed() -> DataFrame:
    return (
        _rows("jolpica_race_results_raw", "Results", "Race")
        .unionByName(_rows("jolpica_sprint_results_raw", "SprintResults", "Sprint"))
    )


# Dedup and keep the first snapshot and every snapshot that differs from the previous one
def race_result_deduped(df: DataFrame) -> DataFrame:
    d = dedup.dedup_latest(df, ["session_id", "driver_id", "snapshot_ts"], order_by=("_ingested_at",))
    w = Window.partitionBy("session_id", "driver_id").orderBy("snapshot_ts")
    d = d.withColumn("_prev_hash", F.lag("result_hash").over(w))
    d = d.withColumn("_is_first", F.col("_prev_hash").isNull())
    return d.where(F.col("_is_first") | (F.col("_prev_hash") != F.col("result_hash")))


def race_result_enriched(df: DataFrame, session: DataFrame, finish_status: DataFrame) -> DataFrame:
    s = session.select("session_id", "openf1_session_key", "session_date_local")
    f = finish_status.select("status_id", F.col("status").alias("_status_text"))
    j = df.join(s, "session_id", "left").join(f, df["status"] == f["_status_text"], "left")
    late = (~F.col("_is_first")) & (F.to_date("snapshot_ts") > F.date_add("session_date_local", LATE_CHANGE_DAYS))
    return j.select(
        "session_id",
        "driver_id",
        "snapshot_ts",
        "openf1_session_key",
        "constructor_id",
        "car_number",
        "grid_position",
        "position_text",
        "classified_position",
        "points",
        "status_id",
        "laps_completed",
        "result_hash",
        F.coalesce(late, F.lit(False)).alias("is_late_change"),
        F.col("_ingested_at").alias("ingested_at"),
        "_chk_grid_valid",
        "_load_id"
    )
