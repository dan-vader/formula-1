from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Column

from formula_1.silver import config
from formula_1.silver.schemas import jolpica as J
from formula_1.silver.transforms import snapshot, dedup
from formula_1.silver.transforms.parse import _try_cast


# silver_finish_status
def finish_status_parsed() -> DataFrame:
    snap = snapshot.latest_complete_jolpica(config.bronze("jolpica_status_raw"))
    payload = F.from_json("payload", J.STATUS)
    return snap.select(
        *config.JOLPICA_TECH,
        F.explode(payload["MRData"]["StatusTable"]["Status"]).alias("status")
    )


def finish_status_deduped(df) -> DataFrame:
    return dedup.dedup_latest(df.withColumn("_k", _try_cast("status.statusId", "int")), ["_k"])


def status_category(status) -> Column:
    s = F.trim(status)
    return (
        F.when(s.isin("Finished", "Lapped"), "Classified")
        .when(s.rlike(r"^\+\d+ Laps?$"), "Classified")
        .when(s.isin("Disqualified", "Excluded"), "Disqualified")
        .when(s == "Did not start", "Did not start")
        .otherwise("Retired")
    )


def finish_status_enriched(df) -> DataFrame:
    return df.select(
        _try_cast("status.statusId", "int").alias("status_id"),
        F.trim("status.status").alias("status"),
        status_category(F.col("status.status")).alias("status_category"),
        F.col("_ingested_at").alias("ingested_at"),
        "_load_id"
    )


# silver_driver
def driver_parsed() -> DataFrame:
    snap = snapshot.latest_complete_jolpica(config.bronze("jolpica_drivers_raw"))
    p = F.from_json("payload", J.DRIVERS)
    return snap.select(
        *config.JOLPICA_TECH,
        F.explode(p["MRData"]["DriverTable"]["Drivers"]).alias("d")
    )


def driver_deduped(df) -> DataFrame:
    driver_df = (
        df.withColumn("_k", F.lower(F.trim("d.driverId")))
        .withColumn("_season_i", _try_cast("_season", "int"))
    )
    return dedup.dedup_latest(driver_df, ["_k"], order_by=("_season_i", "_load_id", "_ingested_at"))


def driver_enriched(df) -> DataFrame:
    return df.select(
        F.lower(F.trim("d.driverId")).alias("driver_id"),
        F.trim("d.givenName").alias("given_name"),
        F.trim("d.familyName").alias("family_name"),
        F.concat_ws(" ", F.trim("d.givenName"), F.trim("d.familyName")).alias("full_name"),
        F.upper(F.trim("d.code")).alias("name_acronym"),
        _try_cast("d.permanentNumber", "int").alias("permanent_number"),
        F.col("_ingested_at").alias("ingested_at"),
        "_load_id",
    )
