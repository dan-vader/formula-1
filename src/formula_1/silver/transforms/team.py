from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from formula_1.silver import config
from formula_1.silver.schemas import jolpica as J
from formula_1.silver.transforms import dedup, snapshot
from formula_1.silver.transforms.parse import _try_cast


def team_parsed() -> DataFrame:
    snap = snapshot.latest_complete_jolpica(config.bronze("jolpica_constructors_raw"))
    p = F.from_json("payload", J.CONSTRUCTORS)
    return snap.select(
        *config.JOLPICA_TECH,
        F.explode(p["MRData"]["ConstructorTable"]["Constructors"]).alias("c"),
    )


def team_deduped(df: DataFrame) -> DataFrame:
    keyed = df.withColumn("constructor_id", F.lower(F.trim("c.constructorId"))).withColumn(
        "season", _try_cast("_season", "int")
    )
    return dedup.dedup_latest(keyed, ["constructor_id", "season"])


def _supplier() -> DataFrame:
    pu = config.bronze(config.CSV_PU_SUPPLIER).select(
        F.lower(F.trim("constructor_id")).alias("constructor_id"),
        _try_cast("season", "int").alias("season"),
        F.trim("power_unit_supplier").alias("power_unit_supplier"),
        "_ingested_at",
    )
    return snapshot.csv_latest(pu, ["constructor_id", "season"]).select(
        "constructor_id", "season", "power_unit_supplier"
    )


def _team_lineage() -> DataFrame:
    lineage = config.bronze(config.CSV_TEAM_LINEAGE).select(
        F.lower(F.trim("constructor_id")).alias("constructor_id"),
        F.trim("team_lineage").alias("team_lineage"),
        "_ingested_at",
    )
    return snapshot.csv_latest(lineage, ["constructor_id"]).select("constructor_id", "team_lineage")


def team_enriched(df: DataFrame) -> DataFrame:
    return (
        df.join(_supplier(), ["constructor_id", "season"], "left")
        .join(_team_lineage(), "constructor_id", "left")
        .select(
            "constructor_id",
            "season",
            "power_unit_supplier",
            F.trim("c.name").alias("team_name"),
            "team_lineage",
            F.lit(None).cast("string").alias("team_colour"),  # TODO feat/silver-sessions-results
            F.col("_ingested_at").alias("ingested_at"),
            "_load_id",
        )
    )