from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from formula_1.silver import config
from formula_1.silver.schemas import jolpica as J
from formula_1.silver.transforms import snapshot
from formula_1.silver.transforms.parse import _try_cast


# silver_team
def team_parsed() -> DataFrame:
    return config.bronze(config.CSV_PU_SUPPLIER).select(
        F.lower(F.trim("constructor_id")).alias("constructor_id"),
        _try_cast("season", "int").alias("season"),
        F.trim("power_unit_supplier").alias("power_unit_supplier"),
        "_ingested_at",
    )


def team_deduped(df: DataFrame) -> DataFrame:
    # the CSV has no _load_id: the newest _ingested_at wins per constructor and season
    return snapshot.csv_latest(df, ["constructor_id", "season"])


def _constructor_names() -> DataFrame:
    snap = snapshot.latest_complete_jolpica(config.bronze("jolpica_constructors_raw"))
    p = F.from_json("payload", J.CONSTRUCTORS)
    return (
        snap.select(
            _try_cast("_season", "int").alias("season"),
            F.explode(p["MRData"]["ConstructorTable"]["Constructors"]).alias("c"),
        )
        .select(
            F.lower(F.trim("c.constructorId")).alias("constructor_id"),
            "season",
            F.trim("c.name").alias("team_name"),
        )
        .dropDuplicates(["constructor_id", "season"])
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
        df.join(_constructor_names(), ["constructor_id", "season"], "left")
        .join(_team_lineage(), "constructor_id", "left")
        .select(
            "constructor_id",
            "season",
            "power_unit_supplier",
            "team_name",
            "team_lineage",
            F.lit(None).cast("string").alias("team_colour"), # TODO feat/silver-sessions-results: colour from openf1
            F.col("_ingested_at").alias("ingested_at"),
            F.lit(None).cast("string").alias("_load_id")
        )
    )
