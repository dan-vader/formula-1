from pyspark.sql import DataFrame, Window, functions as F

from formula_1.gold.config import silver
from formula_1.gold.surrogate import UNKNOWN_SK, sk, with_unknown_member

OPEN_END = "9999-12-31"


def _latest_attributes(seasons: DataFrame) -> DataFrame:
    latest = Window.partitionBy("constructor_id").orderBy(F.col("season").desc())
    return (
        seasons.withColumn("_rn", F.row_number().over(latest))
        .where("_rn = 1")
        .select("constructor_id", "team_name", "team_lineage", "team_colour")
    )


def _versions(seasons: DataFrame) -> DataFrame:
    by_season = Window.partitionBy("constructor_id").orderBy("season")
    changed = ~F.col("power_unit_supplier").eqNullSafe(F.lag("power_unit_supplier").over(by_season))
    return (
        seasons.withColumn("_version", F.sum(changed.cast("int")).over(by_season))
        .groupBy("constructor_id", "_version", "power_unit_supplier")
        .agg(F.min("season").alias("_first_season"), F.max("season").alias("_last_season"))
    )


def dim_team() -> DataFrame:
    seasons = silver("silver_team")
    by_start = Window.partitionBy("constructor_id").orderBy("effective_from")
    next_from = F.lead("effective_from").over(by_start)
    latest_season = F.max("_last_season").over(Window.partitionBy(F.lit(1)))
    retired_end = F.when(
        F.col("_last_season") < latest_season, F.make_date("_last_season", F.lit(12), F.lit(31))
    )
    dim = (
        _versions(seasons)
        .withColumn("effective_from", F.make_date("_first_season", F.lit(1), F.lit(1)))
        .withColumn(
            "effective_to",
            F.coalesce(F.date_sub(next_from, 1), retired_end, F.to_date(F.lit(OPEN_END))),
        )
        .withColumn("is_current", next_from.isNull() & (F.col("_last_season") == latest_season))
        .join(_latest_attributes(seasons), "constructor_id")
        .select(
            sk("constructor_id", "effective_from").alias("team_sk"),
            "constructor_id",
            "team_name",
            "team_lineage",
            "team_colour",
            "power_unit_supplier",
            "effective_from",
            "effective_to",
            "is_current",
        )
    )
    return with_unknown_member(dim, "team_sk", is_current=False)


def lookup_team_sk(facts: DataFrame, dim_team: DataFrame, date_col: str) -> DataFrame:
    versions = F.broadcast(
        dim_team.where(F.col("team_sk") != UNKNOWN_SK).select(
            "team_sk", F.col("constructor_id").alias("_constructor_id"), "effective_from", "effective_to"
        )
    )
    on = (facts["constructor_id"] == versions["_constructor_id"]) & facts[date_col].between(
        versions["effective_from"], versions["effective_to"]
    )
    return (
        facts.join(versions, on, "left")
        .withColumn("team_sk", F.coalesce("team_sk", F.lit(UNKNOWN_SK)))
        .drop("_constructor_id", "effective_from", "effective_to")
    )