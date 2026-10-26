from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window

from formula_1.silver import config


def _select_loads(df: DataFrame, markers: DataFrame, field: str, column: str, newest_only: bool) -> DataFrame:
    listed = F.from_json("payload", f"{field} array<int>").getField(field)
    exploded = markers.select(
        F.col("_load_id").alias("_m_load"),
        F.explode(listed).alias("_v")
    )
    pairs = exploded.select(
        "_m_load",
        F.col("_v").cast("string").alias("_v")
    ).distinct()

    if newest_only:
        pairs = pairs.groupBy("_v").agg(F.max("_m_load").alias("_m_load"))

    on = (df[column].cast("string") == F.col("_v")) & (df["_load_id"] == F.col("_m_load"))
    return df.join(pairs, on, "left_semi")


def _jolpica(df: DataFrame, markers: DataFrame | None, newest_only: bool) -> DataFrame:
    markers = config.bronze(config.JOLPICA_LOADS) if markers is None else markers
    seasons = _select_loads(df, markers, "seasons", "_season", newest_only)
    newest = markers.agg(F.max("_load_id").alias("_newest"))
    no_season = df.where(F.col("_season").isNull() | F.col("_season").isin("", "all"))
    return seasons.unionByName(
        no_season.join(
            newest,
            F.col("_load_id") == F.col("_newest"),
            "left_semi"
        )
    )


def latest_complete_jolpica(df: DataFrame, markers: DataFrame | None = None) -> DataFrame:
    return _jolpica(df, markers, newest_only=True)


# Every complete snapshot of every season (for silver_race_result)
def all_complete_jolpica(df: DataFrame, markers: DataFrame | None = None) -> DataFrame:
    return _jolpica(df, markers, newest_only=False)


def latest_valid_openf1(df: DataFrame, markers: DataFrame | None = None) -> DataFrame:
    markers = config.bronze(config.OPENF1_LOADS) if markers is None else markers
    return _select_loads(df, markers, "years", "_year", newest_only=True)


# CSV reference tables have no _load_id, so the newest _ingested_at wins per key
def csv_latest(df: DataFrame, keys: list[str]) -> DataFrame:
    w = Window.partitionBy(*keys).orderBy(F.col("_ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


# (keys, _load_id) of every load that has all its pages
def complete_jolpica_loads(df: DataFrame, keys: list[str]) -> DataFrame:
    total = F.get_json_object("payload", "$.MRData.total").cast("int")
    limit = F.get_json_object("payload", "$.MRData.limit").cast("int")
    expected = F.greatest(F.lit(1), F.ceil(total / limit))

    per_load = df.groupBy(*keys, "_load_id").agg(
        F.countDistinct("_page_offset").alias("pages"),
        F.max(expected).alias("expected")
    )

    return per_load.where(F.col("pages") == F.col("expected")).select(*keys, "_load_id")
