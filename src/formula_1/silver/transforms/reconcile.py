from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window

from formula_1.silver.transforms import snapshot


def _result(df, check_id, scope, left_name, left, right_name, right) -> DataFrame:
    diff = left - right
    return df.select(
        F.lit(check_id).alias("check_id"),
        scope.alias("scope_key"),
        F.lit(left_name).alias("left_name"),
        left.cast("bigint").alias("left_value"),
        F.lit(right_name).alias("right_name"),
        right.cast("bigint").alias("right_value"),
        diff.cast("bigint").alias("difference"),
        F.coalesce(diff == 0, F.lit(False)).alias("passed"),
        F.current_timestamp().alias("measured_at")
    )


# Records in the newest valid OpenF1 snapshot of Bronze = rows of the parsed view of the same table
# A difference means a file was lost while parsing
def record_count(bronze: DataFrame, parsed: DataFrame, table: str) -> DataFrame:
    in_bronze = snapshot.latest_valid_openf1(bronze).agg(
        F.coalesce(F.sum(F.expr("json_array_length(payload)")), F.lit(0)).alias("n_bronze")
    )
    in_parsed = parsed.agg(F.count(F.lit(1)).alias("n_parsed"))
    return _result(
        in_bronze.crossJoin(in_parsed),
        "record_count",
        F.lit(table),
        "records in bronze",
        F.col("n_bronze"),
        "records after parse",
        F.col("n_parsed")
    )


def _latest_per_driver(race_result: DataFrame) -> DataFrame:
    w = Window.partitionBy("session_id", "driver_id").orderBy(F.col("snapshot_ts").desc())
    return (
        race_result.withColumn("_rn", F.row_number().over(w))
            .where("_rn = 1")
            .drop("_rn")
    )


# Sum of points of the newest results, silver vs the parsed rows before DQ
def points_total(silver_rr: DataFrame, parsed_rr: DataFrame) -> DataFrame:
    s = _latest_per_driver(silver_rr).groupBy("session_id").agg(F.sum("points").alias("p_silver"))
    p = _latest_per_driver(parsed_rr).groupBy("session_id").agg(F.sum("points").alias("p_parsed"))
    joined = s.join(p, "session_id", "full_outer")
    return _result(
        joined,
        "points_total",
        F.col("session_id"),
        "points in silver",
        F.round(F.coalesce("p_silver", F.lit(0)) * 100),
        "points in parsed bronze",
        F.round(F.coalesce("p_parsed", F.lit(0)) * 100)
    )


# The newest Jolpica load of every key has all its pages (1 = yes)
def pages_complete(bronze: DataFrame, keys: list[str], source: str) -> DataFrame:
    newest = bronze.groupBy(*keys).agg(F.max("_load_id").alias("newest"))
    complete = (
        snapshot.complete_jolpica_loads(bronze, keys)
        .groupBy(*keys)
        .agg(F.max("_load_id").alias("newest_complete"))
    )
    j = newest.join(complete, keys, "left")
    is_complete = F.coalesce((F.col("newest") == F.col("newest_complete")).cast("int"), F.lit(0))
    scope = F.concat_ws("|", F.lit(source), *[F.col(k).cast("string") for k in keys])
    return _result(j, "pages_complete", scope, "newest load complete", is_complete, "expected", F.lit(1))


# Files and records declared in the OpenF1 load marker vs what Bronze holds (per endpoint)
def load_marker(markers: DataFrame, tables: dict[str, DataFrame]) -> DataFrame:
    counts = F.from_json("payload", "counts map<string, struct<files: bigint, rows: bigint>>")["counts"]
    expected = markers.select("_load_id", F.explode(counts).alias("endpoint", "c")).select(
        "_load_id",
        "endpoint",
        F.col("c.files").alias("exp_files"),
        F.col("c.rows").alias("exp_rows"),
    )

    records = F.expr("json_array_length(payload)")
    actual = None
    for endpoint, df in tables.items():
        one = (
            df.where(records.isNotNull())
            .groupBy("_load_id")
            .agg(F.count(F.lit(1)).alias("act_files"), F.sum(records).alias("act_rows"))
            .withColumn("endpoint", F.lit(endpoint))
        )
        actual = one if actual is None else actual.unionByName(one)

    j = expected.join(actual, ["_load_id", "endpoint"], "left").fillna({"act_files": 0, "act_rows": 0})
    scope = F.concat_ws("|", "_load_id", "endpoint")
    files = _result(
        j,
        "load_marker",
        scope,
        "files in marker",
        F.col("exp_files"),
        "files in bronze",
        F.col("act_files")
    )
    rows = _result(
        j,
        "load_marker",
        scope,
        "records in marker",
        F.col("exp_rows"),
        "records in bronze",
        F.col("act_rows")
    )
    return files.unionByName(rows)
