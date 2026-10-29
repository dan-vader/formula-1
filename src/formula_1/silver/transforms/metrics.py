from pyspark.sql import functions as F
from pyspark.sql import DataFrame


def _count(df, name) -> DataFrame:
    return df.agg(F.count(F.lit(1)).alias(name))


# Inputs are the stage views and the real tables of one silver table
def table_metrics(
        table: str,
        parsed: DataFrame,
        filtered: DataFrame,
        deduped: DataFrame,
        out: DataFrame,
        quarantine: DataFrame,
        checked: DataFrame
) -> DataFrame:
    warned = checked.where(F.col("_errors").isNull() & F.col("_warnings").isNotNull())
    by_rule = (
        warned.select(F.explode("_warnings").alias("w"))
            .groupBy(F.col("w.name").alias("rule"))
            .count()
            .agg(F.map_from_entries(F.collect_list(F.struct("rule", "count"))).alias("warnings_by_rule"))
    )

    row = _count(parsed, "rows_in")
    for part in [
        _count(filtered, "after_filter"),
        _count(deduped, "after_dedup"),
        _count(out, "rows_out"),
        _count(quarantine, "rows_quarantined"),
        _count(warned, "rows_warned"),
        by_rule
    ]:
        row = row.crossJoin(part)

    filtered_out = F.col("rows_in") - F.col("after_filter")
    deduplicated = F.col("after_filter") - F.col("after_dedup")
    balance = F.col("rows_out") + F.col("rows_quarantined") + deduplicated + filtered_out

    return row.select(
        F.lit(table).alias("table_name"),
        "rows_in",
        filtered_out.alias("rows_filtered_out"),
        deduplicated.alias("rows_deduplicated"),
        "rows_quarantined",
        "rows_out",
        "rows_warned",
        "warnings_by_rule",
        (F.col("rows_in") == balance).alias("reconciles"),
        F.current_timestamp().alias("measured_at")
    )
