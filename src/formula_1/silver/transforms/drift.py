from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from formula_1.silver import config


def expected_keys(record_ddl, ignored):
    return sorted(set(config.field_names(record_ddl)) | set(ignored))


# Keys of the first record in 20 files of the newest load vs the expected keys.
# record_path: JSONPath of one record, e.g. '$[0]' (OpenF1) or '$.MRData.RaceTable.Races[0].Results[0]' (Jolpica)
def schema_drift(bronze, source, record_path, expected) -> DataFrame:
    newest = bronze.agg(F.max("_load_id").alias("_max"))
    sample = bronze.join(newest, bronze["_load_id"] == newest["_max"]).limit(20)
    keys = F.expr(f"json_object_keys(get_json_object(payload, '{record_path}'))")
    seen = (
        sample.select("_load_id", keys.alias("k"))
        .where("k IS NOT NULL")
        .groupBy("_load_id")
        .agg(F.array_distinct(F.flatten(F.collect_list("k"))).alias("seen"))
    )

    known = F.array(*[F.lit(k) for k in expected])
    unexpected = F.array_sort(F.array_except(F.col("seen"), known))
    missing = F.array_sort(F.array_except(known, F.col("seen")))
    return seen.select(
        F.lit(source).alias("source_table"),
        F.col("_load_id").alias("load_id"),
        unexpected.alias("unexpected_keys"),
        missing.alias("missing_keys"),
        ((F.size(unexpected) > 0) | (F.size(missing) > 0)).alias("has_drift"),
        F.current_timestamp().alias("measured_at")
    )
