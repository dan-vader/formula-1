from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Column

from formula_1.silver import config
from formula_1.silver.schemas import jolpica as J
from formula_1.silver.transforms import snapshot, dedup
from formula_1.silver.transforms.parse import _try_cast


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
