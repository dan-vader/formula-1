from pyspark import pipelines as dp

from formula_1.silver import config
from formula_1.silver.schemas.silver import QUARANTINE_DDL, DQ_METRICS_DDL
from formula_1.silver.transforms import metrics


@dp.materialized_view(
    name="silver_quarantine",
    schema=QUARANTINE_DDL,
    comment="Rows rejected by error-level checks, from every silver table"
)
def silver_quarantine():
    result = None
    for table in config.QUARANTINED_TABLES:
        part = dp.read(f"silver_quarantine_{table.removeprefix('silver_')}")
        result = part if result is None else result.unionByName(part)
    return result


@dp.materialized_view(
    name="silver_dq_metrics",
    schema=DQ_METRICS_DDL,
    comment="Row counts per stage for every silver table"
)
def silver_dq_metrics():
    result = None
    for table in config.QUARANTINED_TABLES:
        short = table.removeprefix("silver_")
        part = metrics.table_metrics(
            table=table,
            parsed=dp.read(f"{table}_parsed"),
            filtered=dp.read(f"{table}_filtered"),
            deduped=dp.read(f"{table}_deduped"),
            out=dp.read(table),
            quarantine=dp.read(f"silver_quarantine_{short}"),
            checked=dp.read(f"{table}_checked")
        )
        result = part if result is None else result.unionByName(part)
    return result
