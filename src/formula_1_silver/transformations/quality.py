from pyspark import pipelines as dp

from formula_1.silver import config
from formula_1.silver.schemas.silver import QUARANTINE_DDL


@dp.materialized_view(
    name="silver_quarantine",
    schema=QUARANTINE_DDL,
    comment="Rows rejected by error-level checks, from every silver table"
)
def silver_quarantine():
    result = None
    for table in config.QUARANTINED_TABLES:
        part = dp.read(f"silver_quarantine_{table.removeprefix('silver_')}")
        if result is None:
            result = part
        else:
            result = result.unionByName(part)
    return result
