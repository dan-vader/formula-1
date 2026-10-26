from pyspark import pipelines as dp

from formula_1.silver import factory
from formula_1.silver.transforms import laps
from formula_1.silver.schemas.silver import STINT_DDL


factory.register(
    "silver_stint",
    parsed=laps.stint_parsed,
    filtered=lambda df: laps.stint_filtered(df, dp.read("silver_session")),
    deduped=laps.stint_deduped,
    enriched=laps.stint_enriched,
    keys=["openf1_session_key", "driver_number", "stint_number"],
    ddl=STINT_DDL,
    comment="OpenF1 tyre stints per driver and session"
)

