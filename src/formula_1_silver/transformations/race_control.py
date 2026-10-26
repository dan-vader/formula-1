from pyspark import pipelines as dp

from formula_1.silver import factory
from formula_1.silver.transforms import race_control
from formula_1.silver.schemas.silver import RACE_CONTROL_DDL


factory.register(
    "silver_race_control",
    parsed=race_control.race_control_parsed,
    filtered=lambda df: race_control.race_control_filtered(df, dp.read("silver_session")),
    deduped=race_control.race_control_deduped,
    enriched=race_control.race_control_enriched,
    keys=["openf1_session_key", "event_ts", "event_hash"],
    ddl=RACE_CONTROL_DDL,
    comment="OpenF1 race control messages with a classified event_type"
)
