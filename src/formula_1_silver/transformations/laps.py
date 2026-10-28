from pyspark import pipelines as dp

from formula_1.silver import factory
from formula_1.silver.transforms import laps
from formula_1.silver.schemas.silver import STINT_DDL, LAP_DDL


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

factory.register(
    "silver_lap",
    parsed=laps.lap_parsed,
    filtered=lambda df: laps.lap_filtered(df, dp.read("silver_session")),
    deduped=laps.lap_deduped,
    enriched=lambda df: laps.lap_enriched(
        df,
        dp.read("silver_stint"),
        dp.read("silver_session_driver"),
        dp.read("silver_track_status_interval"),
        laps._pit_in_laps(),
        laps._position_samples()
    ),
    keys=["openf1_session_key", "driver_number", "lap_number"],
    ddl=LAP_DDL,
    comment="OpenF1 laps per driver and session with tyre, track status and position at lap end"
)
