from pyspark import pipelines as dp

from formula_1.silver import factory
from formula_1.silver.transforms import pits
from formula_1.silver.schemas.silver import PIT_STOP_DDL, OVERTAKE_DDL


factory.register(
    "silver_pit_stop",
    parsed=pits.pit_parsed,
    filtered=lambda df: pits.pit_filtered(df, dp.read("silver_session")),
    deduped=pits.pit_deduped,
    enriched=lambda df: pits.pit_enriched(
        df,
        stints=dp.read("silver_stint"),
        lap=dp.read("silver_lap"),
        race_control=dp.read("silver_race_control"),
        session_driver=dp.read("silver_session_driver"),
        intervals=dp.read("silver_track_status_interval")
    ),
    keys=["openf1_session_key", "driver_number", "lap_number"],
    ddl=PIT_STOP_DDL,
    comment="OpenF1 pit lane visits: durations, tyres, penalty, track status, positions before and after"
)

factory.register(
    "silver_overtake",
    parsed=pits.overtake_parsed,
    filtered=lambda df: pits.overtake_filtered(df, dp.read("silver_session")),
    deduped=pits.overtake_deduped,
    enriched=pits.overtake_enriched,
    keys=["openf1_session_key", "overtake_ts", "overtaking_driver_number", "overtaken_driver_number"],
    ddl=OVERTAKE_DDL,
    comment="Overtakes per session (OpenF1 beta)"
)
