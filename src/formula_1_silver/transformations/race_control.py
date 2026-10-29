from pyspark import pipelines as dp

from formula_1.silver import factory
from formula_1.silver.transforms import race_control
from formula_1.silver.schemas.silver import RACE_CONTROL_DDL, TRACK_STATUS_INTERVAL_DDL


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

factory.register(
    "silver_track_status_interval",
    parsed=lambda: race_control.opening_events(dp.read("silver_race_control")),
    enriched=lambda df: race_control.track_status_intervals(
        df.unionByName(race_control.closing_events(dp.read("silver_race_control"))),
        dp.read("silver_session")
    ),
    keys=["openf1_session_key", "interval_start_utc"],
    ddl=TRACK_STATUS_INTERVAL_DDL,
    comment="Safety car, virtual safety car and red flag intervals"
)
