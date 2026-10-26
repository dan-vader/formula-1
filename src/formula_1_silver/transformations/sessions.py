from pyspark import pipelines as dp

from formula_1.silver import factory
from formula_1.silver.schemas.silver import SESSION_DDL, SESSION_DRIVER_DDL
from formula_1.silver.transforms import session


factory.register(
    "silver_session",
    parsed=session.session_parsed,
    filtered=session.session_filtered,
    deduped=session.session_deduped,
    enriched=session.session_enriched,
    keys=["session_id"],
    ddl=SESSION_DDL,
    comment="Race and Sprint sessions: OpenF1 sessions + meetings + Jolpica calendar"
)

factory.register(
    "silver_session_driver",
    parsed=session.session_driver_parsed,
    filtered=lambda df: session.session_driver_filtered(df, dp.read("silver_session")),
    deduped=session.session_driver_deduped,
    enriched=lambda df: session.session_driver_enriched(df, dp.read("silver_session")),
    keys=["openf1_session_key", "driver_number"],
    ddl=SESSION_DRIVER_DDL,
    comment="OpenF1 car number in a session to Jolpica driver and constructor"
)
