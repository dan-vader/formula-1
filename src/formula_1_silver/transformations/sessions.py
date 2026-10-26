from pyspark import pipelines as dp

from formula_1.silver import factory, config
from formula_1.silver.schemas.silver import SESSION_DDL
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
