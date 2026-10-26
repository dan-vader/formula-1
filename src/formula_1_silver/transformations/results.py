from pyspark import pipelines as dp

from formula_1.silver import factory
from formula_1.silver.schemas.silver import RACE_RESULT_DDL
from formula_1.silver.transforms import results


factory.register(
    "silver_race_result",
    parsed=results.race_result_parsed,
    deduped=results.race_result_deduped,
    enriched=lambda df: results.race_result_enriched(df, dp.read("silver_session"), dp.read("silver_finish_status")),
    keys=["session_id", "driver_id", "snapshot_ts"],
    ddl=RACE_RESULT_DDL,
    comment="Race and Sprint results with version history",
    ref_dfs=lambda: {"silver_team": dp.read("silver_team").select("constructor_id").distinct()}
)
