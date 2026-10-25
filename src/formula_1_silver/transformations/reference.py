from formula_1.silver import factory
from formula_1.silver.schemas.silver import FINISH_STATUS_DDL, DRIVER_DDL
from formula_1.silver.transforms import reference as R


factory.register(
    "silver_finish_status",
    parsed=R.finish_status_parsed,
    deduped=R.finish_status_deduped,
    enriched=R.finish_status_enriched,
    keys=["status_id"],
    ddl=FINISH_STATUS_DDL,
    comment="Jolpica finish statuses"
)

factory.register(
    "silver_driver",
    parsed=R.driver_parsed,
    deduped=R.driver_deduped,
    enriched=R.driver_enriched,
    keys=["driver_id"],
    ddl=DRIVER_DDL,
    comment="Jolpica drivers, one row per driver"
)
