from pyspark import pipelines as dp

from formula_1.silver import config
from formula_1.silver.schemas.silver import (
    QUARANTINE_DDL,
    DQ_METRICS_DDL,
    DQ_RECONCILIATION_DDL,
    SCHEMA_DRIFT_DDL
)
from formula_1.silver.transforms import metrics, reconcile, drift
from formula_1.silver.schemas import jolpica as J, openf1 as O

OPENF1_TABLES = {
    "openf1_stints_raw": "silver_stint",
    "openf1_laps_raw": "silver_lap",
    "openf1_pit_raw": "silver_pit_stop",
    "openf1_race_control_raw": "silver_race_control",
    "openf1_overtakes_raw": "silver_overtake",
}
OPENF1_ENDPOINTS = ["sessions", "meetings", "drivers", "laps", "stints", "pit", "race_control", "overtakes", "position"]
JOLPICA_TABLES = {
    "jolpica_schedule_raw": ["_season", "_round"],
    "jolpica_race_results_raw": ["_season", "_round"],
    "jolpica_sprint_results_raw": ["_season", "_round"],
    "jolpica_pitstops_raw": ["_season", "_round"],
    "jolpica_drivers_raw": ["_season"],
    "jolpica_constructors_raw": ["_season"],
    "jolpica_status_raw": ["_season"],
}

OPENF1_SCHEMAS = [
    ("openf1_laps_raw", O.LAPS, O.LAPS_IGNORED),
    ("openf1_pit_raw", O.PIT, set()),
    ("openf1_stints_raw", O.STINTS, set()),
    ("openf1_position_raw", O.POSITION, set()),
    ("openf1_race_control_raw", O.RACE_CONTROL, O.RACE_CONTROL_IGNORED),
    ("openf1_drivers_raw", O.DRIVERS, set()),
    ("openf1_overtakes_raw", O.OVERTAKES, set()),
    ("openf1_sessions_raw", O.SESSIONS, O.SESSIONS_IGNORED),
    ("openf1_meetings_raw", O.MEETINGS, O.MEETINGS_IGNORED),
]


@dp.materialized_view(
    name="silver_quarantine",
    schema=QUARANTINE_DDL,
    comment="Rows rejected by error-level checks, from every silver table"
)
def silver_quarantine():
    result = None
    for table in config.QUARANTINED_TABLES:
        part = dp.read(f"silver_quarantine_{table.removeprefix('silver_')}")
        result = part if result is None else result.unionByName(part)
    return result


@dp.materialized_view(
    name="silver_dq_metrics",
    schema=DQ_METRICS_DDL,
    comment="Row counts per stage for every silver table"
)
def silver_dq_metrics():
    result = None
    for table in config.QUARANTINED_TABLES:
        short = table.removeprefix("silver_")
        part = metrics.table_metrics(
            table=table,
            parsed=dp.read(f"{table}_parsed"),
            filtered=dp.read(f"{table}_filtered"),
            deduped=dp.read(f"{table}_deduped"),
            out=dp.read(table),
            quarantine=dp.read(f"silver_quarantine_{short}"),
            checked=dp.read(f"{table}_checked")
        )
        result = part if result is None else result.unionByName(part)
    return result


@dp.materialized_view(
    name="silver_dq_reconciliation",
    schema=DQ_RECONCILIATION_DDL,
    comment="Bronze to silver checks: load completeness, record counts and control totals"
)
def silver_dq_reconciliation():
    parts = [
        reconcile.record_count(config.bronze(bronze), dp.read(f"{table}_parsed"), table)
        for bronze, table in OPENF1_TABLES.items()
    ]
    parts.append(reconcile.points_total(dp.read("silver_race_result"), dp.read("silver_race_result_parsed")))
    parts.append(reconcile.load_marker(
        config.bronze(config.OPENF1_LOADS),
        {endpoint: config.bronze(f"openf1_{endpoint}_raw") for endpoint in OPENF1_ENDPOINTS}
    ))
    for table, keys in JOLPICA_TABLES.items():
        parts.append(reconcile.pages_complete(config.bronze(table), keys, table))

    result = parts[0]
    for part in parts[1:]:
        result = result.unionByName(part)
    return result


@dp.materialized_view(
    name="silver_schema_drift",
    schema=SCHEMA_DRIFT_DDL,
    comment="Payload keys of the newest Bronze load vs the explicit schemas"
)
def silver_schema_drift():
    parts = [
        drift.schema_drift(config.bronze(table), table, "$[0]", drift.expected_keys(ddl, ignored))
        for table, ddl, ignored in OPENF1_SCHEMAS
    ]
    for table, field in [("jolpica_race_results_raw", "Results"), ("jolpica_sprint_results_raw", "SprintResults")]:
        parts.append(drift.schema_drift(
            config.bronze(table),
            table,
            f"$.MRData.RaceTable.Races[0].{field}[0]",
            drift.expected_keys(J.RESULT_REC, J.KNOWN_IGNORED.get(table, set()))
        ))

    result = parts[0]
    for part in parts[1:]:
        result = result.unionByName(part)
    return result
