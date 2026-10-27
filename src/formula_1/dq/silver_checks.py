"""Data-quality checks for the silver tables.

Same contract as bronze_checks: each check is one SQL query that returns failed_rows.
"""

from formula_1.dq.bronze_checks import DEFAULT_MAX_LOAD_AGE_HOURS, ERROR, WARN, Check, unique_key

SILVER_KEYS = {
    "silver_finish_status": ("status_id",),
    "silver_driver": ("driver_id",),
    "silver_team": ("constructor_id", "season"),
    "silver_session": ("session_id", "openf1_session_key"),
    "silver_session_driver": ("openf1_session_key", "driver_number"),
    "silver_race_result": ("session_id", "driver_id", "snapshot_ts"),
    "silver_jolpica_pit_stop": ("session_id", "driver_id", "stop_number"),
}
# (table, column, referenced table, severity)
FOREIGN_KEYS = [
    ("silver_race_result", "session_id", "silver_session", ERROR),
    ("silver_race_result", "driver_id", "silver_driver", ERROR),
    ("silver_race_result", "constructor_id", "silver_team", ERROR),
    ("silver_race_result", "status_id", "silver_finish_status", ERROR),
    ("silver_jolpica_pit_stop", "session_id", "silver_session", WARN),
    ("silver_jolpica_pit_stop", "driver_id", "silver_driver", ERROR),
    ("silver_session_driver", "session_id", "silver_session", ERROR),
    ("silver_session_driver", "driver_id", "silver_driver", ERROR),
    ("silver_session_driver", "constructor_id", "silver_team", ERROR),
]
DEFAULT_MAX_QUARANTINE_SHARE = 0.05
# reloaded on every Jolpica / OpenF1 load, so their latest ingested_at shows how fresh silver is
FRESHNESS_TABLES = ("silver_finish_status", "silver_session")


def quarantine_table(table):
    """silver_race_result -> silver_quarantine_race_result, in the same catalog and schema."""
    prefix, name = table.rsplit(".", 1)
    return f"{prefix}.silver_quarantine_{name.removeprefix('silver_')}"


def not_empty(table):
    """failed_rows = 1 when the table has no rows: the pipeline ran but produced nothing."""
    return Check(
        "completeness",
        "table_not_empty",
        table,
        f"""
        SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END AS failed_rows
        FROM {table}""",
    )


def quarantine_empty(table):
    """failed_rows = rows in the quarantine table. A warning: a few bad source rows are expected."""
    return Check(
        "validity",
        "quarantine_empty",
        table,
        f"""
        SELECT count(*) AS failed_rows
        FROM {quarantine_table(table)}""",
        severity=WARN,
    )


def quarantine_share(table, max_share):
    """failed_rows = quarantined rows when they are more than max_share of all rows, else 0."""
    quarantine = quarantine_table(table)
    return Check(
        "validity",
        f"quarantine_share_max_{round(max_share * 100)}pct",
        table,
        f"""
        WITH counts AS (
            SELECT
                (SELECT count(*) FROM {quarantine}) AS bad,
                (SELECT count(*) FROM {table}) AS good
        )
        SELECT CASE WHEN bad > {max_share} * (bad + good) THEN bad ELSE 0 END AS failed_rows
        FROM counts""",
    )


def unmatched_session_drivers(table):
    """failed_rows = OpenF1 drivers that were not mapped to a Jolpica driver (R-MAP-01 keeps them as a warning)."""
    return Check(
        "consistency",
        "session_driver_mapped",
        table,
        f"""
        SELECT count_if(driver_id IS NULL) AS failed_rows
        FROM {table}""",
        severity=WARN,
    )


def acronym_mismatches(table):
    """failed_rows = mapped drivers whose OpenF1 acronym differs from the Jolpica code (R-MAP-02)."""
    return Check(
        "consistency",
        "acronym_matches_jolpica",
        table,
        f"""
        SELECT count_if(acronym_match = false) AS failed_rows
        FROM {table}""",
        severity=WARN,
    )


def foreign_key(table, column, ref_table, severity=ERROR):
    """failed_rows = non-null values of table.column that are missing in ref_table."""
    return Check(
        "consistency",
        f"{column}_in_{ref_table.split('.')[-1]}",
        table,
        f"""
        SELECT count(*) AS failed_rows
        FROM (SELECT {column} FROM {table} WHERE {column} IS NOT NULL) AS child
        LEFT ANTI JOIN {ref_table} AS parent ON child.{column} = parent.{column}""",
        severity=severity,
    )


def fresh_table(table, max_age_hours):
    """failed_rows = 1 when the newest row is older than the limit or the table is empty.

    A warning: between race weekends there may be nothing new to load.
    """
    return Check(
        "timeliness",
        f"latest_ingest_within_{max_age_hours}h",
        table,
        f"""
        SELECT CASE
            WHEN max(ingested_at) >= current_timestamp() - INTERVAL {max_age_hours} HOURS THEN 0
            ELSE 1
        END AS failed_rows
        FROM {table}""",
        severity=WARN,
    )


def silver_checks(catalog, schema, max_quarantine_share=DEFAULT_MAX_QUARANTINE_SHARE, max_age_hours=DEFAULT_MAX_LOAD_AGE_HOURS):
    """All silver checks with fully qualified table names."""

    def fq(name):
        return f"{catalog}.{schema}.{name}"

    checks = []
    for table, key in SILVER_KEYS.items():
        checks += [
            not_empty(fq(table)),
            unique_key(fq(table), key),
            quarantine_empty(fq(table)),
            quarantine_share(fq(table), max_quarantine_share),
        ]
    checks.append(unique_key(fq("silver_session"), ("session_id",)))
    checks.append(unique_key(fq("silver_session"), ("openf1_session_key",)))
    checks += [
        foreign_key(fq(table), column, fq(ref_table), severity)
        for table, column, ref_table, severity in FOREIGN_KEYS
    ]
    checks += [
        unmatched_session_drivers(fq("silver_session_driver")),
        acronym_mismatches(fq("silver_session_driver")),
    ]
    checks += [fresh_table(fq(table), max_age_hours) for table in FRESHNESS_TABLES]
    return checks