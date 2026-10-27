"""Data-quality checks for the silver tables.

Same contract as bronze_checks: each check is one SQL query that returns failed_rows.
"""

from formula_1.dq.bronze_checks import ERROR, WARN, Check, unique_key

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
    ("silver_race_result", "session_id", "silver_session", WARN),
    ("silver_race_result", "driver_id", "silver_driver", ERROR),
    ("silver_race_result", "constructor_id", "silver_team", ERROR),
    ("silver_race_result", "status_id", "silver_finish_status", ERROR),
    ("silver_jolpica_pit_stop", "session_id", "silver_session", WARN),
    ("silver_jolpica_pit_stop", "driver_id", "silver_driver", ERROR),
    ("silver_session_driver", "session_id", "silver_session", ERROR),
    ("silver_session_driver", "driver_id", "silver_driver", ERROR),
    ("silver_session_driver", "constructor_id", "silver_team", ERROR),
]


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


def silver_checks(catalog, schema):
    """All silver checks with fully qualified table names."""

    def fq(name):
        return f"{catalog}.{schema}.{name}"

    checks = [unique_key(fq(table), key) for table, key in SILVER_KEYS.items()]
    checks.append(unique_key(fq("silver_session"), ("session_id",)))
    checks.append(unique_key(fq("silver_session"), ("openf1_session_key",)))
    checks += [
        foreign_key(fq(table), column, fq(ref_table), severity)
        for table, column, ref_table, severity in FOREIGN_KEYS
    ]
    return checks