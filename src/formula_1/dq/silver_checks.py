"""Data-quality checks for the silver tables.

Same contract as bronze_checks: each check is one SQL query that returns failed_rows.
"""

from formula_1.dq.bronze_checks import unique_key

SILVER_KEYS = {
    "silver_finish_status": ("status_id",),
    "silver_driver": ("driver_id",),
    "silver_team": ("constructor_id", "season"),
    "silver_session": ("session_id", "openf1_session_key"),
    "silver_session_driver": ("openf1_session_key", "driver_number"),
    "silver_race_result": ("session_id", "driver_id", "snapshot_ts"),
    "silver_jolpica_pit_stop": ("session_id", "driver_id", "stop_number"),
}


def silver_checks(catalog, schema):
    """All silver checks with fully qualified table names."""

    def fq(name):
        return f"{catalog}.{schema}.{name}"

    checks = [unique_key(fq(table), key) for table, key in SILVER_KEYS.items()]
    checks.append(unique_key(fq("silver_session"), ("session_id",)))
    checks.append(unique_key(fq("silver_session"), ("openf1_session_key",)))
    return checks