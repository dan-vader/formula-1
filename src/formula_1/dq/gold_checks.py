from formula_1.dq.bronze_checks import WARN, Check, unique_key
from formula_1.dq.silver_checks import not_empty

UNKNOWN_SK = -1
DEFAULT_MAX_UNKNOWN_SHARE = 0.05

DIMENSION_KEYS = {
    "dim_date": "date_sk",
    "dim_session": "session_sk",
    "dim_driver": "driver_sk",
    "dim_team": "team_sk",
    "dim_finish_status": "finish_status_sk",
    "dim_tyre_compound": "tyre_compound_sk",
    "dim_track_status": "track_status_sk",
    "dim_lap_flags": "lap_flags_sk",
    "dim_pit_stop_flags": "pit_stop_flags_sk",
}
# fact -> {fact column: dimension}, the same references as in src/formula_1_gold/transformations
FACT_REFERENCES = {
    "fact_transaction_lap": {
        "session_sk": "dim_session",
        "driver_sk": "dim_driver",
        "team_sk": "dim_team",
        "session_date_sk": "dim_date",
        "track_status_sk": "dim_track_status",
        "lap_flags_sk": "dim_lap_flags",
        "tyre_compound_sk": "dim_tyre_compound",
    },
    "fact_transaction_pit_stop": {
        "session_sk": "dim_session",
        "driver_sk": "dim_driver",
        "team_sk": "dim_team",
        "session_date_sk": "dim_date",
        "tyre_compound_removed_sk": "dim_tyre_compound",
        "tyre_compound_fitted_sk": "dim_tyre_compound",
        "track_status_sk": "dim_track_status",
        "pit_stop_flags_sk": "dim_pit_stop_flags",
    },
    "fact_transaction_race_classification": {
        "session_sk": "dim_session",
        "driver_sk": "dim_driver",
        "session_date_sk": "dim_date",
        "classification_date_sk": "dim_date",
        "finish_status_sk": "dim_finish_status",
        "team_sk": "dim_team",
    },
}
CURRENT_CLASSIFICATION_VIEW = "v_race_classification_current"


def unknown_member_once(table, key):
    """failed_rows = 0 when the dimension has exactly one Unknown row, otherwise how far it is from one."""
    return Check(
        "completeness",
        "unknown_member_once",
        table,
        f"""
        SELECT abs(count_if({key} = {UNKNOWN_SK}) - 1) AS failed_rows
        FROM {table}""",
    )


def key_in_dimension(table, column, dim_table, dim_key):
    """failed_rows = fact rows whose key is missing in the dimension (a dangling key)."""
    return Check(
        "consistency",
        f"{column}_in_{dim_table.split('.')[-1]}",
        table,
        f"""
        SELECT count(*) AS failed_rows
        FROM {table} AS fact
        LEFT ANTI JOIN {dim_table} AS dim ON fact.{column} = dim.{dim_key}""",
    )


def unknown_share(table, column, max_share):
    """failed_rows = rows pointing to Unknown when they are more than max_share of the fact, else 0.

    A warning: a few unknown keys are expected, many mean a broken source.
    """
    return Check(
        "validity",
        f"{column}_unknown_share_max_{round(max_share * 100)}pct",
        table,
        f"""
        WITH counts AS (
            SELECT count_if({column} = {UNKNOWN_SK}) AS unknown, count(*) AS total
            FROM {table}
        )
        SELECT CASE WHEN unknown > {max_share} * total THEN unknown ELSE 0 END AS failed_rows
        FROM counts""",
        severity=WARN,
    )


def one_current_team_version(table):
    """failed_rows = constructors with more than one current version. A retired team may have none."""
    return Check(
        "consistency",
        "one_current_version_per_team",
        table,
        f"""
        SELECT count(*) AS failed_rows
        FROM (
            SELECT constructor_id
            FROM {table}
            WHERE team_sk <> {UNKNOWN_SK}
            GROUP BY constructor_id
            HAVING count_if(is_current) > 1
        ) AS teams""",
    )


def contiguous_team_versions(table):
    """failed_rows = team versions followed by a version that does not start the next day (gap or overlap)."""
    return Check(
        "consistency",
        "team_versions_contiguous",
        table,
        f"""
        SELECT count_if(next_from IS NOT NULL AND next_from <> date_add(effective_to, 1)) AS failed_rows
        FROM (
            SELECT
                effective_to,
                lead(effective_from) OVER (PARTITION BY constructor_id ORDER BY effective_from) AS next_from
            FROM {table}
            WHERE team_sk <> {UNKNOWN_SK}
        ) AS versions""",
    )


def gold_checks(catalog, schema, max_unknown_share=DEFAULT_MAX_UNKNOWN_SHARE):
    """All gold checks with fully qualified table names."""

    def fq(name):
        return f"{catalog}.{schema}.{name}"

    checks = [unknown_member_once(fq(dim), key) for dim, key in DIMENSION_KEYS.items()]
    for fact, references in FACT_REFERENCES.items():
        checks.append(not_empty(fq(fact)))
        for column, dim in references.items():
            checks += [
                key_in_dimension(fq(fact), column, fq(dim), DIMENSION_KEYS[dim]),
                unknown_share(fq(fact), column, max_unknown_share),
            ]
    checks += [
        one_current_team_version(fq("dim_team")),
        contiguous_team_versions(fq("dim_team")),
        unique_key(fq(CURRENT_CLASSIFICATION_VIEW), ("session_sk", "driver_sk")),
    ]
    return checks