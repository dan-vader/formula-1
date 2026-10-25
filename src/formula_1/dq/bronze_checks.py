"""Data-quality checks for the bronze tables.

Each check is a SQL query that returns one row with one column, failed_rows:
the number of rows that break the rule. 0 means the check passed.
"""

from typing import NamedTuple

from formula_1.bronze import jolpica, openf1, openf1_stream

STREAM_TABLE = "openf1_stream_raw"
JOLPICA_LOADS = "jolpica_loads_raw"
OPENF1_LOADS = "openf1_loads_raw"

JOLPICA_KEY = ("_load_id", "_season", "_round", "_page_offset")
OPENF1_KEY = ("_load_id", "_year", "_session_key")
LOAD_ID_PATTERN = "^[0-9]{8}T[0-9]{6}Z$"
REFERENCE_TABLES = ("ref_pu_supplier", "ref_team_lineage")
CONSTRUCTORS_TABLE = "jolpica_constructors_raw"
CONSTRUCTORS_SCHEMA = "MRData STRUCT<ConstructorTable: STRUCT<Constructors: ARRAY<STRUCT<constructorId: STRING>>>>"
DEFAULT_MAX_LOAD_AGE_HOURS = 48


class Check(NamedTuple):
    group: str  # completeness, uniqueness, validity, consistency, reconciliation, timeliness
    name: str
    table: str
    sql: str


def file_tables():
    """Bronze tables loaded from landing files: Jolpica, OpenF1 and their load markers."""
    return sorted({*jolpica.landing_tables().values(), *openf1.landing_tables().values()})


def jolpica_data_tables():
    return sorted(table for table in jolpica.landing_tables().values() if table != JOLPICA_LOADS)


def openf1_data_tables():
    """OpenF1 endpoint -> bronze table, without the load markers."""
    tables = openf1.landing_tables()
    return {endpoint: tables[endpoint] for endpoint in (*openf1.YEAR_ENDPOINTS, *openf1.SESSION_ENDPOINTS)}


# --- completeness -----------------------------------------------------------


def completeness(table):
    return Check(
        "completeness",
        "required_fields_and_valid_json",
        table,
        f"""
        SELECT count_if(
            _load_id IS NULL OR _load_id = ''
            OR _source_file_name IS NULL
            OR _ingested_at IS NULL
            OR try_parse_json(payload) IS NULL
        ) AS failed_rows
        FROM {table}""",
    )


def stream_completeness(table):
    return Check(
        "completeness",
        "required_stream_fields",
        table,
        f"""
        SELECT count_if(
            payload IS NULL OR endpoint IS NULL OR session_key IS NULL OR replay_id IS NULL
        ) AS failed_rows
        FROM {table}""",
    )


# --- uniqueness -------------------------------------------------------------


def unique_source_file(table):
    return Check(
        "uniqueness",
        "unique_source_file",
        table,
        f"""
        SELECT count(*) - count(DISTINCT _source_file_name) AS failed_rows
        FROM {table}""",
    )


def unique_key(table, columns):
    key = ", ".join(columns)
    return Check(
        "uniqueness",
        f"unique_key({key})",
        table,
        f"""
        SELECT count(*) - count(DISTINCT {key}) AS failed_rows
        FROM {table}""",
    )


def unique_stream_message(table):
    return Check(
        "uniqueness",
        "unique_stream_message",
        table,
        f"""
        SELECT count(*) - count(DISTINCT _kafka_partition, _kafka_offset) AS failed_rows
        FROM {table}""",
    )


# --- validity ---------------------------------------------------------------


def valid_load_id(table):
    return Check(
        "validity",
        "load_id_format",
        table,
        f"""
        SELECT count_if(NOT coalesce(_load_id RLIKE '{LOAD_ID_PATTERN}', false)) AS failed_rows
        FROM {table}""",
    )


def session_key_level(table, per_session):
    """Per-session tables need a real session key; year-level tables and markers use -1."""
    if per_session:
        name, broken = "session_key_set", "_session_key = -1"
    else:
        name, broken = "session_key_is_minus_one", "_session_key <> -1"
    return Check(
        "validity",
        name,
        table,
        f"""
        SELECT count_if({broken}) AS failed_rows
        FROM {table}""",
    )


def valid_stream_endpoint(table):
    allowed = ", ".join(f"'{endpoint}'" for endpoint in openf1_stream.STREAM_ENDPOINTS)
    return Check(
        "validity",
        "stream_endpoint_allowed",
        table,
        f"""
        SELECT count_if(endpoint NOT IN ({allowed})) AS failed_rows
        FROM {table}""",
    )


# --- consistency ------------------------------------------------------------


def has_load_marker(table, loads_table):
    """failed_rows = loads that have data but no marker."""
    return Check(
        "consistency",
        "load_has_marker",
        table,
        f"""
        SELECT count(*) AS failed_rows
        FROM (SELECT DISTINCT _load_id FROM {table}) AS data
        LEFT ANTI JOIN {loads_table} AS marker ON data._load_id = marker._load_id""",
    )


def reference_constructor_known(reference_table, constructors_table):
    """failed_rows = constructors of a reference file that Jolpica does not know."""
    return Check(
        "consistency",
        "constructor_in_jolpica",
        reference_table,
        f"""
        WITH known AS (
            SELECT explode(
                from_json(payload, '{CONSTRUCTORS_SCHEMA}').MRData.ConstructorTable.Constructors.constructorId
            ) AS constructor_id
            FROM {constructors_table}
        )
        SELECT count(DISTINCT ref.constructor_id) AS failed_rows
        FROM {reference_table} AS ref
        LEFT ANTI JOIN known ON ref.constructor_id = known.constructor_id""",
    )


# --- reconciliation ---------------------------------------------------------


def marker_counts_match(table, endpoint, loads_table):
    """failed_rows = OpenF1 loads whose marker counts differ from the table. Old markers without counts are skipped."""
    return Check(
        "reconciliation",
        "marker_counts_match",
        table,
        f"""
        WITH expected AS (
            SELECT
                _load_id,
                coalesce(try_cast(get_json_object(payload, '$.counts.{endpoint}.files') AS BIGINT), 0) AS file_count,
                coalesce(try_cast(get_json_object(payload, '$.counts.{endpoint}.rows') AS BIGINT), 0) AS row_count
            FROM {loads_table}
            WHERE get_json_object(payload, '$.counts') IS NOT NULL
        ),
        actual AS (
            SELECT _load_id, count(*) AS file_count, sum(json_array_length(payload)) AS row_count
            FROM {table}
            GROUP BY _load_id
        )
        SELECT count_if(
            expected.file_count <> coalesce(actual.file_count, 0)
            OR expected.row_count <> coalesce(actual.row_count, 0)
        ) AS failed_rows
        FROM expected
        LEFT JOIN actual ON expected._load_id = actual._load_id""",
    )


# --- timeliness -------------------------------------------------------------


def fresh_load(loads_table, max_age_hours):
    """failed_rows = 1 when the latest load is older than the limit or there is no load at all."""
    return Check(
        "timeliness",
        f"latest_load_within_{max_age_hours}h",
        loads_table,
        f"""
        SELECT CASE
            WHEN max(_ingested_at) >= current_timestamp() - INTERVAL {max_age_hours} HOURS THEN 0
            ELSE 1
        END AS failed_rows
        FROM {loads_table}""",
    )


# --- all checks -------------------------------------------------------------


def bronze_checks(catalog, schema, max_load_age_hours=DEFAULT_MAX_LOAD_AGE_HOURS):
    """All bronze checks with fully qualified table names."""

    def fq(name):
        return f"{catalog}.{schema}.{name}"

    jolpica_loads, openf1_loads = fq(JOLPICA_LOADS), fq(OPENF1_LOADS)
    checks = []

    for name in file_tables():
        checks += [completeness(fq(name)), unique_source_file(fq(name)), valid_load_id(fq(name))]

    for name in jolpica_data_tables():
        checks += [unique_key(fq(name), JOLPICA_KEY), has_load_marker(fq(name), jolpica_loads)]

    for endpoint, name in openf1_data_tables().items():
        checks += [
            unique_key(fq(name), OPENF1_KEY),
            session_key_level(fq(name), per_session=endpoint in openf1.SESSION_ENDPOINTS),
            has_load_marker(fq(name), openf1_loads),
            marker_counts_match(fq(name), endpoint, openf1_loads),
        ]

    for loads in (jolpica_loads, openf1_loads):
        checks += [unique_key(loads, ("_load_id",)), fresh_load(loads, max_load_age_hours)]
    checks.append(session_key_level(openf1_loads, per_session=False))

    for name in REFERENCE_TABLES:
        checks.append(reference_constructor_known(fq(name), fq(CONSTRUCTORS_TABLE)))

    stream = fq(STREAM_TABLE)
    checks += [stream_completeness(stream), unique_stream_message(stream), valid_stream_endpoint(stream)]
    return checks
