FINISH_STATUS_DDL = (
    "status_id int, status string, status_category string, ingested_at timestamp"
)

DRIVER_DDL = (
    "driver_id string, given_name string, family_name string, full_name string, name_acronym string, "
    "permanent_number int, ingested_at timestamp"
)

TEAM_DDL = (
    "constructor_id string, season int, power_unit_supplier string, team_name string, "
    "team_lineage string, team_colour string, ingested_at timestamp"
)

SESSION_DDL = (
    "session_id string, openf1_session_key int, meeting_key int, season int, round int, session_type "
    "string, event_name string, circuit_name string, circuit_type string, regulation_era string, "
    "session_start_utc timestamp, session_end_utc timestamp, gmt_offset string, session_date_local date, "
    "jolpica_circuit_id string, ingested_at timestamp"
)

SESSION_DRIVER_DDL = (
    "openf1_session_key int, driver_number int, session_id string, driver_id string, constructor_id "
    "string, name_acronym string, full_name string, team_name_openf1 string, team_colour string, "
    "map_method string, acronym_match boolean, ingested_at timestamp"
)
