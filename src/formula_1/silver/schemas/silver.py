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

RACE_RESULT_DDL = (
    "session_id string, driver_id string, snapshot_ts timestamp, openf1_session_key int, constructor_id string, "
    "car_number int, grid_position int, position_text string, classified_position int, "
    "points decimal(5,2), status_id int, laps_completed int, result_hash string, is_late_change boolean"
)

JOLPICA_PIT_STOP_DDL = (
    "session_id string, driver_id string, stop_number int, lap_number int, "
    "pit_time_utc timestamp, duration_ms bigint, ingested_at timestamp"
)

RACE_CONTROL_DDL = (
    "openf1_session_key int, event_ts timestamp, event_hash string, category string, flag string, "
    "scope string, message string, driver_number int, lap_number int, event_type string, ingested_at timestamp"
)

TRACK_STATUS_INTERVAL_DDL = (
    "openf1_session_key int, interval_start_utc timestamp, interval_end_utc timestamp, "
    "track_status_name string, severity_rank int, ingested_at timestamp"
)

STINT_DDL = (
    "openf1_session_key int, driver_number int, stint_number int, lap_start int, lap_end int, "
    "compound string, tyre_age_at_start int, ingested_at timestamp"
)

LAP_DDL = (
    "openf1_session_key int, driver_number int, lap_number int, driver_id string, constructor_id string, "
    "lap_start_utc timestamp, lap_end_utc timestamp, lap_duration_ms bigint, is_pit_in_lap boolean, "
    "is_pit_out_lap boolean, compound string, stint_number int, tyre_age_laps int, "
    "track_status_name string, position_at_lap_end int, ingested_at timestamp"
)

PIT_STOP_DDL = (
    "openf1_session_key int, driver_number int, lap_number int, driver_id string, constructor_id string, "
    "pit_entry_utc timestamp, pit_lane_duration_ms bigint, stop_duration_ms bigint, "
    "compound_removed string, compound_fitted string, is_tyre_change boolean, is_penalty_served boolean, "
    "track_status_name string, position_before int, position_after int, positions_lost int, ingested_at timestamp"
)

OVERTAKE_DDL = (
    "openf1_session_key int, overtake_ts timestamp, overtaking_driver_number int, "
    "overtaken_driver_number int, position int, ingested_at timestamp"
)

QUARANTINE_DDL = (
    "quarantine_id string, source_table string, record_key string, failed_rules array<string>, "
    "payload string, source_load_id string, source_ingested_at timestamp"
)
