DIM_DATE_DDL = (
    "date_sk int, full_date date, day_of_week string, month_number int, month_name string, "
    "quarter string, year int, loaded_at timestamp"
)

DIM_SESSION_DDL = (
    "session_sk bigint, openf1_session_key int, season int, round int, session_type string, "
    "session_start_utc timestamp, event_name string, circuit_name string, circuit_type string, "
    "regulation_era string, is_inferred boolean, inferred_at timestamp, "
    "loaded_at timestamp, updated_at timestamp"
)

DIM_DRIVER_DDL = (
    "driver_sk bigint, driver_id string, full_name string, name_acronym string, "
    "is_inferred boolean, inferred_at timestamp, loaded_at timestamp, updated_at timestamp"
)

DIM_TEAM_DDL = (
    "team_sk bigint, constructor_id string, team_name string, team_lineage string, "
    "team_colour string, power_unit_supplier string, effective_from date, effective_to date, "
    "is_current boolean, loaded_at timestamp, updated_at timestamp"
)

DIM_TYRE_COMPOUND_DDL = (
    "tyre_compound_sk bigint, compound_name string, is_slick boolean, hardness_rank int, "
    "loaded_at timestamp, updated_at timestamp"
)

DIM_FINISH_STATUS_DDL = (
    "finish_status_sk bigint, status_id int, status string, status_category string, "
    "loaded_at timestamp, updated_at timestamp"
)

DIM_TRACK_STATUS_DDL = (
    "track_status_sk bigint, track_status_name string, is_neutralised boolean, "
    "loaded_at timestamp, updated_at timestamp"
)

DIM_LAP_FLAGS_DDL = (
    "lap_flags_sk bigint, is_pit_in_lap boolean, is_pit_out_lap boolean, loaded_at timestamp"
)

DIM_PIT_STOP_FLAGS_DDL = (
    "pit_stop_flags_sk bigint, is_tyre_change boolean, is_penalty_served boolean, "
    "loaded_at timestamp"
)


FACT_RACE_CLASSIFICATION_DDL = (
    "session_sk bigint, driver_sk bigint, classification_version int, session_date_sk int, "
    "classification_date_sk int, finish_status_sk bigint, team_sk bigint, grid_position int, "
    "classified_position int, points_change decimal(5,2), points decimal(5,2), "
    "overtakes_made int, loaded_at timestamp"
)

FACT_LAP_DDL = (
    "session_sk bigint, driver_sk bigint, team_sk bigint, session_date_sk int, "
    "track_status_sk bigint, lap_flags_sk bigint, tyre_compound_sk bigint, "
    "lap_duration_ms bigint, tyre_age_laps int, position_at_lap_end int, lap_number int, "
    "stint_number int, loaded_at timestamp, updated_at timestamp"
)

FACT_PIT_STOP_DDL = (
    "session_sk bigint, driver_sk bigint, team_sk bigint, session_date_sk int, "
    "tyre_compound_removed_sk bigint, tyre_compound_fitted_sk bigint, track_status_sk bigint, "
    "pit_stop_flags_sk bigint, pit_lane_duration_ms bigint, stop_duration_ms bigint, "
    "positions_lost int, lap_number int, loaded_at timestamp, updated_at timestamp"
)