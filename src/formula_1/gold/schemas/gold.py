DIM_DATE_DDL = (
    "date_sk int NOT NULL PRIMARY KEY, full_date date, day_of_week string, month_number int, "
    "month_name string, quarter string, year int"
)

DIM_SESSION_DDL = (
    "session_sk bigint NOT NULL PRIMARY KEY, openf1_session_key int, season int, round int, "
    "session_type string, session_start_utc timestamp, event_name string, circuit_name string, "
    "circuit_type string, regulation_era string, is_inferred boolean, inferred_at timestamp"
)

DIM_DRIVER_DDL = (
    "driver_sk bigint NOT NULL PRIMARY KEY, driver_id string, full_name string, name_acronym string, "
    "is_inferred boolean, inferred_at timestamp"
)

DIM_TEAM_DDL = (
    "team_sk bigint NOT NULL PRIMARY KEY, constructor_id string, team_name string, team_lineage string, "
    "team_colour string, power_unit_supplier string, effective_from date, effective_to date, "
    "is_current boolean"
)

DIM_TYRE_COMPOUND_DDL = (
    "tyre_compound_sk bigint NOT NULL PRIMARY KEY, compound_name string, is_slick boolean, "
    "hardness_rank int"
)

DIM_FINISH_STATUS_DDL = (
    "finish_status_sk bigint NOT NULL PRIMARY KEY, status_id int, status string, status_category string"
)

DIM_TRACK_STATUS_DDL = (
    "track_status_sk bigint NOT NULL PRIMARY KEY, track_status_name string, is_neutralised boolean"
)

DIM_LAP_FLAGS_DDL = (
    "lap_flags_sk bigint NOT NULL PRIMARY KEY, is_pit_in_lap boolean, is_pit_out_lap boolean"
)

DIM_PIT_STOP_FLAGS_DDL = (
    "pit_stop_flags_sk bigint NOT NULL PRIMARY KEY, is_tyre_change boolean, is_penalty_served boolean"
)

FACT_RACE_CLASSIFICATION_DDL = (
    "session_sk bigint NOT NULL, driver_sk bigint NOT NULL, classification_version int NOT NULL, "
    "session_date_sk int NOT NULL, classification_date_sk int NOT NULL, finish_status_sk bigint NOT NULL, "
    "team_sk bigint NOT NULL, grid_position int, classified_position int, points_change decimal(5,2), "
    "points decimal(5,2), overtakes_made int, "
    "CONSTRAINT fact_transaction_race_classification_pk "
    "PRIMARY KEY (session_sk, driver_sk, classification_version)"
)

FACT_LAP_DDL = (
    "session_sk bigint NOT NULL, driver_sk bigint NOT NULL, team_sk bigint NOT NULL, "
    "session_date_sk int NOT NULL, track_status_sk bigint NOT NULL, lap_flags_sk bigint NOT NULL, "
    "tyre_compound_sk bigint NOT NULL, lap_duration_ms bigint, tyre_age_laps int, "
    "position_at_lap_end int, lap_number int NOT NULL, stint_number int, "
    "CONSTRAINT fact_transaction_lap_pk PRIMARY KEY (session_sk, driver_sk, lap_number)"
)

FACT_PIT_STOP_DDL = (
    "session_sk bigint NOT NULL, driver_sk bigint NOT NULL, team_sk bigint NOT NULL, "
    "session_date_sk int NOT NULL, tyre_compound_removed_sk bigint NOT NULL, "
    "tyre_compound_fitted_sk bigint NOT NULL, track_status_sk bigint NOT NULL, "
    "pit_stop_flags_sk bigint NOT NULL, pit_lane_duration_ms bigint, stop_duration_ms bigint, "
    "positions_lost int, lap_number int NOT NULL, "
    "CONSTRAINT fact_transaction_pit_stop_pk PRIMARY KEY (session_sk, driver_sk, lap_number)"
)