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
