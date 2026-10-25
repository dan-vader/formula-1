def _all_strings(*names: str) -> str:
    return ", ".join(f"{name} string" for name in names)


LAPS = _all_strings(
    "meeting_key", "session_key", "driver_number", "lap_number",
    "date_start", "lap_duration", "is_pit_out_lap"
)
LAPS_IGNORED = {
    "duration_sector_1", "duration_sector_2", "duration_sector_3",
    "i1_speed", "i2_speed", "st_speed",
    "segments_sector_1", "segments_sector_2", "segments_sector_3"
}

PIT = _all_strings(
    "meeting_key", "session_key", "driver_number", "lap_number",
    "date", "lane_duration", "pit_duration", "stop_duration"
)

STINTS = _all_strings(
    "meeting_key", "session_key", "driver_number", "stint_number",
    "lap_start", "lap_end", "compound", "tyre_age_at_start"
)

POSITION = _all_strings(
    "meeting_key", "session_key", "driver_number",
    "date", "position"
)

RACE_CONTROL = _all_strings(
    "meeting_key", "session_key", "date", "driver_number", "lap_number",
    "category", "flag", "scope", "sector", "qualifying_phase", "message"
)
RACE_CONTROL_IGNORED = {"sector", "qualifying_phase"}

DRIVERS = _all_strings(
    "meeting_key", "session_key", "driver_number",
    "broadcast_name", "full_name", "first_name", "last_name", "name_acronym",
    "team_name", "team_colour", "headshot_url", "country_code"
)

OVERTAKES = _all_strings(
    "meeting_key", "session_key",
    "date", "overtaking_driver_number", "overtaken_driver_number", "position"
)

SESSIONS = _all_strings(
    "session_key", "session_name", "date_start", "date_end", "meeting_key",
    "circuit_short_name", "gmt_offset", "year", "is_cancelled"
)
SESSIONS_IGNORED = {"session_type", "circuit_key", "country_key", "country_code", "country_name", "location"}

MEETINGS = _all_strings(
    "meeting_key", "meeting_name", "circuit_type", "year", "is_cancelled"
)
MEETINGS_IGNORED = {
    "meeting_official_name", "location",
    "country_key", "country_code", "country_name", "country_flag",
    "circuit_key", "circuit_short_name", "circuit_info_url", "circuit_image",
    "gmt_offset", "date_start", "date_end"
}
