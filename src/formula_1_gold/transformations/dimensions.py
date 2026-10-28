from formula_1.gold import factory
from formula_1.gold.schemas import gold as S
from formula_1.gold.transforms import (
    dim_date,
    dim_driver,
    dim_finish_status,
    dim_junk,
    dim_session,
    dim_static,
    dim_team,
)

factory.register_dimension(
    "dim_date", dim_date.dim_date, S.DIM_DATE_DDL,
    comment="One calendar day of the seasons in scope",
    sk_col="date_sk", natural_key=["full_date"],
)

factory.register_dimension(
    "dim_session", dim_session.dim_session, S.DIM_SESSION_DDL,
    comment="One Race or Sprint session with its event, season and circuit",
    sk_col="session_sk",
)

factory.register_dimension(
    "dim_driver", dim_driver.dim_driver, S.DIM_DRIVER_DDL,
    comment="One driver who took part in at least one race session",
    sk_col="driver_sk", natural_key=["driver_id"],
)

factory.register_dimension(
    "dim_team", dim_team.dim_team, S.DIM_TEAM_DDL,
    comment="One version of a constructor; a new version starts when the power unit supplier changes",
    sk_col="team_sk", natural_key=["constructor_id", "effective_from"],
)

factory.register_dimension(
    "dim_finish_status", dim_finish_status.dim_finish_status, S.DIM_FINISH_STATUS_DDL,
    comment="How a driver's race session ended",
    sk_col="finish_status_sk", natural_key=["status_id"],
)

factory.register_dimension(
    "dim_tyre_compound", dim_static.dim_tyre_compound, S.DIM_TYRE_COMPOUND_DDL,
    comment="One Pirelli tyre compound",
    sk_col="tyre_compound_sk", natural_key=["compound_name"],
)

factory.register_dimension(
    "dim_track_status", dim_static.dim_track_status, S.DIM_TRACK_STATUS_DDL,
    comment="State of the track during a lap or a pit stop",
    sk_col="track_status_sk", natural_key=["track_status_name"],
)

factory.register_dimension(
    "dim_lap_flags", dim_junk.dim_lap_flags, S.DIM_LAP_FLAGS_DDL,
    comment="Junk dimension: every combination of lap flags",
    sk_col="lap_flags_sk", natural_key=dim_junk.LAP_FLAGS,
)

factory.register_dimension(
    "dim_pit_stop_flags", dim_junk.dim_pit_stop_flags, S.DIM_PIT_STOP_FLAGS_DDL,
    comment="Junk dimension: every combination of pit stop flags",
    sk_col="pit_stop_flags_sk", natural_key=dim_junk.PIT_STOP_FLAGS,
)