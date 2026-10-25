DRIVER = "struct<driverId: string, permanentNumber: string, code: string, givenName: string, familyName: string>"

RESULT_REC = f"""
    number string, position string, positionText string, points string, grid string, laps string, status string,
    Driver {DRIVER},
    Constructor struct<constructorId: string, name: string>,
    Time struct<millis: string, time: string>,
    FastestLap struct<rank: string, lap: string>
"""

RACE_RESULTS = f"""
MRData struct<
    total: string,
    limit: string,
    RaceTable: struct<
        Races: array<struct<
            season: string, round: string, raceName: string, date: string, time: string,
            Circuit: struct<circuitId: string>,
            Results: array<struct<{RESULT_REC}>>,
            SprintResults: array<struct<{RESULT_REC}>>
        >>
    >
>
"""

SCHEDULE = """
MRData struct<
    total: string,
    limit: string,
    RaceTable: struct<
        Races: array<struct<
            season: string, round: string, raceName: string, date: string, time: string,
            Circuit: struct<circuitId: string>,
            Sprint: struct<date: string, time: string>
        >>
    >
>
"""

DRIVERS = f"""
MRData struct<
    total: string,
    limit: string,
    DriverTable: struct<
        Drivers: array<{DRIVER}>
    >
>
"""

CONSTRUCTORS = """
MRData struct<
    total: string,
    limit: string,
    ConstructorTable: struct<
        Constructors: array<struct<constructorId: string, name: string>>
    >
>
"""

STATUS = """
MRData struct<
    total: string,
    limit: string,
    StatusTable: struct<
        Status: array<struct<statusId: string, status: string>>
    >
>
"""

PITSTOPS = """
MRData struct<
    total: string,
    limit: string,
    RaceTable: struct<
        Races: array<struct<
            season: string, round: string,
            PitStops: array<struct<driverId: string, stop: string, lap: string, time: string, duration: string>>
        >>
    >
>
"""

# payload fields that exist on purpose and are not read
KNOWN_IGNORED = {"jolpica_race_results_raw": set()}