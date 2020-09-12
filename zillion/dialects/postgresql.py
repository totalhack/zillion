POSTGRESQL_YEAR_CRITERIA_CONVERSIONS = {
    "=": [
        [">=", "TO_DATE(:0 || '-01-01', 'YYYY-MM-DD')"],
        ["<", "(TO_DATE(:0 || '-01-01', 'YYYY-MM-DD') + interval '1 year')"],
    ],
    # NOTE: all of the conditions in the criteria replacement lists get AND'd together
    # so we can't simply do "x < 2020-01-01 or x >= 2021-01-01"
    "!=": [
        [
            "not between",
            [
                "TO_DATE(:0 || '-01-01', 'YYYY-MM-DD')",
                "(TO_DATE(:0 || '-01-01', 'YYYY-MM-DD') + interval '1 year' - interval '1 second')",
            ],
        ]
    ],
    ">": [[">=", "(TO_DATE(:0 || '-01-01', 'YYYY-MM-DD') + interval '1 year')"]],
    ">=": [[">=", "TO_DATE(:0 || '-01-01', 'YYYY-MM-DD')"]],
    "<": [["<", "TO_DATE(:0 || '-01-01', 'YYYY-MM-DD')"]],
    "<=": [["<", "(TO_DATE(:0 || '-01-01', 'YYYY-MM-DD') + interval '1 year')"]],
    "between": [
        [">=", "TO_DATE(:0 || '-01-01', 'YYYY-MM-DD')"],
        ["<", "(TO_DATE(:1 || '-01-01', 'YYYY-MM-DD') + interval '1 year')"],
    ],
    "not between": [
        [
            "not between",
            [
                "TO_DATE(:0 || '-01-01', 'YYYY-MM-DD')",
                "(TO_DATE(:1 || '-01-01', 'YYYY-MM-DD') + interval '1 year' - interval '1 second')",
            ],
        ]
    ],
}

POSTGRESQL_MONTH_CRITERIA_CONVERSIONS = {
    "=": [
        [">=", "TO_DATE(:0 || '-01', 'YYYY-MM')"],
        ["<", "(TO_DATE(:0 || '-01', 'YYYY-MM') + interval '1 month')"],
    ],
    "!=": [
        [
            "not between",
            [
                "TO_DATE(:0 || '-01', 'YYYY-MM')",
                "(TO_DATE(:0 || '-01', 'YYYY-MM') + interval '1 month' - interval '1 second')",
            ],
        ]
    ],
    ">": [[">=", "(TO_DATE(:0 || '-01', 'YYYY-MM') + interval '1 month')"]],
    ">=": [[">=", "TO_DATE(:0 || '-01', 'YYYY-MM')"]],
    "<": [["<", "TO_DATE(:0 || '-01', 'YYYY-MM')"]],
    "<=": [["<", "(TO_DATE(:0 || '-01', 'YYYY-MM') + interval '1 month')"]],
    "between": [
        [">=", "TO_DATE(:0 || '-01', 'YYYY-MM')"],
        ["<", "(TO_DATE(:0 || '-01', 'YYYY-MM') + interval '1 month')"],
    ],
    "not between": [
        [
            "not between",
            [
                "TO_DATE(:0 || '-01', 'YYYY-MM')",
                "(TO_DATE(:0 || '-01', 'YYYY-MM') + interval '1 month' - interval '1 second')",
            ],
        ]
    ],
}

POSTGRESQL_DATE_CRITERIA_CONVERSIONS = {
    "=": [[">=", ":0"], ["<", "(TO_DATE(:0, 'YYYY-MM-DD') + interval '1 day')"]],
    "!=": [
        [
            "not between",
            [
                ":0",
                "(TO_DATE(:0, 'YYYY-MM-DD') + interval '1 day' - interval '1 second')",
            ],
        ]
    ],
    ">": [[">=", "(TO_DATE(:0, 'YYYY-MM-DD') + interval '1 day')"]],
    ">=": [[">=", ":0"]],
    "<": [["<", ":0"]],
    "<=": [["<", "(TO_DATE(:0, 'YYYY-MM-DD') + interval '1 day')"]],
    "between": [[">=", ":0"], ["<", "(TO_DATE(:1, 'YYYY-MM-DD') + interval '1 day')"]],
    "not between": [
        [
            "not between",
            [
                ":0",
                "(TO_DATE(:1, 'YYYY-MM-DD') + interval '1 day' - interval '1 second')",
            ],
        ]
    ],
}

POSTGRESQL_HOUR_CRITERIA_CONVERSIONS = {
    "=": [
        [">=", ":0"],
        ["<", "(TO_TIMESTAMP(:0, 'YYYY-MM-DD HH24:MI:SS') + interval '1 hour')"],
    ],
    "!=": [
        [
            "not between",
            [
                ":0",
                "(TO_TIMESTAMP(:0, 'YYYY-MM-DD HH24:MI:SS') + interval '1 hour' - interval '1 second')",
            ],
        ]
    ],
    ">": [[">=", "(TO_TIMESTAMP(:0, 'YYYY-MM-DD HH24:MI:SS') + interval '1 hour')"]],
    ">=": [[">=", ":0"]],
    "<": [["<", ":0"]],
    "<=": [["<", "(TO_TIMESTAMP(:0, 'YYYY-MM-DD HH24:MI:SS') + interval '1 hour')"]],
    "between": [
        [">=", ":0"],
        ["<", "(TO_TIMESTAMP(:1, 'YYYY-MM-DD HH24:MI:SS') + interval '1 hour')"],
    ],
    "not between": [
        [
            "not between",
            [
                ":0",
                "(TO_TIMESTAMP(:1, 'YYYY-MM-DD HH24:MI:SS') + interval '1 hour' - interval '1 second')",
            ],
        ]
    ],
}

POSTGRESQL_MINUTE_CRITERIA_CONVERSIONS = {
    "=": [
        [">=", ":0"],
        ["<", "(TO_TIMESTAMP(:0, 'YYYY-MM-DD HH24:MI:SS') + interval '1 minute')"],
    ],
    "!=": [
        [
            "not between",
            [
                ":0",
                "(TO_TIMESTAMP(:0, 'YYYY-MM-DD HH24:MI:SS') + interval '1 minute' - interval '1 second')",
            ],
        ]
    ],
    ">": [[">=", "(TO_TIMESTAMP(:0, 'YYYY-MM-DD HH24:MI:SS') + interval '1 minute')"]],
    ">=": [[">=", ":0"]],
    "<": [["<", ":0"]],
    "<=": [["<", "(TO_TIMESTAMP(:0, 'YYYY-MM-DD HH24:MI:SS') + interval '1 minute')"]],
    "between": [
        [">=", ":0"],
        ["<", "(TO_TIMESTAMP(:1, 'YYYY-MM-DD HH24:MI:SS') + interval '1 minute')"],
    ],
    "not between": [
        [
            "not between",
            [
                ":0",
                "(TO_TIMESTAMP(:1, 'YYYY-MM-DD HH24:MI:SS') + interval '1 minute' - interval '1 second')",
            ],
        ]
    ],
}

POSTGRESQL_DATETIME_CRITERIA_CONVERSIONS = {
    "=": [["=", ":0"]],
    "!=": [["!=", ":0"]],
    ">": [[">", ":0"]],
    ">=": [[">=", ":0"]],
    "<": [["<", ":0"]],
    "<=": [["<=", ":0"]],
    "between": [["between", [":0", ":1"]]],
    "not between": [["not between", [":0", ":1"]]],
}

POSTGRESQL_DIALECT_CONVERSIONS = {
    "year": {
        "ds_formula": "EXTRACT(YEAR FROM {})",
        "ds_criteria_conversions": POSTGRESQL_YEAR_CRITERIA_CONVERSIONS,
    },
    "quarter": "TO_CHAR({}, 'FMYYYY-\"Q\"Q')",
    "quarter_of_year": "EXTRACT(QUARTER FROM {})",
    "month": {
        "ds_formula": "TO_CHAR({}, 'FMYYYY-MM')",
        "ds_criteria_conversions": POSTGRESQL_MONTH_CRITERIA_CONVERSIONS,
    },
    "month_name": "TO_CHAR({}, 'FMMonth')",
    "month_of_year": "EXTRACT(MONTH FROM {})",
    "date": {
        "ds_formula": "TO_CHAR({}, 'FMYYYY-MM-DD')",
        "ds_criteria_conversions": POSTGRESQL_DATE_CRITERIA_CONVERSIONS,
    },
    "day_name": "TO_CHAR({}, 'FMDay')",
    "day_of_week": "EXTRACT(ISODOW FROM {})",  # Monday = 1
    "day_of_month": "EXTRACT(DAY FROM {})",
    "day_of_year": "EXTRACT(DOY FROM {})",
    "hour": {
        "ds_formula": "TO_CHAR({}, 'FMYYYY-MM-DD HH24:00:00')",
        "ds_criteria_conversions": POSTGRESQL_HOUR_CRITERIA_CONVERSIONS,
    },
    "hour_of_day": "EXTRACT(HOUR FROM {})",
    "minute": {
        "ds_formula": "TO_CHAR({}, 'FMYYYY-MM-DD HH24:MI:00')",
        "ds_criteria_conversions": POSTGRESQL_MINUTE_CRITERIA_CONVERSIONS,
    },
    "minute_of_hour": "EXTRACT(MINUTE FROM {})",
    "datetime": {
        "ds_formula": "TO_CHAR({}, 'FMYYYY-MM-DD HH24:MI:SS')",
        "ds_criteria_conversions": POSTGRESQL_DATETIME_CRITERIA_CONVERSIONS,
    },
    "unixtime": "EXTRACT(epoch from {})",
}
