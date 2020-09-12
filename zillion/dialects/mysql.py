MYSQL_YEAR_CRITERIA_CONVERSIONS = {
    "=": [
        [">=", "CONCAT(:0, '-01-01')"],
        ["<", "DATE_ADD(CONCAT(:0, '-01-01'), INTERVAL 1 YEAR)"],
    ],
    # NOTE: all of the conditions in the criteria replacement lists get AND'd together
    # so we can't simply do "x < 2020-01-01 or x >= 2021-01-01"
    "!=": [
        [
            "not between",
            [
                "CONCAT(:0, '-01-01')",
                "DATE_SUB(DATE_ADD(CONCAT(:0, '-01-01'), INTERVAL 1 YEAR), INTERVAL 1 SECOND)",
            ],
        ]
    ],
    ">": [[">=", "DATE_ADD(CONCAT(:0, '-01-01'), INTERVAL 1 YEAR)"]],
    ">=": [[">=", "CONCAT(:0, '-01-01')"]],
    "<": [["<", "CONCAT(:0, '-01-01')"]],
    "<=": [["<", "DATE_ADD(CONCAT(:0, '-01-01'), INTERVAL 1 YEAR)"]],
    "between": [
        [">=", "CONCAT(:0, '-01-01')"],
        ["<", "DATE_ADD(CONCAT(:1, '-01-01'), INTERVAL 1 YEAR)"],
    ],
    "not between": [
        [
            "not between",
            [
                "CONCAT(:0, '-01-01')",
                "DATE_SUB(DATE_ADD(CONCAT(:1, '-01-01'), INTERVAL 1 YEAR), INTERVAL 1 SECOND)",
            ],
        ]
    ],
}

MYSQL_MONTH_CRITERIA_CONVERSIONS = {
    "=": [
        [">=", "CONCAT(:0, '-01')"],
        ["<", "DATE_ADD(CONCAT(:0, '-01'), INTERVAL 1 MONTH)"],
    ],
    "!=": [
        [
            "not between",
            [
                "CONCAT(:0, '-01')",
                "DATE_SUB(DATE_ADD(CONCAT(:0, '-01'), INTERVAL 1 MONTH), INTERVAL 1 SECOND)",
            ],
        ]
    ],
    ">": [[">=", "DATE_ADD(CONCAT(:0, '-01'), INTERVAL 1 MONTH)"]],
    ">=": [[">=", "CONCAT(:0, '-01')"]],
    "<": [["<", "CONCAT(:0, '-01')"]],
    "<=": [["<", "DATE_ADD(CONCAT(:0, '-01'), INTERVAL 1 MONTH)"]],
    "between": [
        [">=", "CONCAT(:0, '-01')"],
        ["<", "DATE_ADD(CONCAT(:1, '-01'), INTERVAL 1 MONTH)"],
    ],
    "not between": [
        [
            "not between",
            [
                "CONCAT(:0, '-01')",
                "DATE_SUB(DATE_ADD(CONCAT(:1, '-01'), INTERVAL 1 MONTH), INTERVAL 1 SECOND)",
            ],
        ]
    ],
}

MYSQL_DATE_CRITERIA_CONVERSIONS = {
    "=": [[">=", ":0"], ["<", "DATE_ADD(:0, INTERVAL 1 DAY)"]],
    "!=": [
        [
            "not between",
            [":0", "DATE_SUB(DATE_ADD(:0, INTERVAL 1 DAY), INTERVAL 1 SECOND)"],
        ]
    ],
    ">": [[">=", "DATE_ADD(:0, INTERVAL 1 DAY)"]],
    ">=": [[">=", ":0"]],
    "<": [["<", ":0"]],
    "<=": [["<", "DATE_ADD(:0, INTERVAL 1 DAY)"]],
    "between": [[">=", ":0"], ["<", "DATE_ADD(:1, INTERVAL 1 DAY)"]],
    "not between": [
        [
            "not between",
            [":0", "DATE_SUB(DATE_ADD(:1, INTERVAL 1 DAY), INTERVAL 1 SECOND)"],
        ]
    ],
}

MYSQL_HOUR_CRITERIA_CONVERSIONS = {
    "=": [[">=", ":0"], ["<", "DATE_ADD(:0, INTERVAL 1 HOUR)"]],
    "!=": [
        [
            "not between",
            [":0", "DATE_SUB(DATE_ADD(:0, INTERVAL 1 HOUR), INTERVAL 1 SECOND)"],
        ]
    ],
    ">": [[">=", "DATE_ADD(:0, INTERVAL 1 HOUR)"]],
    ">=": [[">=", ":0"]],
    "<": [["<", ":0"]],
    "<=": [["<", "DATE_ADD(:0, INTERVAL 1 HOUR)"]],
    "between": [[">=", ":0"], ["<", "DATE_ADD(:1, INTERVAL 1 HOUR)"]],
    "not between": [
        [
            "not between",
            [":0", "DATE_SUB(DATE_ADD(:1, INTERVAL 1 HOUR), INTERVAL 1 SECOND)"],
        ]
    ],
}

MYSQL_MINUTE_CRITERIA_CONVERSIONS = {
    "=": [[">=", ":0"], ["<", "DATE_ADD(:0, INTERVAL 1 MINUTE)"]],
    "!=": [
        [
            "not between",
            [":0", "DATE_SUB(DATE_ADD(:0, INTERVAL 1 MINUTE), INTERVAL 1 SECOND)"],
        ]
    ],
    ">": [[">=", "DATE_ADD(:0, INTERVAL 1 MINUTE)"]],
    ">=": [[">=", ":0"]],
    "<": [["<", ":0"]],
    "<=": [["<", "DATE_ADD(:0, INTERVAL 1 MINUTE)"]],
    "between": [[">=", ":0"], ["<", "DATE_ADD(:1, INTERVAL 1 MINUTE)"]],
    "not between": [
        [
            "not between",
            [":0", "DATE_SUB(DATE_ADD(:1, INTERVAL 1 MINUTE), INTERVAL 1 SECOND)"],
        ]
    ],
}

MYSQL_DATETIME_CRITERIA_CONVERSIONS = {
    "=": [["=", ":0"]],
    "!=": [["!=", ":0"]],
    ">": [[">", ":0"]],
    ">=": [[">=", ":0"]],
    "<": [["<", ":0"]],
    "<=": [["<=", ":0"]],
    "between": [["between", [":0", ":1"]]],
    "not between": [["not between", [":0", ":1"]]],
}

MYSQL_DIALECT_CONVERSIONS = {
    "year": {
        "ds_formula": "EXTRACT(YEAR FROM {})",
        "ds_criteria_conversions": MYSQL_YEAR_CRITERIA_CONVERSIONS,
    },
    "quarter": "CONCAT(YEAR({}), '-Q', QUARTER({}))",
    "quarter_of_year": "EXTRACT(QUARTER FROM {})",
    "month": {
        "ds_formula": "DATE_FORMAT({}, '%Y-%m')",
        "ds_criteria_conversions": MYSQL_MONTH_CRITERIA_CONVERSIONS,
    },
    "month_name": "MONTHNAME({})",
    "month_of_year": "EXTRACT(MONTH FROM {})",
    "date": {
        "ds_formula": "DATE_FORMAT({}, '%Y-%m-%d')",
        "ds_criteria_conversions": MYSQL_DATE_CRITERIA_CONVERSIONS,
    },
    "day_name": "DAYNAME({})",
    "day_of_week": "WEEKDAY({}) + 1",  # Monday = 1
    "day_of_month": "EXTRACT(DAY FROM {})",
    "day_of_year": "DAYOFYEAR({})",
    "hour": {
        "ds_formula": "DATE_FORMAT({}, '%Y-%m-%d %H:00:00')",
        "ds_criteria_conversions": MYSQL_HOUR_CRITERIA_CONVERSIONS,
    },
    "hour_of_day": "EXTRACT(HOUR FROM {})",
    "minute": {
        "ds_formula": "DATE_FORMAT({}, '%Y-%m-%d %H:%i:00')",
        "ds_criteria_conversions": MYSQL_MINUTE_CRITERIA_CONVERSIONS,
    },
    "minute_of_hour": "EXTRACT(MINUTE FROM {})",
    "datetime": {
        "ds_formula": "DATE_FORMAT({}, '%Y-%m-%d %H:%i:%S')",
        "ds_criteria_conversions": MYSQL_DATETIME_CRITERIA_CONVERSIONS,
    },
    "unixtime": "UNIX_TIMESTAMP({})",
}
