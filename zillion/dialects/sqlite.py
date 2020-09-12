SQLITE_YEAR_CRITERIA_CONVERSIONS = {
    "=": [[">=", "DATE(:0 || '-01-01')"], ["<", "DATE(:0 || '-01-01', '+1 year')"]],
    # NOTE: all of the conditions in the criteria replacement lists get AND'd together
    # so we can't simply do "x < 2020-01-01 or x >= 2021-01-01"
    "!=": [
        [
            "not between",
            [
                "DATE(:0 || '-01-01')",
                "DATETIME(:0 || '-01-01', '+1 year', '-1 second')",
            ],
        ]
    ],
    ">": [[">=", "DATE(:0 || '-01-01', '+1 year')"]],
    ">=": [[">=", "DATE(:0 || '-01-01')"]],
    "<": [["<", "DATE(:0 || '-01-01')"]],
    "<=": [["<", "DATE(:0 || '-01-01', '+1 year')"]],
    "between": [
        [">=", "DATE(:0 || '-01-01')"],
        ["<", "DATE(:1 || '-01-01', '+1 year')"],
    ],
    "not between": [
        [
            "not between",
            [
                "DATE(:0 || '-01-01')",
                "DATETIME(:1 || '-01-01', '+1 year', '-1 second')",
            ],
        ]
    ],
}

SQLITE_MONTH_CRITERIA_CONVERSIONS = {
    "=": [[">=", "DATE(:0 || '-01')"], ["<", "DATE(:0 || '-01', '+1 month')"]],
    "!=": [
        [
            "not between",
            ["DATE(:0 || '-01')", "DATETIME(:0 || '-01', '+1 month', '-1 second')"],
        ]
    ],
    ">": [[">=", "DATE(:0 || '-01', '+1 month')"]],
    ">=": [[">=", "DATE(:0 || '-01')"]],
    "<": [["<", "DATE(:0 || '-01')"]],
    "<=": [["<", "DATE(:0 || '-01', '+1 month')"]],
    "between": [[">=", "DATE(:0 || '-01')"], ["<", "DATE(:1 || '-01', '+1 month')"]],
    "not between": [
        [
            "not between",
            ["DATE(:0 || '-01')", "DATETIME(:1 || '-01', '+1 month', '-1 second')"],
        ]
    ],
}

SQLITE_DATE_CRITERIA_CONVERSIONS = {
    "=": [[">=", ":0"], ["<", "DATE(:0, '+1 day')"]],
    "!=": [["not between", [":0", "DATETIME(:0, '+1 day', '-1 second')"]]],
    ">": [[">=", "DATE(:0, '+1 day')"]],
    ">=": [[">=", ":0"]],
    "<": [["<", ":0"]],
    "<=": [["<", "DATE(:0, '+1 day')"]],
    "between": [[">=", ":0"], ["<", "DATE(:1, '+1 day')"]],
    "not between": [["not between", [":0", "DATETIME(:1, '+1 day', '-1 second')"]]],
}

SQLITE_HOUR_CRITERIA_CONVERSIONS = {
    "=": [[">=", ":0"], ["<", "DATETIME(:0, '+1 hour')"]],
    "!=": [["not between", [":0", "DATETIME(:0, '+1 hour', '-1 second')"]]],
    ">": [[">=", "DATETIME(:0, '+1 hour')"]],
    ">=": [[">=", ":0"]],
    "<": [["<", ":0"]],
    "<=": [["<", "DATETIME(:0, '+1 hour')"]],
    "between": [[">=", ":0"], ["<", "DATETIME(:1, '+1 hour')"]],
    "not between": [["not between", [":0", "DATETIME(:1, '+1 hour', '-1 second')"]]],
}

SQLITE_MINUTE_CRITERIA_CONVERSIONS = {
    "=": [[">=", ":0"], ["<", "DATETIME(:0, '+1 minute')"]],
    "!=": [["not between", [":0", "DATETIME(:0, '+1 minute', '-1 second')"]]],
    ">": [[">=", "DATETIME(:0, '+1 minute')"]],
    ">=": [[">=", ":0"]],
    "<": [["<", ":0"]],
    "<=": [["<", "DATETIME(:0, '+1 minute')"]],
    "between": [[">=", ":0"], ["<", "DATETIME(:1, '+1 minute')"]],
    "not between": [["not between", [":0", "DATETIME(:1, '+1 minute', '-1 second')"]]],
}

SQLITE_DATETIME_CRITERIA_CONVERSIONS = {
    "=": [["=", ":0"]],
    "!=": [["!=", ":0"]],
    ">": [[">", ":0"]],
    ">=": [[">=", ":0"]],
    "<": [["<", ":0"]],
    "<=": [["<=", ":0"]],
    "between": [["between", [":0", ":1"]]],
    "not between": [["not between", [":0", ":1"]]],
}

SQLITE_DIALECT_CONVERSIONS = {
    "year": {
        "ds_formula": "cast(strftime('%Y', {}) as integer)",
        "ds_criteria_conversions": SQLITE_YEAR_CRITERIA_CONVERSIONS,
    },
    "quarter": "strftime('%Y', {}) || '-Q' || ((cast(strftime('%m', {}) as integer) + 2) / 3)",  # 2020-Q1
    "quarter_of_year": "(cast(strftime('%m', {}) as integer) + 2) / 3",
    "month": {
        "ds_formula": "strftime('%Y-%m', {})",
        "ds_criteria_conversions": SQLITE_MONTH_CRITERIA_CONVERSIONS,
    },
    "month_name": (
        "CASE strftime('%m', {}) "
        "WHEN '01' THEN 'January' "
        "WHEN '02' THEN 'February' "
        "WHEN '03' THEN 'March' "
        "WHEN '04' THEN 'April' "
        "WHEN '05' THEN 'May' "
        "WHEN '06' THEN 'June' "
        "WHEN '07' THEN 'July' "
        "WHEN '08' THEN 'August' "
        "WHEN '09' THEN 'September' "
        "WHEN '10' THEN 'October' "
        "WHEN '11' THEN 'November' "
        "WHEN '12' THEN 'December' "
        "ELSE NULL "
        "END"
    ),
    "month_of_year": "cast(strftime('%m', {}) as integer)",
    "date": {
        "ds_formula": "strftime('%Y-%m-%d', {})",
        "ds_criteria_conversions": SQLITE_DATE_CRITERIA_CONVERSIONS,
    },
    "day_name": (
        "CASE cast(strftime('%w', {}) as integer) "
        "WHEN 0 THEN 'Sunday' "
        "WHEN 1 THEN 'Monday' "
        "WHEN 2 THEN 'Tuesday' "
        "WHEN 3 THEN 'Wednesday' "
        "WHEN 4 THEN 'Thursday' "
        "WHEN 5 THEN 'Friday' "
        "WHEN 6 THEN 'Saturday' "
        "ELSE NULL "
        "END"
    ),
    "day_of_week": "(cast(strftime('%w', {}) as integer) + 6) % 7 + 1",  # Convert to Monday = 1
    "day_of_month": "cast(strftime('%d', {}) as integer)",
    "day_of_year": "cast(strftime('%j', {}) as integer)",
    "hour": {
        "ds_formula": "strftime('%Y-%m-%d %H:00:00', {})",
        "ds_criteria_conversions": SQLITE_HOUR_CRITERIA_CONVERSIONS,
    },
    "hour_of_day": "cast(strftime('%H', {}) as integer)",
    "minute": {
        "ds_formula": "strftime('%Y-%m-%d %H:%M:00', {})",
        "ds_criteria_conversions": SQLITE_MINUTE_CRITERIA_CONVERSIONS,
    },
    "minute_of_hour": "cast(strftime('%M', {}) as integer)",
    "datetime": {
        "ds_formula": "strftime('%Y-%m-%d %H:%M:%S', {})",
        "ds_criteria_conversions": SQLITE_DATETIME_CRITERIA_CONVERSIONS,
    },
    "unixtime": "cast(strftime('%s', {}) as integer)",
}
