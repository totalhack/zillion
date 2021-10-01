from sqlalchemy import func, text
from sqlalchemy.dialects.postgresql import INTERVAL
from sqlalchemy.sql.functions import concat

from zillion.dialects.conversions import DialectDateConversions


def get_interval(n, t):
    return func.cast(concat(n, f" {t}"), INTERVAL)


class PostgreSQLDialectDateConversions(DialectDateConversions):
    @classmethod
    def date_year_start(cls, x):
        return func.TO_DATE(str(x) + "-01-01", "YYYY-MM-DD")

    @classmethod
    def date_year_plus_year(cls, x):
        return func.TO_DATE(str(x) + "-01-01", "YYYY-MM-DD") + get_interval(1, "YEARS")

    @classmethod
    def datetime_year_end(cls, x):
        return (
            func.TO_DATE(str(x) + "-01-01", "YYYY-MM-DD")
            + get_interval(1, "YEARS")
            - get_interval(1, "SECONDS")
        )

    @classmethod
    def date_month_start(cls, x):
        return func.TO_DATE(str(x) + "-01", "YYYY-MM")

    @classmethod
    def date_month_plus_month(cls, x):
        return func.TO_DATE(str(x) + "-01", "YYYY-MM") + get_interval(1, "MONTHS")

    @classmethod
    def datetime_month_end(cls, x):
        return (
            func.TO_DATE(str(x) + "-01", "YYYY-MM")
            + get_interval(1, "MONTHS")
            - get_interval(1, "SECONDS")
        )

    @classmethod
    def date_plus_day(cls, x):
        return func.TO_DATE(x, "YYYY-MM-DD") + get_interval(1, "DAYS")

    @classmethod
    def datetime_day_end(cls, x):
        return (
            func.TO_DATE(x, "YYYY-MM-DD")
            + get_interval(1, "DAYS")
            - get_interval(1, "SECONDS")
        )

    @classmethod
    def datetime_hour_plus_hour(cls, x):
        return func.TO_TIMESTAMP(x, "YYYY-MM-DD HH24:MI:SS") + get_interval(1, "HOURS")

    @classmethod
    def datetime_hour_end(cls, x):
        return (
            func.TO_TIMESTAMP(x, "YYYY-MM-DD HH24:MI:SS")
            + get_interval(1, "HOURS")
            - get_interval(1, "SECONDS")
        )

    @classmethod
    def datetime_minute_plus_minute(cls, x):
        return func.TO_TIMESTAMP(x, "YYYY-MM-DD HH24:MI:SS") + get_interval(
            1, "MINUTES"
        )

    @classmethod
    def datetime_minute_end(cls, x):
        return (
            func.TO_TIMESTAMP(x, "YYYY-MM-DD HH24:MI:SS")
            + get_interval(1, "MINUTES")
            - get_interval(1, "SECONDS")
        )


POSTGRESQL_DIALECT_CONVERSIONS = {
    "year": {
        "ds_formula": "EXTRACT(YEAR FROM {})",
        "ds_criteria_conversions": PostgreSQLDialectDateConversions.get_year_criteria_conversions(),
    },
    "quarter": "TO_CHAR({}, 'FMYYYY-\"Q\"Q')",
    "quarter_of_year": "EXTRACT(QUARTER FROM {})",
    "month": {
        "ds_formula": "TO_CHAR({}, 'FMYYYY-MM')",
        "ds_criteria_conversions": PostgreSQLDialectDateConversions.get_month_criteria_conversions(),
    },
    "month_name": "TO_CHAR({}, 'FMMonth')",
    "month_of_year": "EXTRACT(MONTH FROM {})",
    "week_of_year": "EXTRACT(WEEK FROM {})-1",  # HACK: attempt to get tests compatible with mysql and sqlite
    "date": {
        "ds_formula": "TO_CHAR({}, 'FMYYYY-MM-DD')",
        "ds_criteria_conversions": PostgreSQLDialectDateConversions.get_date_criteria_conversions(),
    },
    "day_name": "TO_CHAR({}, 'FMDay')",
    "day_of_week": "EXTRACT(ISODOW FROM {})",  # Monday = 1
    "is_weekday": (
        "CASE EXTRACT(ISODOW FROM {}) "
        "WHEN 1 THEN 1 "
        "WHEN 2 THEN 1 "
        "WHEN 3 THEN 1 "
        "WHEN 4 THEN 1 "
        "WHEN 5 THEN 1 "
        "WHEN 6 THEN 0 "
        "WHEN 7 THEN 0 "
        "ELSE NULL "
        "END"
    ),
    "day_of_month": "EXTRACT(DAY FROM {})",
    "day_of_year": "EXTRACT(DOY FROM {})",
    "hour": {
        "ds_formula": "TO_CHAR({}, 'FMYYYY-MM-DD HH24:00:00')",
        "ds_criteria_conversions": PostgreSQLDialectDateConversions.get_hour_criteria_conversions(),
    },
    "hour_of_day": "EXTRACT(HOUR FROM {})",
    "minute": {
        "ds_formula": "TO_CHAR({}, 'FMYYYY-MM-DD HH24:MI:00')",
        "ds_criteria_conversions": PostgreSQLDialectDateConversions.get_minute_criteria_conversions(),
    },
    "minute_of_hour": "EXTRACT(MINUTE FROM {})",
    "datetime": {
        "ds_formula": "TO_CHAR({}, 'FMYYYY-MM-DD HH24:MI:SS')",
        "ds_criteria_conversions": PostgreSQLDialectDateConversions.get_datetime_criteria_conversions(),
    },
    "unixtime": "EXTRACT(epoch from {})",
}
