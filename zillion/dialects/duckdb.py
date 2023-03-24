import sqlalchemy as sa
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import DATE, TIMESTAMP
from sqlalchemy.sql.elements import Grouping
from tlbx import st

from zillion.dialects.conversions import DialectDateConversions

# Have to add all the sa.literals to avoid compile errors while printing
# queries: https://github.com/sqlalchemy/sqlalchemy/issues/5859


class DuckDBDialectDateConversions(DialectDateConversions):
    @classmethod
    def date_year_start(cls, x):
        return func.CAST(sa.literal(str(x) + "-01-01"), DATE)

    @classmethod
    def date_year_plus_year(cls, x):
        return Grouping(
            func.CAST(sa.literal(str(x) + "-01-01"), DATE) + func.to_years(1)
        )

    @classmethod
    def datetime_year_end(cls, x):
        return Grouping(
            func.CAST(sa.literal(str(x) + "-01-01"), TIMESTAMP)
            + func.to_years(1)
            - func.to_seconds(1)
        )

    @classmethod
    def date_month_start(cls, x):
        return func.strftime(func.CAST(sa.literal(str(x) + "-01"), DATE), "%Y-%m")

    @classmethod
    def date_month_plus_month(cls, x):
        return func.strftime(
            func.CAST(sa.literal(str(x) + "-01"), DATE) + func.to_months(1), "%Y-%m"
        )

    @classmethod
    def datetime_month_end(cls, x):
        return Grouping(
            func.CAST(sa.literal(str(x) + "-01"), TIMESTAMP)
            + func.to_months(1)
            - func.to_seconds(1)
        )

    @classmethod
    def date_plus_day(cls, x):
        return Grouping(func.CAST(sa.literal(str(x)), DATE) + func.to_days(1))

    @classmethod
    def datetime_day_end(cls, x):
        return Grouping(
            func.CAST(sa.literal(str(x)), TIMESTAMP)
            + func.to_days(1)
            - func.to_seconds(1)
        )

    @classmethod
    def datetime_hour_plus_hour(cls, x):
        return Grouping(func.CAST(sa.literal(str(x)), TIMESTAMP) + func.to_hours(1))

    @classmethod
    def datetime_hour_end(cls, x):
        return Grouping(
            func.CAST(sa.literal(str(x)), TIMESTAMP)
            + func.to_hours(1)
            - func.to_seconds(1)
        )

    @classmethod
    def datetime_minute_plus_minute(cls, x):
        return Grouping(func.CAST(sa.literal(str(x)), TIMESTAMP) + func.to_minutes(1))

    @classmethod
    def datetime_minute_end(cls, x):
        return Grouping(
            func.CAST(sa.literal(str(x)), TIMESTAMP)
            + func.to_minutes(1)
            - func.to_seconds(1)
        )


DUCKDB_DIALECT_CONVERSIONS = {
    "year": {
        "ds_formula": "EXTRACT(YEAR FROM {})",
        "ds_criteria_conversions": DuckDBDialectDateConversions.get_year_criteria_conversions(),
    },
    "quarter": "strftime({}, '%Y-Q')  || date_part('quarter', {})",
    "quarter_of_year": "EXTRACT(QUARTER FROM {})",
    "month": {
        "ds_formula": "strftime({}, '%Y-%m')",
        "ds_criteria_conversions": DuckDBDialectDateConversions.get_month_criteria_conversions(),
    },
    "month_name": "strftime({}, '%B')",
    "month_of_year": "EXTRACT(MONTH FROM {})",
    "week_of_month": "EXTRACT(WEEK FROM {}) - EXTRACT(WEEK FROM CAST(DATE_TRUNC('month', {}) as date)) + 1",
    "week_of_year": "EXTRACT(WEEK FROM {})",  # HACK: attempt to get tests compatible with mysql and sqlite
    "period_of_month_7d": "FLOOR((EXTRACT(DAY FROM {}) - 1) / 7) + 1",
    "date": {
        "ds_formula": "strftime({}, '%Y-%m-%d')",
        "ds_criteria_conversions": DuckDBDialectDateConversions.get_date_criteria_conversions(),
    },
    "day_name": "strftime({}, '%A')",
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
        "ds_formula": "strftime({}, '%Y-%m-%d %H:00:00')",
        "ds_criteria_conversions": DuckDBDialectDateConversions.get_hour_criteria_conversions(),
    },
    "hour_of_day": "EXTRACT(HOUR FROM {})",
    "minute": {
        "ds_formula": "strftime({}, '%Y-%m-%d %H:%M:00')",
        "ds_criteria_conversions": DuckDBDialectDateConversions.get_minute_criteria_conversions(),
    },
    "minute_of_hour": "EXTRACT(MINUTE FROM {})",
    "datetime": {
        "ds_formula": "strftime({}, '%Y-%m-%d %H:%M:%S')",
        "ds_criteria_conversions": DuckDBDialectDateConversions.get_datetime_criteria_conversions(),
    },
    "unixtime": "EXTRACT(epoch from {})",
}
