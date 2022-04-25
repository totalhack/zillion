from sqlalchemy import func
from tlbx import st

from zillion.dialects.conversions import DialectDateConversions


class SQLiteDialectDateConversions(DialectDateConversions):
    @classmethod
    def date_year_start(cls, x):
        return func.DATE(str(x) + "-01-01")

    @classmethod
    def date_year_plus_year(cls, x):
        return func.DATE(str(x) + "-01-01", "+1 year")

    @classmethod
    def datetime_year_end(cls, x):
        return func.DATETIME(str(x) + "-01-01", "+1 year", "-1 second")

    @classmethod
    def date_month_start(cls, x):
        return func.DATE(str(x) + "-01")

    @classmethod
    def date_month_plus_month(cls, x):
        return func.DATE(str(x) + "-01", "+1 month")

    @classmethod
    def datetime_month_end(cls, x):
        return func.DATETIME(str(x) + "-01", "+1 month", "-1 second")

    @classmethod
    def date_plus_day(cls, x):
        return func.DATE(x, "+1 day")

    @classmethod
    def datetime_day_end(cls, x):
        return func.DATETIME(x, "+1 day", "-1 second")

    @classmethod
    def datetime_hour_plus_hour(cls, x):
        return func.DATETIME(x, "+1 hour")

    @classmethod
    def datetime_hour_end(cls, x):
        return func.DATETIME(x, "+1 hour", "-1 second")

    @classmethod
    def datetime_minute_plus_minute(cls, x):
        return func.DATETIME(x, "+1 minute")

    @classmethod
    def datetime_minute_end(cls, x):
        return func.DATETIME(x, "+1 minute", "-1 second")


SQLITE_DIALECT_CONVERSIONS = {
    "year": {
        "ds_formula": "cast(strftime('%Y', {}) as integer)",
        "ds_criteria_conversions": SQLiteDialectDateConversions.get_year_criteria_conversions(),
    },
    "quarter": "strftime('%Y', {}) || '-Q' || ((cast(strftime('%m', {}) as integer) + 2) / 3)",  # 2020-Q1
    "quarter_of_year": "(cast(strftime('%m', {}) as integer) + 2) / 3",
    "month": {
        "ds_formula": "strftime('%Y-%m', {})",
        "ds_criteria_conversions": SQLiteDialectDateConversions.get_month_criteria_conversions(),
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
    "week_of_month": "cast(strftime('%W', {}) as integer) - cast(strftime('%W', strftime('%Y-%m-01', {})) as integer) + 1",
    "week_of_year": "cast(strftime('%W', {}) as integer)+1",
    "period_of_month_7d": "cast((cast(strftime('%d', {}) as integer) - 1) / 7 as integer) + 1",
    "date": {
        "ds_formula": "strftime('%Y-%m-%d', {})",
        "ds_criteria_conversions": SQLiteDialectDateConversions.get_date_criteria_conversions(),
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
    "is_weekday": (
        "CASE cast(strftime('%w', {}) as integer) "
        "WHEN 0 THEN 0 "
        "WHEN 1 THEN 1 "
        "WHEN 2 THEN 1 "
        "WHEN 3 THEN 1 "
        "WHEN 4 THEN 1 "
        "WHEN 5 THEN 1 "
        "WHEN 6 THEN 0 "
        "ELSE NULL "
        "END"
    ),
    "day_of_month": "cast(strftime('%d', {}) as integer)",
    "day_of_year": "cast(strftime('%j', {}) as integer)",
    "hour": {
        "ds_formula": "strftime('%Y-%m-%d %H:00:00', {})",
        "ds_criteria_conversions": SQLiteDialectDateConversions.get_hour_criteria_conversions(),
    },
    "hour_of_day": "cast(strftime('%H', {}) as integer)",
    "minute": {
        "ds_formula": "strftime('%Y-%m-%d %H:%M:00', {})",
        "ds_criteria_conversions": SQLiteDialectDateConversions.get_minute_criteria_conversions(),
    },
    "minute_of_hour": "cast(strftime('%M', {}) as integer)",
    "datetime": {
        "ds_formula": "strftime('%Y-%m-%d %H:%M:%S', {})",
        "ds_criteria_conversions": SQLiteDialectDateConversions.get_datetime_criteria_conversions(),
    },
    "unixtime": "cast(strftime('%s', {}) as integer)",
}
