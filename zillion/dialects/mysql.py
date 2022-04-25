from sqlalchemy import func, text

from zillion.dialects.conversions import DialectDateConversions


class MySQLDialectDateConversions(DialectDateConversions):
    @classmethod
    def date_year_start(cls, x):
        return func.CONCAT(x, "-01-01")

    @classmethod
    def date_year_plus_year(cls, x):
        return func.DATE_ADD(str(x) + "-01-01", text("INTERVAL 1 YEAR"))

    @classmethod
    def datetime_year_end(cls, x):
        return func.DATE_SUB(
            func.DATE_ADD(str(x) + "-01-01", text("INTERVAL 1 YEAR")),
            text("INTERVAL 1 SECOND"),
        )

    @classmethod
    def date_month_start(cls, x):
        return func.CONCAT(x, "-01")

    @classmethod
    def date_month_plus_month(cls, x):
        return func.DATE_ADD(str(x) + "-01", text("INTERVAL 1 MONTH"))

    @classmethod
    def datetime_month_end(cls, x):
        return func.DATE_SUB(
            func.DATE_ADD(str(x) + "-01", text("INTERVAL 1 MONTH")),
            text("INTERVAL 1 SECOND"),
        )

    @classmethod
    def date_plus_day(cls, x):
        return func.DATE_ADD(x, text("INTERVAL 1 DAY"))

    @classmethod
    def datetime_day_end(cls, x):
        return func.DATE_SUB(
            func.DATE_ADD(x, text("INTERVAL 1 DAY")), text("INTERVAL 1 SECOND")
        )

    @classmethod
    def datetime_hour_plus_hour(cls, x):
        return func.DATE_ADD(x, text("INTERVAL 1 HOUR"))

    @classmethod
    def datetime_hour_end(cls, x):
        return func.DATE_SUB(
            func.DATE_ADD(x, text("INTERVAL 1 HOUR")), text("INTERVAL 1 SECOND")
        )

    @classmethod
    def datetime_minute_plus_minute(cls, x):
        return func.DATE_ADD(x, text("INTERVAL 1 MINUTE"))

    @classmethod
    def datetime_minute_end(cls, x):
        return func.DATE_SUB(
            func.DATE_ADD(x, text("INTERVAL 1 MINUTE")), text("INTERVAL 1 SECOND")
        )


MYSQL_DIALECT_CONVERSIONS = {
    "year": {
        "ds_formula": "EXTRACT(YEAR FROM {})",
        "ds_criteria_conversions": MySQLDialectDateConversions.get_year_criteria_conversions(),
    },
    "quarter": "CONCAT(YEAR({}), '-Q', QUARTER({}))",
    "quarter_of_year": "EXTRACT(QUARTER FROM {})",
    "month": {
        "ds_formula": "DATE_FORMAT({}, '%Y-%m')",
        "ds_criteria_conversions": MySQLDialectDateConversions.get_month_criteria_conversions(),
    },
    "month_name": "MONTHNAME({})",
    "month_of_year": "EXTRACT(MONTH FROM {})",
    "week_of_month": "WEEK({}, 1) - WEEK(DATE_FORMAT({},'%Y-%m-01'), 1) + 1",
    "week_of_year": "WEEK({}, 1)",  # Monday week start
    "period_of_month_7d": "FLOOR((DAYOFMONTH({}) - 1) / 7) + 1",
    "date": {
        "ds_formula": "DATE_FORMAT({}, '%Y-%m-%d')",
        "ds_criteria_conversions": MySQLDialectDateConversions.get_date_criteria_conversions(),
    },
    "day_name": "DAYNAME({})",
    "day_of_week": "WEEKDAY({}) + 1",  # Monday = 1
    "is_weekday": "IF((WEEKDAY({}) + 1) < 6, 1, 0)",  # Monday = 1
    "day_of_month": "EXTRACT(DAY FROM {})",
    "day_of_year": "DAYOFYEAR({})",
    "hour": {
        "ds_formula": "DATE_FORMAT({}, '%Y-%m-%d %H:00:00')",
        "ds_criteria_conversions": MySQLDialectDateConversions.get_hour_criteria_conversions(),
    },
    "hour_of_day": "EXTRACT(HOUR FROM {})",
    "minute": {
        "ds_formula": "DATE_FORMAT({}, '%Y-%m-%d %H:%i:00')",
        "ds_criteria_conversions": MySQLDialectDateConversions.get_minute_criteria_conversions(),
    },
    "minute_of_hour": "EXTRACT(MINUTE FROM {})",
    "datetime": {
        "ds_formula": "DATE_FORMAT({}, '%Y-%m-%d %H:%i:%S')",
        "ds_criteria_conversions": MySQLDialectDateConversions.get_datetime_criteria_conversions(),
    },
    "unixtime": "UNIX_TIMESTAMP({})",
}
