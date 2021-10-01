import sqlalchemy as sa


class DialectDateConversions:
    @classmethod
    def f(cls, func, i=0):
        """Generate a callable of a conversion function taking a specific index from
        the input data"""
        return lambda x, cls=cls, i=i: getattr(cls, func)(x[i])

    @classmethod
    def raw_value(cls, v):
        return sa.literal(v)

    @classmethod
    def date_year_start(cls, v):
        raise NotImplementedError

    @classmethod
    def date_year_plus_year(cls, v):
        raise NotImplementedError

    @classmethod
    def datetime_year_end(cls, x):
        raise NotImplementedError

    @classmethod
    def date_month_start(cls, x):
        raise NotImplementedError

    @classmethod
    def date_month_plus_month(cls, x):
        raise NotImplementedError

    @classmethod
    def datetime_month_end(cls, x):
        raise NotImplementedError

    @classmethod
    def date_plus_day(cls, x):
        raise NotImplementedError

    @classmethod
    def datetime_day_end(cls, x):
        raise NotImplementedError

    @classmethod
    def datetime_hour_plus_hour(cls, x):
        raise NotImplementedError

    @classmethod
    def datetime_hour_end(cls, x):
        raise NotImplementedError

    @classmethod
    def datetime_minute_plus_minute(cls, x):
        raise NotImplementedError

    @classmethod
    def datetime_minute_end(cls, x):
        raise NotImplementedError

    @classmethod
    def get_year_criteria_conversions(cls):
        return {
            "=": [
                [">=", cls.f("date_year_start")],
                ["<", cls.f("date_year_plus_year")],
            ],
            # NOTE: all of the conditions in the criteria replacement lists get AND'd together
            # so we can't simply do "x < 2020-01-01 or x >= 2021-01-01"
            "!=": [
                ["not between", [cls.f("date_year_start"), cls.f("datetime_year_end")]]
            ],
            ">": [[">=", cls.f("date_year_plus_year")]],
            ">=": [[">=", cls.f("date_year_start")]],
            "<": [["<", cls.f("date_year_start")]],
            "<=": [["<", cls.f("date_year_plus_year")]],
            "between": [
                [">=", cls.f("date_year_start")],
                ["<", cls.f("date_year_plus_year", i=1)],
            ],
            "not between": [
                [
                    "not between",
                    [cls.f("date_year_start"), cls.f("datetime_year_end", i=1)],
                ]
            ],
        }

    @classmethod
    def get_month_criteria_conversions(cls):
        return {
            "=": [
                [">=", cls.f("date_month_start")],
                ["<", cls.f("date_month_plus_month")],
            ],
            "!=": [
                [
                    "not between",
                    [cls.f("date_month_start"), cls.f("datetime_month_end")],
                ]
            ],
            ">": [[">=", cls.f("date_month_plus_month")]],
            ">=": [[">=", cls.f("date_month_start")]],
            "<": [["<", cls.f("date_month_start")]],
            "<=": [["<", cls.f("date_month_plus_month")]],
            "between": [
                [">=", cls.f("date_month_start")],
                ["<", cls.f("date_month_plus_month", i=1)],
            ],
            "not between": [
                [
                    "not between",
                    [cls.f("date_month_start"), cls.f("datetime_month_end", i=1)],
                ]
            ],
        }

    @classmethod
    def get_date_criteria_conversions(cls):
        return {
            "=": [[">=", cls.f("raw_value")], ["<", cls.f("date_plus_day")]],
            "!=": [["not between", [cls.f("raw_value"), cls.f("datetime_day_end")]]],
            ">": [[">=", cls.f("date_plus_day")]],
            ">=": [[">=", cls.f("raw_value")]],
            "<": [["<", cls.f("raw_value")]],
            "<=": [["<", cls.f("date_plus_day")]],
            "between": [[">=", cls.f("raw_value")], ["<", cls.f("date_plus_day", i=1)]],
            "not between": [
                ["not between", [cls.f("raw_value"), cls.f("datetime_day_end", i=1)]]
            ],
        }
        # return {
        #     "=": [[">=", ":0"], ["<", cls.f("date_plus_day")]],
        #     "!=": [["not between", [":0", cls.f("datetime_day_end")]]],
        #     ">": [[">=", cls.f("date_plus_day")]],
        #     ">=": [[">=", ":0"]],
        #     "<": [["<", ":0"]],
        #     "<=": [["<", cls.f("date_plus_day")]],
        #     "between": [[">=", ":0"], ["<", cls.f("date_plus_day", i=1)]],
        #     "not between": [["not between", [":0", cls.f("datetime_day_end", i=1)]]],
        # }

    @classmethod
    def get_hour_criteria_conversions(cls):
        return {
            "=": [[">=", cls.f("raw_value")], ["<", cls.f("datetime_hour_plus_hour")]],
            "!=": [["not between", [cls.f("raw_value"), cls.f("datetime_hour_end")]]],
            ">": [[">=", cls.f("datetime_hour_plus_hour")]],
            ">=": [[">=", cls.f("raw_value")]],
            "<": [["<", cls.f("raw_value")]],
            "<=": [["<", cls.f("datetime_hour_plus_hour")]],
            "between": [
                [">=", cls.f("raw_value")],
                ["<", cls.f("datetime_hour_plus_hour", i=1)],
            ],
            "not between": [
                ["not between", [cls.f("raw_value"), cls.f("datetime_hour_end", i=1)]]
            ],
        }

    @classmethod
    def get_minute_criteria_conversions(cls):
        return {
            "=": [
                [">=", cls.f("raw_value")],
                ["<", cls.f("datetime_minute_plus_minute")],
            ],
            "!=": [["not between", [cls.f("raw_value"), cls.f("datetime_minute_end")]]],
            ">": [[">=", cls.f("datetime_minute_plus_minute")]],
            ">=": [[">=", cls.f("raw_value")]],
            "<": [["<", cls.f("raw_value")]],
            "<=": [["<", cls.f("datetime_minute_plus_minute")]],
            "between": [
                [">=", cls.f("raw_value")],
                ["<", cls.f("datetime_minute_plus_minute", i=1)],
            ],
            "not between": [
                ["not between", [cls.f("raw_value"), cls.f("datetime_minute_end", i=1)]]
            ],
        }

    @classmethod
    def get_datetime_criteria_conversions(cls):
        return {
            "=": [["=", cls.f("raw_value")]],
            "!=": [["!=", cls.f("raw_value")]],
            ">": [[">", cls.f("raw_value")]],
            ">=": [[">=", cls.f("raw_value")]],
            "<": [["<", cls.f("raw_value")]],
            "<=": [["<=", cls.f("raw_value")]],
            "between": [["between", [cls.f("raw_value"), cls.f("raw_value", i=1)]]],
            "not between": [
                ["not between", [cls.f("raw_value"), cls.f("raw_value", i=1)]]
            ],
        }
