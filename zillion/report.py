from collections import OrderedDict
from concurrent.futures import as_completed, ThreadPoolExecutor, TimeoutError
import decimal
import inspect
import logging
import random
from sqlite3 import connect, Row
import time

from orderedset import OrderedSet
import pandas as pd
import sqlalchemy as sa
from tlbx import (
    dbg,
    dbgsql,
    error,
    sqlformat,
    json,
    st,
    chunks,
    initializer,
    is_int,
    orderedsetify,
    PrintMixin,
)

from zillion.configs import zillion_config
from zillion.core import (
    AggregationTypes,
    DataSourceQueryModes,
    FieldTypes,
    TechnicalTypes,
    UnsupportedGrainException,
    ROW_FILTER_OPS,
)
from zillion.sql_utils import sqla_compile, get_sqla_clause, to_sqlite_type

if zillion_config["DEBUG"]:
    logging.getLogger().setLevel(logging.DEBUG)

# Last unicode char - this helps get the rollup rows to sort last, but may
# need to be replaced for presentation
ROLLUP_INDEX_LABEL = chr(1114111)
ROLLUP_INDEX_PRETTY_LABEL = "::"
ROLLUP_TOTALS = "totals"

PANDAS_ROLLUP_AGGR_TRANSLATION = {
    AggregationTypes.AVG: "mean",
    AggregationTypes.COUNT: "sum",
    AggregationTypes.COUNT_DISTINCT: "sum",
}

zillion_engine = sa.create_engine(zillion_config["ZILLION_DB_URL"])
zillion_metadata = sa.MetaData()
zillion_metadata.bind = zillion_engine

Reports = sa.Table(
    "reports",
    zillion_metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("params", sa.Text),
    sa.Column("created_at", sa.DateTime, server_default=sa.func.now()),
)
zillion_metadata.create_all(zillion_engine)


class DataSourceQuery(PrintMixin):
    repr_attrs = ["metrics", "dimensions", "criteria"]

    @initializer
    def __init__(self, warehouse, metrics, dimensions, criteria, table_set):
        self.field_map = {}
        self.metrics = orderedsetify(metrics) if metrics else []
        self.dimensions = orderedsetify(dimensions) if dimensions else []
        self.select = self.build_select()

    def get_datasource_name(self):
        return self.table_set.ds_name

    def get_datasource(self):
        return self.warehouse.get_datasource(self.get_datasource_name())

    def get_bind(self):
        ds = self.get_datasource()
        assert ds.metadata.bind, (
            'Datasource "%s" does not have metadata.bind set'
            % self.get_datasource_name()
        )
        return ds.metadata.bind

    def get_conn(self):
        bind = self.get_bind()
        conn = bind.connect()
        return conn

    def execute(self):
        # TODOs:
        # Add straight joins? Optimize indexes?
        # MySQL: SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
        #  finally: SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ

        start = time.time()
        conn = self.get_conn()
        try:
            result = conn.execute(self.select)
            data = result.fetchall()
        except:
            error("Exception during query:")
            dbgsql(self.select)
            raise
        finally:
            conn.close()
        diff = time.time() - start
        dbg("Got %d rows in %.3fs" % (len(data), diff))
        return DataSourceQueryResult(self, data, diff)

    def build_select(self):
        # https://docs.sqlalchemy.org/en/latest/core/selectable.html
        select = sa.select()

        join = self.get_join()
        select = select.select_from(join)

        for dimension in self.dimensions:
            select = select.column(self.get_field_expression(dimension))

        for metric in self.metrics:
            select = select.column(self.get_field_expression(metric))

        select = self.add_where(select)
        select = self.add_group_by(select)
        return select

    def column_for_field(self, field, table=None):
        ts = self.table_set
        if table is not None:
            column = self.warehouse.table_field_map[ts.ds_name][table.fullname][field]
        else:
            if ts.join and field in ts.join.field_map:
                column = ts.join.field_map[field]
            elif (
                field
                in self.warehouse.table_field_map[ts.ds_name][ts.ds_table.fullname]
            ):
                column = self.column_for_field(field, table=ts.ds_table)
            else:
                assert False, "Could not determine column for field %s" % field
        self.field_map[field] = column
        return column

    def get_field_expression(self, field):
        column = self.column_for_field(field)
        field_obj = self.warehouse.get_field(field)
        return field_obj.get_ds_expression(column)

    def get_join(self):
        ts = self.table_set
        sqla_join = None
        last_table = None

        if not ts.join:
            return ts.ds_table

        for join_part in ts.join.join_parts:
            for table_name in join_part.table_names:
                table = self.warehouse.tables[ts.ds_name][table_name]
                if sqla_join is None:
                    sqla_join = table
                    last_table = table
                    continue

                if table == last_table:
                    continue

                conditions = []
                for field in join_part.join_fields:
                    last_column = self.column_for_field(field, table=last_table)
                    column = self.column_for_field(field, table=table)
                    conditions.append(column == last_column)
                sqla_join = sqla_join.outerjoin(table, *tuple(conditions))
                last_table = table

        return sqla_join

    def add_where(self, select):
        if not self.criteria:
            return select
        for row in self.criteria:
            field = row[0]
            column = self.column_for_field(field)
            clause = sa.and_(get_sqla_clause(column, row))
            select = select.where(clause)
        return select

    def add_group_by(self, select):
        if not self.dimensions:
            return select
        return select.group_by(
            *[sa.text(str(x)) for x in range(1, len(self.dimensions) + 1)]
        )

    def add_order_by(self, select, asc=True):
        if not self.dimensions:
            return select
        order_func = sa.asc
        if not asc:
            order_func = sa.desc
        return select.order_by(*[order_func(sa.text(x)) for x in self.dimensions])

    def covers_metric(self, metric):
        if metric in self.table_set.get_covered_metrics(self.warehouse):
            return True
        return False

    def covers_field(self, field):
        if field in self.table_set.get_covered_fields(self.warehouse):
            return True
        return False

    def add_metric(self, metric):
        assert self.covers_metric(metric), (
            "Metric %s can not be covered by query" % metric
        )
        # TODO: improve the way we maintain targeted metrics/dims
        self.table_set.target_fields.add(metric)
        self.metrics.add(metric)
        self.select = self.select.column(self.get_field_expression(metric))


class DataSourceQuerySummary(PrintMixin):
    repr_attrs = ["datasource_name", "rowcount", "duration"]

    def __init__(self, query, data, duration):
        self.datasource_name = query.get_datasource_name()
        self.metrics = query.metrics
        self.dimensions = query.dimensions
        self.select = query.select
        self.duration = round(duration, 4)
        self.rowcount = len(data)

    def format_query(self):
        return sqlformat(sqla_compile(self.select))

    def format(self):
        sql = self.format_query()
        parts = [
            "%d rows in %.4f seconds" % (self.rowcount, self.duration),
            "Datasource: %s" % self.datasource_name,
            "Metrics: %s" % list(self.metrics),
            "Dimensions: %s" % list(self.dimensions),
            "\n%s" % sql,
            # TODO: Explain of query plan
        ]
        return "\n".join(parts)


class DataSourceQueryResult(PrintMixin):
    repr_attrs = ["summary"]

    def __init__(self, query, data, duration):
        self.query = query
        self.data = data
        self.summary = DataSourceQuerySummary(query, data, duration)


class BaseCombinedResult:
    @initializer
    def __init__(self, warehouse, ds_query_results, primary_ds_dimensions):
        self.conn = self.get_conn()
        self.cursor = self.get_cursor(self.conn)
        self.table_name = "zillion_%s_%s" % (
            str(time.time()).replace(".", "_"),
            random.randint(0, 1e9),
        )
        self.primary_ds_dimensions = (
            orderedsetify(primary_ds_dimensions) if primary_ds_dimensions else []
        )
        self.ds_dimensions, self.ds_metrics = self.get_fields()
        self.create_table()
        self.load_table()

    def get_conn(self):
        raise NotImplementedError

    def get_cursor(self, conn):
        raise NotImplementedError

    def create_table(self):
        raise NotImplementedError

    def load_table(self):
        raise NotImplementedError

    def clean_up(self):
        raise NotImplementedError

    def get_final_result(self, metrics, dimensions, row_filters, rollup, pivot):
        raise NotImplementedError

    def get_row_hash(self, row):
        return hash(row[: len(self.primary_ds_dimensions)])

    def get_fields(self):
        dimensions = OrderedDict()
        metrics = OrderedDict()

        for qr in self.ds_query_results:
            for dim_name in qr.query.dimensions:
                if dim_name in dimensions:
                    continue
                dim = self.warehouse.get_dimension(dim_name)
                dimensions[dim_name] = dim

            for metric_name in qr.query.metrics:
                if metric_name in metrics:
                    continue
                metric = self.warehouse.get_metric(metric_name)
                metrics[metric_name] = metric

        return dimensions, metrics

    def get_field_names(self):
        dims, metrics = self.get_fields()
        return list(dims.keys()) + list(metrics.keys())


class SQLiteMemoryCombinedResult(BaseCombinedResult):
    def get_conn(self):
        return connect(":memory:")

    def get_cursor(self, conn):
        conn.row_factory = Row
        return conn.cursor()

    def create_table(self):
        create_sql = "CREATE TEMP TABLE %s (" % self.table_name
        column_clauses = ["hash BIGINT NOT NULL PRIMARY KEY"]

        for field_name, field in self.ds_dimensions.items():
            type_str = str(to_sqlite_type(field.type))
            clause = "%s %s NOT NULL" % (field_name, type_str)
            column_clauses.append(clause)

        for field_name, field in self.ds_metrics.items():
            type_str = str(to_sqlite_type(field.type))
            clause = "%s %s DEFAULT NULL" % (field_name, type_str)
            column_clauses.append(clause)

        create_sql += ", ".join(column_clauses)
        create_sql += ") WITHOUT ROWID"
        dbg(create_sql)  # Creates don't pretty print well with dbgsql?
        self.cursor.execute(create_sql)
        if self.primary_ds_dimensions:
            index_sql = "CREATE INDEX idx_dims ON %s (%s)" % (
                self.table_name,
                ", ".join(self.primary_ds_dimensions),
            )
            dbgsql(index_sql)
            self.cursor.execute(index_sql)
        self.conn.commit()

    def get_bulk_insert_sql(self, rows):
        columns = [k for k in rows[0].keys()]
        placeholder = "(%s)" % (", ".join(["?"] * (1 + len(columns))))
        columns_clause = "hash, " + ", ".join(columns)

        sql = "INSERT INTO %s (%s) VALUES %s" % (
            self.table_name,
            columns_clause,
            placeholder,
        )

        update_clauses = []
        for k in columns:
            if k in self.primary_ds_dimensions:
                continue
            update_clauses.append("%s=excluded.%s" % (k, k))

        if update_clauses:
            update_clause = " ON CONFLICT(hash) DO UPDATE SET " + ", ".join(
                update_clauses
            )
            sql = sql + update_clause

        values = []
        for row in rows:
            row_values = [self.get_row_hash(row)]
            for value in row.values():
                # XXX: sqlite cant handle Decimal values. Alternative approach:
                # https://stackoverflow.com/questions/6319409/how-to-convert-python-decimal-to-sqlite-numeric
                if isinstance(value, decimal.Decimal):
                    value = float(value)
                row_values.append(value)
            values.append(row_values)

        return sql, values

    def load_table(self):
        for qr in self.ds_query_results:
            for rows in chunks(qr.data, zillion_config["LOAD_TABLE_CHUNK_SIZE"]):
                insert_sql, values = self.get_bulk_insert_sql(rows)
                self.cursor.executemany(insert_sql, values)
            self.conn.commit()

    def select_all(self):
        qr = self.cursor.execute("SELECT * FROM %s" % self.table_name)
        return [OrderedDict(row) for row in qr.fetchall()]

    def get_final_select_sql(self, columns, dimension_aliases):
        columns_clause = ", ".join(columns)
        order_clause = "1"
        if dimension_aliases:
            order_clause = ", ".join(["%s ASC" % d for d in dimension_aliases])
        sql = "SELECT %s FROM %s GROUP BY hash ORDER BY %s" % (
            columns_clause,
            self.table_name,
            order_clause,
        )
        dbgsql(sql)
        return sql

    def apply_row_filters(self, df, row_filters, metrics, dimensions):
        filter_parts = []
        for row_filter in row_filters:
            field, op, value = row_filter
            assert (field in metrics) or (field in dimensions), (
                'Row filter field "%s" is not in result table' % field
            )
            assert op in ROW_FILTER_OPS, "Invalid row filter operation: %s" % op
            filter_parts.append("(%s %s %s)" % (field, op, value))
        return df.query(" and ".join(filter_parts))

    def get_multi_rollup_df(self, df, rollup, dimensions, aggrs, wavg, wavgs):
        # TODO: signature of this is a bit odd
        # TODO: test weighted averages
        # https://stackoverflow.com/questions/36489576/why-does-concatenation-of-dataframes-get-exponentially-slower
        level_aggrs = [df]

        for level in range(rollup):
            if (level + 1) == len(dimensions):
                # Unnecessary to rollup at the most granular level
                break

            grouped = df.groupby(level=list(range(0, level + 1)))
            level_aggr = grouped.agg(aggrs, skipna=True)
            for metric_name, weighting_metric in wavgs:
                level_aggr[metric_name] = wavg(metric_name, weighting_metric)

            # for the remaining levels, set index cols to ROLLUP_INDEX_LABEL
            if level != (len(dimensions) - 1):
                new_index_dims = []
                for dim in dimensions[level + 1 :]:
                    level_aggr[dim] = ROLLUP_INDEX_LABEL
                    new_index_dims.append(dim)
                level_aggr = level_aggr.set_index(new_index_dims, append=True)

            level_aggrs.append(level_aggr)

        df = pd.concat(level_aggrs, sort=False, copy=False)
        df.sort_index(inplace=True)
        return df

    def apply_rollup(self, df, rollup, metrics, dimensions):
        assert dimensions, "Can not rollup without dimensions"

        aggrs = {}
        wavgs = []

        def wavg(avg_name, weight_name):
            d = df[avg_name]
            w = df[weight_name]
            try:
                return (d * w).sum() / w.sum()
            except ZeroDivisionError:
                return d.mean()  # Return mean if there are no weights

        for metric_name in metrics:
            metric = self.warehouse.get_metric(metric_name)
            if metric.weighting_metric:
                wavgs.append((metric.name, metric.weighting_metric))
                continue
            else:
                aggr_func = PANDAS_ROLLUP_AGGR_TRANSLATION.get(
                    metric.aggregation, metric.aggregation
                )
            aggrs[metric.name] = aggr_func

        aggr = df.agg(aggrs, skipna=True)
        for metric_name, weighting_metric in wavgs:
            aggr[metric_name] = wavg(metric_name, weighting_metric)

        apply_totals = True
        if rollup != ROLLUP_TOTALS:
            df = self.get_multi_rollup_df(df, rollup, dimensions, aggrs, wavg, wavgs)
            if rollup != len(dimensions):
                apply_totals = False

        if apply_totals:
            totals_rollup_index = (
                (ROLLUP_INDEX_LABEL,) * len(dimensions)
                if len(dimensions) > 1
                else ROLLUP_INDEX_LABEL
            )
            with pd.option_context("mode.chained_assignment", None):
                df.at[totals_rollup_index, :] = aggr

        return df

    def apply_technicals(self, df, technicals, rounding):
        for metric, tech in technicals.items():
            rolling = df[metric].rolling(
                tech.window, min_periods=tech.min_periods, center=tech.center
            )
            if tech.type == TechnicalTypes.MA:
                df[metric] = rolling.mean()
            elif tech.type == TechnicalTypes.SUM:
                df[metric] = rolling.sum()
            elif tech.type == TechnicalTypes.BOLL:
                ma = rolling.mean()
                std = rolling.std()
                upper = metric + "_upper"
                lower = metric + "_lower"
                if metric in rounding:
                    # This adds some extra columns for the bounds, so we use the same rounding
                    # as the root metric if applicable.
                    df[upper] = round(ma + 2 * std, rounding[metric])
                    df[lower] = round(ma - 2 * std, rounding[metric])
                else:
                    df[upper] = ma + 2 * std
                    df[lower] = ma - 2 * std
            else:
                assert False, "Invalid technical type: %s" % tech.type
        return df

    def get_final_result(self, metrics, dimensions, row_filters, rollup, pivot):
        columns = []
        dimension_aliases = []

        for dim_name in dimensions:
            dim_def = self.warehouse.get_dimension(dim_name)
            columns.append(
                "%s as %s"
                % (dim_def.get_final_select_clause(self.warehouse), dim_def.name)
            )
            dimension_aliases.append(dim_def.name)

        technicals = {}
        rounding = {}
        for metric_name in metrics:
            metric_def = self.warehouse.get_metric(metric_name)
            if metric_def.technical:
                technicals[metric_def.name] = metric_def.technical
            if metric_def.rounding is not None:
                rounding[metric_def.name] = metric_def.rounding
            columns.append(
                "%s as %s"
                % (metric_def.get_final_select_clause(self.warehouse), metric_def.name)
            )

        sql = self.get_final_select_sql(columns, dimension_aliases)

        df = pd.read_sql(sql, self.conn, index_col=dimension_aliases or None)

        if row_filters:
            df = self.apply_row_filters(df, row_filters, metrics, dimensions)

        if technicals:
            df = self.apply_technicals(df, technicals, rounding)

        if rollup:
            df = self.apply_rollup(df, rollup, metrics, dimensions)

        if rounding:
            df = df.round(rounding)

        if pivot:
            df = df.unstack(pivot)

        return df

    def clean_up(self):
        drop_sql = "DROP TABLE IF EXISTS %s " % self.table_name
        self.cursor.execute(drop_sql)
        self.conn.commit()
        self.conn.close()


class Report:
    @initializer
    def __init__(
        self,
        warehouse,
        metrics=None,
        dimensions=None,
        criteria=None,
        row_filters=None,
        rollup=None,
        pivot=None,
    ):
        start = time.time()
        self.id = None
        self.metrics = self.metrics or []
        self.dimensions = self.dimensions or []
        assert (
            self.metrics or self.dimensions
        ), "One of metrics or dimensions must be specified for Report"
        self.criteria = self.criteria or []
        self.row_filters = self.row_filters or []

        if rollup is not None:
            assert dimensions, "Must specify dimensions in order to use rollup"
            if rollup != ROLLUP_TOTALS:
                assert is_int(rollup) and (0 < int(rollup) <= len(dimensions)), (
                    "Invalid rollup value: %s" % rollup
                )
                self.rollup = int(rollup)

        self.pivot = self.pivot or []
        if pivot:
            assert set(self.pivot).issubset(
                set(self.dimensions)
            ), "Pivot columms must be a subset of dimensions"

        self.ds_metrics = OrderedSet()
        self.ds_dimensions = OrderedSet()

        for metric_name in self.metrics:
            self.add_ds_fields(metric_name, FieldTypes.METRIC)

        for dim_name in self.dimensions:
            self.add_ds_fields(dim_name, FieldTypes.DIMENSION)

        self.queries = self.build_ds_queries()
        self.combined_query = None
        self.result = None
        dbg("Report init took %.3fs" % (time.time() - start))

    def get_params(self):
        used_datasources = list({q.get_datasource_name() for q in self.queries})
        datasources = [ds.get_params() for ds in self.warehouse.datasources]
        return dict(
            kwargs=dict(
                metrics=self.metrics,
                dimensions=self.dimensions,
                criteria=self.criteria,
                row_filters=self.row_filters,
                rollup=self.rollup,
                pivot=self.pivot,
            ),
            datasources=datasources,
            used_datasources=used_datasources,
        )

    def get_json(self):
        return json.dumps(self.get_params())

    def save(self):
        conn = zillion_engine.connect()
        try:
            result = conn.execute(Reports.insert(), params=self.get_json())
            report_id = result.inserted_primary_key[0]
            assert report_id, "No report ID found!"
        finally:
            conn.close()
        self.id = report_id
        return report_id

    @classmethod
    def load_params(cls, report_id):
        s = sa.select([Reports.c.params]).where(Reports.c.id == report_id)
        conn = zillion_engine.connect()
        try:
            result = conn.execute(s)
            row = result.fetchone()
            params = json.loads(row["params"])
            return params
        finally:
            conn.close()

    @classmethod
    def from_params(cls, warehouse, params):
        return Report(warehouse, **params["kwargs"])

    @classmethod
    def load(cls, warehouse, report_id):
        params = cls.load_params(report_id)
        # XXX TODO: if build fails, check whether its because expected datasources are missing
        result = cls.from_params(warehouse, params)
        return result

    @classmethod
    def delete(cls, report_id):
        s = Reports.delete().where(Reports.c.id == report_id)
        conn = zillion_engine.connect()
        try:
            result = conn.execute(s)
        finally:
            conn.close()

    def add_ds_fields(self, field_name, field_type):
        if field_type == FieldTypes.METRIC:
            field = self.warehouse.get_metric(field_name)
        elif field_type == FieldTypes.DIMENSION:
            field = self.warehouse.get_dimension(field_name)
        else:
            assert False, "Invalid field_type: %s" % field_type

        formula_fields, _ = field.get_formula_fields(self.warehouse) or (
            [field_name],
            None,
        )

        if field_type == FieldTypes.METRIC and field.weighting_metric:
            assert field.weighting_metric in self.warehouse.metrics, (
                "Could not find weighting metric %s in warehouse"
                % field.weighting_metric
            )
            self.ds_metrics.add(field.weighting_metric)

        for formula_field in formula_fields:
            if formula_field in self.warehouse.metrics:
                self.ds_metrics.add(formula_field)
            elif formula_field in self.warehouse.dimensions:
                self.ds_dimensions.add(formula_field)
            else:
                assert False, "Could not find field %s in warehouse" % formula_field

    def execute_ds_queries_sequential(self, queries):
        results = []
        for query in queries:
            result = query.execute()
            results.append(result)
        return results

    def execute_ds_queries_multithreaded(self, queries):
        # https://docs.python.org/3/library/concurrent.futures.html
        finished = {}

        with ThreadPoolExecutor(max_workers=len(queries)) as executor:
            # Note: if we eventually want to kill() a query on timeout so the thread returns immediately,
            # need to loop over futures and call future.result() rather than using as_completed, so we have
            # a ref to the timed out query in the loop
            # https://stackoverflow.com/questions/6509261/how-to-use-concurrent-futures-with-timeouts
            futures_map = {executor.submit(query.execute): query for query in queries}
            try:
                for future in as_completed(
                    futures_map, timeout=zillion_config["DATASOURCE_QUERY_TIMEOUT"]
                ):
                    data = future.result()
                    query = futures_map[future]
                    finished[future] = data
            except TimeoutError as e:
                error("TimeoutError: %s" % str(e))
                raise

        return finished.values()

    def execute_ds_queries(self, queries):
        mode = zillion_config["DATASOURCE_QUERY_MODE"]
        dbg("Executing %s datasource queries in %s mode" % (len(queries), mode))
        if mode == DataSourceQueryModes.SEQUENTIAL:
            return self.execute_ds_queries_sequential(queries)
        if mode == DataSourceQueryModes.MULTITHREADED:
            return self.execute_ds_queries_multithreaded(queries)
        assert False, "Invalid DATASOURCE_QUERY_MODE: %s" % mode

    def execute(self):
        start = very_start = time.time()
        ds_query_results = self.execute_ds_queries(self.queries)
        dbg("DataSource queries took %.3fs" % (time.time() - start))
        ds_query_summaries = [x.summary for x in ds_query_results]

        start = time.time()
        cr = self.create_combined_result(ds_query_results)
        dbg("Combined result took %.3fs" % (time.time() - start))

        try:
            start = time.time()
            final_result = cr.get_final_result(
                self.metrics, self.dimensions, self.row_filters, self.rollup, self.pivot
            )
            dbg(final_result)
            dbg("Final result took %.3fs" % (time.time() - start))
            self.result = ReportResult(
                final_result, time.time() - very_start, ds_query_summaries
            )
            return self.result
        finally:
            cr.clean_up()

    def get_grain(self):
        if not (self.ds_dimensions or self.criteria):
            return None
        grain = set()
        if self.ds_dimensions:
            grain = grain | set(self.ds_dimensions)
        if self.criteria:
            grain = grain | {x[0] for x in self.criteria}
        return grain

    def build_ds_queries(self):
        grain = self.get_grain()
        grain_errors = []
        queries = []

        def metric_covered_in_queries(metric):
            for query in queries:
                if query.covers_metric(metric):
                    query.add_metric(metric)
                    return query
            return False

        for metric in self.ds_metrics:
            existing_query = metric_covered_in_queries(metric)
            if existing_query:
                # TODO: we could do a single consolidation at the end instead
                # and that might get more optimal results
                dbg("Metric %s is covered by existing query" % metric)
                continue

            try:
                table_set = self.warehouse.get_metric_table_set(metric, grain)
            except UnsupportedGrainException as e:
                # Gather all grain errors to be raised in one exception
                grain_errors.append(str(e))
                continue

            query = DataSourceQuery(
                self.warehouse, [metric], self.ds_dimensions, self.criteria, table_set
            )
            queries.append(query)

        if grain_errors:
            raise UnsupportedGrainException(grain_errors)

        if not self.ds_metrics:
            dbg("No metrics requested, getting dimension table sets")
            table_set = self.warehouse.get_dimension_table_set(grain)
            query = DataSourceQuery(
                self.warehouse, None, self.ds_dimensions, self.criteria, table_set
            )
            queries.append(query)

        for query in queries:
            dbgsql(sqla_compile(query.select))

        return queries

    def create_combined_result(self, ds_query_results):
        return SQLiteMemoryCombinedResult(
            self.warehouse, ds_query_results, self.ds_dimensions
        )


class ReportResult(PrintMixin):
    repr_attrs = ["rowcount", "duration", "query_summaries"]

    @initializer
    def __init__(self, df, duration, query_summaries):
        self.duration = round(duration, 4)
        self.rowcount = len(df)

    def get_rollup_mask(self):
        # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Index.isin.html
        mask = None
        for i, level in enumerate(self.df.index.levels):
            if mask is None:
                mask = self.df.index.isin([ROLLUP_INDEX_LABEL], i)
            else:
                mask = mask | self.df.index.isin([ROLLUP_INDEX_LABEL], i)
        return mask

    def rollup_rows(self):
        return self.df.loc[self.get_rollup_mask()]

    def non_rollup_rows(self):
        return self.df.loc[~self.get_rollup_mask()]
