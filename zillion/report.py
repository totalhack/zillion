from collections import OrderedDict
from concurrent.futures import as_completed, ThreadPoolExecutor
from contextlib import contextmanager
import decimal
import inspect
import logging
import random
from sqlite3 import connect, Row
import threading
import time
import uuid

from orderedset import OrderedSet
from pymysql import escape_string
import pandas as pd
import sqlalchemy as sa
from stopit import TimeoutException, async_raise
from tlbx import (
    dbg,
    dbgsql,
    info,
    warn,
    error,
    get_class_var_values,
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
    UnsupportedGrainException,
    UnsupportedKillException,
    ReportException,
    FailedKillException,
    ExecutionKilledException,
    ExecutionLockException,
    ExecutionState,
    AggregationTypes,
    DataSourceQueryModes,
    DataSourceQueryTimeoutException,
    FieldTypes,
    TechnicalTypes,
)
from zillion.field import get_table_fields, get_table_field_column, FormulaField
from zillion.sql_utils import sqla_compile, get_sqla_clause, to_sqlite_type

logging.getLogger(name="stopit").setLevel(logging.ERROR)

if zillion_config["DEBUG"]:
    logging.getLogger().setLevel(logging.DEBUG)

# Last unicode char - this helps get the rollup rows to sort last, but may
# need to be replaced for presentation
ROLLUP_INDEX_LABEL = chr(1114111)
ROLLUP_INDEX_PRETTY_LABEL = "::"
ROLLUP_TOTALS = "totals"

ROW_FILTER_OPS = [">", ">=", "<", "<=", "==", "!=", "in", "not in"]

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


class ExecutionStateMixin:
    def __init__(self):
        self._lock = threading.RLock()
        self._state = None

    @property
    def ready(self):
        return self._state == ExecutionState.READY

    @property
    def querying(self):
        return self._state == ExecutionState.QUERYING

    @property
    def killed(self):
        return self._state == ExecutionState.KILLED

    @contextmanager
    def get_lock(self, timeout=None):
        timeout = timeout or -1  # convert to `acquire` default if falsey

        result = self._lock.acquire(timeout=timeout)
        if not result:
            raise ExecutionLockException("lock wait timeout after %.3fs" % timeout)

        try:
            yield
        finally:
            self._lock.release()

    def raise_if_killed(self, timeout=None):
        with self.get_lock(timeout=timeout):
            if self.killed:
                raise ExecutionKilledException

    def get_state(self):
        return self._state

    def set_state(
        self,
        state,
        timeout=None,
        assert_ready=False,
        raise_if_killed=False,
        set_if_killed=False,
    ):
        assert state in get_class_var_values(ExecutionState), (
            "Invalid state value: %s" % state
        )
        cls_name = self.__class__.__name__

        with self.get_lock(timeout=timeout):
            if assert_ready:
                assert self.ready, "%s: expected ready state, got: %s" % (
                    cls_name,
                    self._state,
                )

            if raise_if_killed:
                try:
                    self.raise_if_killed()
                except ExecutionKilledException:
                    if set_if_killed:
                        dbg(
                            "%s: state transition: %s -> %s"
                            % (cls_name, self._state, state)
                        )
                        self._state = state
                    raise

            dbg("%s: state transition: %s -> %s" % (cls_name, self._state, state))
            self._state = state


class DataSourceQuery(ExecutionStateMixin, PrintMixin):
    repr_attrs = ["metrics", "dimensions", "criteria"]

    @initializer
    def __init__(self, warehouse, metrics, dimensions, criteria, table_set):
        self._conn = None
        self.field_map = {}
        self.metrics = metrics or {}
        self.dimensions = dimensions or {}
        self.select = self.build_select()
        super().__init__()
        self.set_state(ExecutionState.READY)

    def get_datasource_name(self):
        return self.table_set.datasource.name

    def get_datasource(self):
        return self.table_set.datasource

    def format_query(self):
        return sqlformat(sqla_compile(self.select))

    def get_bind(self):
        ds = self.get_datasource()
        assert ds.metadata.bind, (
            'Datasource "%s" does not have metadata.bind set' % ds.name
        )
        return ds.metadata.bind

    def get_dialect_name(self):
        return self.get_bind().dialect.name

    def get_conn(self):
        bind = self.get_bind()
        conn = bind.connect()
        return conn

    def execute(self, timeout=None, label=None):
        start = time.time()
        is_timeout = False
        t = None

        self.set_state(ExecutionState.QUERYING, assert_ready=True)

        try:
            assert not self._conn, "Called execute with active query connection"
            self._conn = self.get_conn()

            if label:
                self.select = self.select.comment(label)

            try:
                info("\n" + self.format_query())

                def do_timeout(main_thread):
                    nonlocal is_timeout
                    is_timeout = True
                    self.kill(main_thread=main_thread)

                if timeout:
                    main_thread = threading.current_thread()
                    t = threading.Timer(timeout, lambda: do_timeout(main_thread))
                    t.start()

                try:
                    result = self._conn.execute(self.select)
                    data = result.fetchall()
                except Exception as e:
                    if not is_timeout:
                        raise

                    diff = time.time() - start
                    self._conn.invalidate()
                    raise DataSourceQueryTimeoutException(
                        "query timed out after %.3fs" % diff
                    )
                finally:
                    if t:
                        t.cancel()
            finally:
                try:
                    self._conn.close()
                except Exception as e:
                    warn("Exception on connection close: %s" % str(e))
                self._conn = None

            diff = time.time() - start
            info("Got %d rows in %.3fs" % (len(data), diff))
            return DataSourceQueryResult(self, data, diff)
        finally:
            raise_if_killed = False if is_timeout else True
            self.set_state(
                ExecutionState.READY,
                raise_if_killed=raise_if_killed,
                set_if_killed=True,
            )

    def kill(self, main_thread=None):
        # Note: if query finishes as this is processing, it should end up
        # hitting raise_if_killed once this code is done rather than causing
        # the underlying connection to die and raise an exception.
        with self.get_lock():
            if self.ready:
                warn("kill called on query that isn't running")
                return

            if self.killed:
                warn("kill called on query already being killed")
                return

            self.set_state(ExecutionState.KILLED)

        # I don't see how this could happen, but get loud if it does...
        assert self._conn, "Attempting to kill with no active query connection"
        raw_conn = self._conn.connection

        dialect = self.get_dialect_name()
        info("Attempting kill on %s conn: %s" % (dialect, self._conn))

        # TODO: add support for more connection libraries
        if dialect == "mysql" and callable(getattr(raw_conn, "thread_id", None)):
            kill_conn = self.get_conn()
            conn_id = raw_conn.thread_id()
            try:
                kill_conn.execute("kill {}".format(conn_id))
            finally:
                kill_conn.close()
        elif dialect == "sqlite" and callable(getattr(raw_conn, "interrupt", None)):
            raw_conn.interrupt()
        elif dialect == "postgresql" and callable(getattr(raw_conn, "cancel", None)):
            raw_conn.cancel()  # TODO: assumes psycopg2
        elif main_thread:
            # This isn't guaranteed to work as the thread may be waiting for
            # an external resource to finish, but worth a shot.
            info("Trying async raise for unsupported dialect=%s" % dialect)
            async_raise(main_thread.ident, ExecutionKilledException)
        else:
            raise UnsupportedKillException("No kill support for dialect=%s" % dialect)

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

    def get_field(self, name):
        if name in self.metrics:
            return self.metrics[name]
        elif name in self.dimensions:
            return self.dimensions[name]
        for row in self.criteria:
            if row[0].name == name:
                return row[0]
        assert False, "Could not find field for DataSourceQuery: %s" % name

    def column_for_field(self, field, table=None):
        ts = self.table_set

        if table is not None:
            column = get_table_field_column(
                ts.datasource.get_table(table.fullname), field
            )
        else:
            if ts.join and field in ts.join.field_map:
                column = ts.join.field_map[field]
            elif field in get_table_fields(
                ts.datasource.get_table(ts.ds_table.fullname)
            ):
                column = self.column_for_field(field, table=ts.ds_table)
            else:
                assert False, "Could not determine column for field %s" % field

        self.field_map[field] = column
        return column

    def get_field_expression(self, field, label=True):
        column = self.column_for_field(field)
        field_obj = self.get_field(field)
        return field_obj.get_ds_expression(column, label=label)

    def get_join(self):
        ts = self.table_set
        sqla_join = None
        last_table = None

        if not ts.join:
            return ts.ds_table

        for join_part in ts.join.join_parts:
            for table_name in join_part.table_names:
                table = ts.datasource.get_table(table_name)

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
                sqla_join = sqla_join.outerjoin(table, sa.and_(*tuple(conditions)))
                last_table = table

        return sqla_join

    def add_where(self, select):
        if not self.criteria:
            return select
        for row in self.criteria:
            field = row[0]
            expr = self.get_field_expression(field.name, label=False)
            clause = sa.and_(get_sqla_clause(expr, row))
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
        if field in self.table_set.get_covered_fields():
            return True
        return False

    def add_metric(self, name):
        assert self.covers_metric(name), "Metric %s can not be covered by query" % name
        # TODO: improve the way we maintain targeted metrics/dims
        self.table_set.target_fields.add(name)
        self.metrics[name] = self.table_set.datasource.get_metric(name)
        self.select = self.select.column(self.get_field_expression(name))


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
    def __init__(
        self, warehouse, ds_query_results, primary_ds_dimensions, adhoc_datasources=None
    ):
        self.conn = self.get_conn()
        self.cursor = self.get_cursor(self.conn)
        self.adhoc_datasources = adhoc_datasources or []
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
            for dim_name, dim in qr.query.dimensions.items():
                if dim_name in dimensions:
                    continue
                dimensions[dim_name] = dim

            for metric_name, metric in qr.query.metrics.items():
                if metric_name in metrics:
                    continue
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
        create_sql = escape_string(create_sql)
        dbg(create_sql)  # Creates don't pretty print well with dbgsql?

        self.cursor.execute(create_sql)
        if self.primary_ds_dimensions:
            index_sql = "CREATE INDEX idx_dims ON %s (%s)" % (
                self.table_name,
                ", ".join(self.primary_ds_dimensions),
            )
            index_sql = escape_string(index_sql)
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

        return escape_string(sql), values

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
        info("\n" + sqlformat(sql))
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
        dim_names = list(dimensions.keys())

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
                for dim in dim_names[level + 1 :]:
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

        for metric in metrics.values():
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
            result = tech.apply(df, metric)

            if tech.type == TechnicalTypes.BOLL:
                assert len(result) == 2, (
                    "Expected two items in %s technical result" % tech.type
                )
                lower = metric + "_lower"
                upper = metric + "_upper"
                if metric in rounding:
                    # This adds some extra columns for the bounds, so we use
                    # the same rounding as the root metric if applicable.
                    df[lower] = round(result[0], rounding[metric])
                    df[upper] = round(result[1], rounding[metric])
                else:
                    df[lower] = result[0]
                    df[upper] = result[1]
            else:
                df[metric] = result

        return df

    def get_final_result(self, metrics, dimensions, row_filters, rollup, pivot):
        start = time.time()
        columns = []
        dimension_aliases = []

        for dim in dimensions.values():
            columns.append(
                "%s as %s"
                % (
                    dim.get_final_select_clause(
                        self.warehouse, adhoc_fms=self.adhoc_datasources
                    ),
                    dim.name,
                )
            )
            dimension_aliases.append(dim.name)

        technicals = {}
        rounding = {}
        for metric in metrics.values():
            if metric.technical:
                technicals[metric.name] = metric.technical
            if metric.rounding is not None:
                rounding[metric.name] = metric.rounding
            columns.append(
                "%s as %s"
                % (
                    metric.get_final_select_clause(
                        self.warehouse, adhoc_fms=self.adhoc_datasources
                    ),
                    metric.name,
                )
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

        dbg(df)
        dbg("Final result took %.3fs" % (time.time() - start))
        return df

    def clean_up(self):
        drop_sql = "DROP TABLE IF EXISTS %s " % self.table_name
        self.cursor.execute(drop_sql)
        self.conn.commit()
        self.conn.close()


class Report(ExecutionStateMixin):
    def __init__(
        self,
        warehouse,
        metrics=None,
        dimensions=None,
        criteria=None,
        row_filters=None,
        rollup=None,
        pivot=None,
        adhoc_datasources=None,
    ):
        start = time.time()
        self.id = None  # TODO: consider renaming spec_id or save_id
        self.uuid = uuid.uuid1()
        self.warehouse = warehouse

        if adhoc_datasources:
            self.warehouse.run_integrity_checks(adhoc_datasources=adhoc_datasources)

        self._requested_metrics = metrics
        self._requested_dimensions = dimensions
        self._requested_criteria = criteria

        self.metrics = self._get_fields_dict(
            metrics, FieldTypes.METRIC, adhoc_datasources=adhoc_datasources
        )
        self.dimensions = self._get_fields_dict(
            dimensions, FieldTypes.DIMENSION, adhoc_datasources=adhoc_datasources
        )

        assert (
            self.metrics or self.dimensions
        ), "One of metrics or dimensions must be specified for Report"

        self.criteria = self._populate_criteria_fields(
            criteria or [], adhoc_datasources=adhoc_datasources
        )
        self.row_filters = row_filters or []

        self.rollup = None
        if rollup is not None:
            assert dimensions, "Must specify dimensions in order to use rollup"
            if rollup != ROLLUP_TOTALS:
                assert is_int(rollup) and (0 < int(rollup) <= len(dimensions)), (
                    "Invalid rollup value: %s" % rollup
                )
                self.rollup = int(rollup)
            else:
                self.rollup = rollup

        self.pivot = pivot or []
        if pivot:
            assert set(self.pivot).issubset(
                set(self.dimensions)
            ), "Pivot columms must be a subset of dimensions"

        self.adhoc_datasources = adhoc_datasources or []

        self.ds_metrics = OrderedDict()
        self.ds_dimensions = OrderedDict()

        for metric in self.metrics.values():
            self.add_ds_fields(metric)

        for dim in self.dimensions.values():
            self.add_ds_fields(dim)

        self.check_required_grain()
        self.queries = self.build_ds_queries()
        self.combined_query = None
        self.result = None

        super().__init__()
        self.set_state(ExecutionState.READY)
        dbg("Report init took %.3fs" % (time.time() - start))

    def get_params(self):
        used_datasources = list({q.get_datasource_name() for q in self.queries})
        datasources = [ds.get_params() for ds in self.warehouse.get_datasources()]
        return dict(
            kwargs=dict(
                metrics=self._requested_metrics,
                dimensions=self._requested_dimensions,
                criteria=self._requested_criteria,
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
    def from_params(cls, warehouse, params, adhoc_datasources=None):
        used_dses = set(params.get("used_datasources", []))
        wh_dses = warehouse.get_datasource_names()
        all_dses = set(wh_dses) | set([x.name for x in (adhoc_datasources or [])])
        if not used_dses.issubset(all_dses):
            raise ReportException(
                "Report requires datasources that are not present: %s Found: %s"
                % (used_dses - all_dses, all_dses)
            )
        return Report(
            warehouse, **params["kwargs"], adhoc_datasources=adhoc_datasources
        )

    @classmethod
    def load(cls, warehouse, report_id, adhoc_datasources=None):
        params = cls.load_params(report_id)
        result = cls.from_params(warehouse, params, adhoc_datasources=adhoc_datasources)
        return result

    @classmethod
    def delete(cls, report_id):
        s = Reports.delete().where(Reports.c.id == report_id)
        conn = zillion_engine.connect()
        try:
            result = conn.execute(s)
        finally:
            conn.close()

    def _get_fields_dict(self, names, field_type, adhoc_datasources=None):
        d = OrderedDict()
        for name in names or []:
            if field_type == FieldTypes.METRIC:
                field = self.warehouse.get_metric(name, adhoc_fms=adhoc_datasources)
            elif field_type == FieldTypes.DIMENSION:
                field = self.warehouse.get_dimension(name, adhoc_fms=adhoc_datasources)
            else:
                assert False, "Invalid field type: %s" % field_type
            d[field.name] = field
        return d

    def _populate_criteria_fields(self, criteria, adhoc_datasources=None):
        field_names = [row[0] for row in criteria]
        fields_dict = self._get_fields_dict(
            field_names, FieldTypes.DIMENSION, adhoc_datasources=adhoc_datasources
        )
        final_criteria = []
        for row in criteria:
            row = list(row)
            field = fields_dict[row[0]]  # Replace field name with field object
            if isinstance(field, FormulaField):
                raise ReportException(
                    "FormulaFields are not allowed in criteria: %s" % field.name
                )
            row[0] = field
            final_criteria.append(row)
        return final_criteria

    def add_ds_fields(self, field):
        formula_fields, _ = field.get_formula_fields(
            self.warehouse, adhoc_fms=self.adhoc_datasources
        )
        if not formula_fields:
            formula_fields = [field.name]

        if field.field_type == FieldTypes.METRIC and field.weighting_metric:
            weighting_field = self.warehouse.get_metric(
                field.weighting_metric, adhoc_fms=self.adhoc_datasources
            )
            self.ds_metrics[field.weighting_metric] = weighting_field

        for formula_field in formula_fields:
            if formula_field == field.name:
                if field.field_type == FieldTypes.METRIC:
                    self.ds_metrics[formula_field] = field
                elif field.field_type == FieldTypes.DIMENSION:
                    self.ds_dimensions[formula_field] = field
                else:
                    assert False, "Invalid field_type: %s" % field.field_type
                continue

            if self.warehouse.has_metric(
                formula_field, adhoc_fms=self.adhoc_datasources
            ):
                self.ds_metrics[formula_field] = self.warehouse.get_metric(
                    formula_field, adhoc_fms=self.adhoc_datasources
                )
            elif self.warehouse.has_dimension(
                formula_field, adhoc_fms=self.adhoc_datasources
            ):
                self.ds_dimensions[formula_field] = self.warehouse.get_dimension(
                    formula_field, adhoc_fms=self.adhoc_datasources
                )
            else:
                assert False, "Could not find field %s in warehouse" % formula_field

    def get_query_label(self, query_label):
        return "Report: %s | Query: %s" % (str(self.uuid), query_label)

    def execute_ds_queries_sequential(self, queries):
        results = []
        timeout = zillion_config["DATASOURCE_QUERY_TIMEOUT"]
        for i, query in enumerate(queries):
            self.raise_if_killed()
            label = self.get_query_label("%s / %s" % (i + 1, len(queries)))
            result = query.execute(timeout=timeout, label=label)
            results.append(result)
        return results

    def execute_ds_queries_multithread(self, queries):
        # TODO: currently if any query times out, the entire report fails.
        # It might be better if partial results could be returned.
        finished = {}
        timeout = zillion_config["DATASOURCE_QUERY_TIMEOUT"]
        workers = zillion_config.get("DATASOURCE_QUERY_WORKERS", len(queries))

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures_map = {}
            for i, query in enumerate(queries):
                label = self.get_query_label("%s / %s" % (i + 1, len(queries)))
                future = executor.submit(query.execute, timeout=timeout, label=label)
                futures_map[future] = query

            for future in as_completed(futures_map):
                data = future.result()
                query = futures_map[future]
                finished[future] = data

        return finished.values()

    def execute_ds_queries(self, queries):
        mode = zillion_config["DATASOURCE_QUERY_MODE"]
        dbg("Executing %s datasource queries in %s mode" % (len(queries), mode))
        start = time.time()
        if mode == DataSourceQueryModes.SEQUENTIAL:
            return self.execute_ds_queries_sequential(queries)
        if mode == DataSourceQueryModes.MULTITHREAD:
            return self.execute_ds_queries_multithread(queries)
        dbg("DataSource queries took %.3fs" % (time.time() - start))
        assert False, "Invalid DATASOURCE_QUERY_MODE: %s" % mode

    def execute(self):
        start = time.time()

        self.set_state(ExecutionState.QUERYING, assert_ready=True)

        try:
            query_results = self.execute_ds_queries(self.queries)
            summaries = [x.summary for x in query_results]

            self.raise_if_killed()
            cr = self.create_combined_result(query_results)

            try:
                self.raise_if_killed()
                final_result = cr.get_final_result(
                    self.metrics,
                    self.dimensions,
                    self.row_filters,
                    self.rollup,
                    self.pivot,
                )
                diff = time.time() - start
                self.result = ReportResult(final_result, diff, summaries)
                return self.result
            finally:
                cr.clean_up()
        finally:
            self.set_state(
                ExecutionState.READY, raise_if_killed=True, set_if_killed=True
            )

    def kill(self, soft=False, raise_if_failed=False):
        info("killing report %s" % self.uuid)

        with self.get_lock():
            if self.ready:
                warn("kill called on report that isn't running")
                return

            if self.killed:
                warn("kill called on report already being killed")
                return

            if soft:
                self.set_state(ExecutionState.KILLED)
                return

            querying = self.querying  # grab state before changing it
            self.set_state(ExecutionState.KILLED)

            if querying:
                dbg("attempting kill on %d report queries" % len(self.queries))
                exceptions = []

                for query in self.queries:
                    try:
                        query.kill()
                    except Exception as e:
                        setattr(e, "query", query)
                        exceptions.append(e)

                if exceptions:
                    if raise_if_failed:
                        raise FailedKillException(exceptions)
                    else:
                        warn("failed to kill some queries: %s" % exceptions)

    def get_grain(self):
        if not (self.ds_dimensions or self.criteria):
            return None
        grain = set()
        if self.ds_dimensions:
            grain = grain | self.ds_dimensions.keys()
        if self.criteria:
            grain = grain | {x[0].name for x in self.criteria}
        return grain

    def check_required_grain(self):
        grain = self.get_grain()
        grain_errors = []

        for metric in self.metrics.values():
            if metric.required_grain:
                if not set(metric.required_grain).issubset(grain):
                    grain_errors.append(
                        "Grain %s is not a superset of required_grain %s for metric: %s"
                        % (grain, metric.required_grain, metric.name)
                    )

        for metric in self.ds_metrics.values():
            if metric.required_grain:
                if not set(metric.required_grain).issubset(grain):
                    grain_errors.append(
                        "Grain %s is not a superset of required_grain %s for metric: %s"
                        % (grain, metric.required_grain, metric.name)
                    )

        if grain_errors:
            raise UnsupportedGrainException(grain_errors)

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

        for metric_name, metric in self.ds_metrics.items():
            existing_query = metric_covered_in_queries(metric_name)
            if existing_query:
                # TODO: we could do a single consolidation at the end instead
                # and that might get more optimal results
                dbg("Metric %s is covered by existing query" % metric_name)
                continue

            try:
                table_set = self.warehouse.get_metric_table_set(
                    metric_name, grain, adhoc_datasources=self.adhoc_datasources
                )
            except UnsupportedGrainException as e:
                # Gather all grain errors to be raised in one exception
                grain_errors.append(str(e))
                continue

            query = DataSourceQuery(
                self.warehouse,
                {metric.name: metric},
                self.ds_dimensions,
                self.criteria,
                table_set,
            )
            queries.append(query)

        if grain_errors:
            raise UnsupportedGrainException(grain_errors)

        if not self.ds_metrics:
            dbg("No metrics requested, getting dimension table sets")
            table_set = self.warehouse.get_dimension_table_set(
                grain, adhoc_datasources=self.adhoc_datasources
            )
            query = DataSourceQuery(
                self.warehouse, None, self.ds_dimensions, self.criteria, table_set
            )
            queries.append(query)

        for query in queries:
            dbgsql(sqla_compile(query.select))

        return queries

    def create_combined_result(self, ds_query_results):
        start = time.time()
        result = SQLiteMemoryCombinedResult(
            self.warehouse,
            ds_query_results,
            list(self.ds_dimensions.keys()),
            adhoc_datasources=self.adhoc_datasources,
        )
        dbg("Combined result took %.3fs" % (time.time() - start))
        return result


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
