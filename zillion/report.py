from collections import OrderedDict
from concurrent.futures import as_completed, ThreadPoolExecutor
from contextlib import contextmanager
import decimal
import logging
import random
from sqlite3 import connect, Row
import threading
import time
import uuid

import numpy as np
from pymysql import escape_string
import pandas as pd
import sqlalchemy as sa
from stopit import async_raise
from tlbx import is_int

from zillion.configs import zillion_config, default_field_display_name
from zillion.core import *
from zillion.field import (
    get_table_fields,
    get_table_field_column,
    FormulaField,
    FIELD_VALUE_CHECK_OPERATIONS,
)
from zillion.model import zillion_engine, ReportSpecs
from zillion.sql_utils import (
    sqla_compile,
    get_sqla_criterion_expr,
    to_sqlite_type,
    type_string_to_sa_type,
)

logging.getLogger(name="stopit").setLevel(logging.ERROR)

# Last unicode char - this helps get the rollup rows to sort last, but may
# need to be replaced for presentation.
ROLLUP_INDEX_LABEL = chr(1114111)
# This is more friendly for front-end viewing, but has a better chance of
# conflicting with actual report data.
ROLLUP_INDEX_DISPLAY_LABEL = "::"

PANDAS_ROLLUP_AGGR_TRANSLATION = {
    AggregationTypes.COUNT: "sum",
    AggregationTypes.COUNT_DISTINCT: "sum",
}


class ExecutionStateMixin:
    """A mixin to manage the state of a report or query"""

    def __init__(self):
        self._lock = threading.RLock()
        self._state = None

    @property
    def _ready(self):
        """Return True if in the ready state"""
        return self._state == ExecutionState.READY

    @property
    def _querying(self):
        """Return True if in the querying state"""
        return self._state == ExecutionState.QUERYING

    @property
    def _killed(self):
        """Return True if in the killed state"""
        return self._state == ExecutionState.KILLED

    @contextmanager
    def _get_lock(self, timeout=None):
        """Acquire the lock for this object
        
        **Parameters:**
        
        * **timeout** - (*float, optional*) A timeout to wait trying to acquire
        the lock
        
        """
        timeout = timeout or -1  # convert to `acquire` default if falsey

        result = self._lock.acquire(timeout=timeout)
        if not result:
            raise ExecutionLockException("lock wait timeout after %.3fs" % timeout)

        try:
            yield
        finally:
            self._lock.release()

    def _raise_if_killed(self, timeout=None):
        """Raise an exception if in the killed state
        
        **Parameters:**
        
        * **timeout** - (*float, optional*) A timeout to wait trying to acquire
        the lock
        
        """
        with self._get_lock(timeout=timeout):
            if self._killed:
                raise ExecutionKilledException

    def _get_state(self):
        """Get the current object state"""
        return self._state

    def _set_state(
        self,
        state,
        timeout=None,
        assert_ready=False,
        raise_if_killed=False,
        set_if_killed=False,
    ):
        """Set the current object state
        
        **Parameters:**
        
        * **state** - (*str*) A valid ExecutionState
        * **timeout** - (*float, optional*) A timeout to wait trying to acquire
        the lock
        * **assert_ready** - (*bool, optional*) Raise an exception if not in the
        ready state when called
        * **raise_if_killed** - (*bool, optional*) Raise an exception if in the
        killed state
        * **set_if_killed** - (*bool, optional*) Set the execution state even if
        killed
        
        """
        raiseifnot(
            state in get_class_var_values(ExecutionState),
            "Invalid state value: %s" % state,
        )
        cls_name = self.__class__.__name__

        with self._get_lock(timeout=timeout):
            if assert_ready:
                raiseifnot(
                    self._ready,
                    "%s: expected ready state, got: %s" % (cls_name, self._state),
                )

            if raise_if_killed:
                try:
                    self._raise_if_killed()
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
    """Build a query to run against a particular datasource
    
    **Parameters:**
    
    * **warehouse** - (*Warehouse*) A zillion warehouse
    * **metrics** - (*OrderedDict*) An OrderedDict mapping metric names to
    Metric objects
    * **dimensions** - (*OrderedDict*) An OrderedDict mapping dimension names to
    Dimension objects
    * **criteria** - (*list*) A list of criteria to be applied when querying.
    See the Report docs for more details.
    * **table_set** - (*TableSet*) Build the query against this set of tables
    that supports the requested metrics and grain
    
    """

    repr_attrs = ["metrics", "dimensions", "criteria"]

    @initializer
    def __init__(self, warehouse, metrics, dimensions, criteria, table_set):
        self._conn = None
        self.field_map = {}
        self.metrics = metrics or {}
        self.dimensions = dimensions or {}
        self.select = self._build_select()
        super().__init__()
        self._set_state(ExecutionState.READY)

    def get_datasource(self):
        """Get a reference to the datasource for this query"""
        return self.table_set.datasource

    def get_datasource_name(self):
        """Get the name of the datasource used in this query"""
        return self.get_datasource().name

    def get_dialect_name(self):
        """Get the name of the datasource dialect"""
        return self._get_bind().dialect.name

    def covers_metric(self, metric):
        """Check whether a metric is covered in this query
        
        **Parameters:**
        
        * **metric** - (*str*) A metric name
        
        **Returns:**
        
        (*bool*) - True if this metric is covered in this query
        
        """
        if metric in self.table_set.get_covered_metrics(self.warehouse):
            return True
        return False

    def covers_field(self, field):
        """Check whether a field is covered in this query
        
        **Parameters:**
        
        * **field** - (*str*) A field name
        
        **Returns:**
        
        (*bool*) - True if this field is covered in this query
        
        """
        if field in self.table_set.get_covered_fields():
            return True
        return False

    def add_metric(self, metric):
        """Add a metric to this query
        
        **Parameters:**
        
        * **metric** - (*str*) A metric name
        
        """
        raiseifnot(
            self.covers_metric(metric), "Metric %s can not be covered by query" % metric
        )
        self.table_set.target_fields.add(metric)
        # TODO: this implies metrics defined on multiple levels will
        # favor warehouse-level definition.
        self.metrics[metric] = self.warehouse.get_metric(metric)
        self.select = self.select.column(self._get_field_expression(metric))

    def get_conn(self):
        """Get a connection to this query's datasource"""
        bind = self._get_bind()
        conn = bind.connect()
        return conn

    def execute(self, timeout=None, label=None):
        """Execute the datasource query
        
        **Parameters:**
        
        * **timeout** - (*float, optional*) A query timeout in seconds
        * **label** - (*str, optional*) A label to apply to the SQL query
        
        **Returns:**
        
        (*DataSourceQueryResult*) - The result of the SQL query
        
        """
        start = time.time()
        is_timeout = False
        t = None

        self._set_state(ExecutionState.QUERYING, assert_ready=True)

        try:
            raiseif(self._conn, "Called execute with active query connection")
            self._conn = self.get_conn()

            if label:
                self.select = self.select.comment(label)

            try:
                info("\n" + self._format_query())

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
            raise_if_killed = not is_timeout
            self._set_state(
                ExecutionState.READY,
                raise_if_killed=raise_if_killed,
                set_if_killed=True,
            )

    def kill(self, main_thread=None):
        """Kill this datasource query
        
        **Parameters:**
        
        * **main_thread** - (*Thread, optional*) A reference to the thread that
        started the query. This is used as a backup for dialects that don't have
        a supported way to kill a query. An exception will be asynchronously
        raised in this thread. It is not guaranteed to actually interrupt the
        query.
        
        """
        with self._get_lock():
            if self._ready:
                warn("kill called on query that isn't running")
                return

            if self._killed:
                warn("kill called on query already being killed")
                return

            self._set_state(ExecutionState.KILLED)

        # I don't see how this could happen, but get loud if it does...
        raiseifnot(self._conn, "Attempting to kill with no active query connection")
        raw_conn = self._conn.connection

        dialect = self.get_dialect_name()
        info("Attempting kill on %s conn: %s" % (dialect, self._conn))

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

    def _format_query(self):
        """Return a formatted query string"""
        return sqlformat(sqla_compile(self.select))

    def _get_bind(self):
        """Get a connection to the datasource"""
        ds = self.get_datasource()
        raiseifnot(
            ds.metadata.bind,
            'Datasource "%s" does not have metadata.bind set' % ds.name,
        )
        return ds.metadata.bind

    def _build_select(self):
        """Build the select for this datasource query"""
        # https://docs.sqlalchemy.org/en/latest/core/selectable.html
        select = sa.select()

        join = self._get_join()
        select = select.select_from(join)

        for dimension in self.dimensions:
            select = select.column(self._get_field_expression(dimension))

        for metric in self.metrics:
            select = select.column(self._get_field_expression(metric))

        select = self._add_where(select)
        select = self._add_group_by(select)
        return select

    def _get_field(self, name):
        """Get a reference to a field that is part of this query
        
        **Parameters:**
        
        * **name** - (*str*) A field name
        
        **Returns:**
        
        (*Field*) - A Field object
        
        """
        if name in self.metrics:
            return self.metrics[name]

        if name in self.dimensions:
            return self.dimensions[name]

        for row in self.criteria:
            if row[0].name == name:
                return row[0]

        raise ZillionException("Could not find field for DataSourceQuery: %s" % name)

    def _column_for_field(self, field, table=None):
        """Get the column that will be providing this field
        
        **Parameters:**
        
        * **field** - (*str*) A field name
        * **table** - (*Table, optional*) Limit the search to this table
        
        **Returns:**
        
        (*SQLALchemy column*) - The table column that provides this field
        
        """
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
                column = self._column_for_field(field, table=ts.ds_table)
            else:
                raise ZillionException(
                    "Could not determine column for field %s" % field
                )

        self.field_map[field] = column
        return column

    def _get_field_expression(self, field, label=True):
        """Get the expression for this field
        
        **Parameters:**
        
        * **field** - (*str*) A field name to get an expression for
        * **label** - (*bool, optional*) If True, label the expression with the
        field name
        
        **Returns:**
        
        (*str*) - A string representing the field SQL expression
        
        """
        column = self._column_for_field(field)
        field_obj = self._get_field(field)
        return field_obj.get_ds_expression(column, label=label)

    def _get_join(self):
        """Get a SQLAlchemy join for this query"""
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
                    last_column = self._column_for_field(field, table=last_table)
                    column = self._column_for_field(field, table=table)
                    conditions.append(column == last_column)
                sqla_join = sqla_join.outerjoin(table, sa.and_(*tuple(conditions)))
                last_table = table

        return sqla_join

    def _convert_criteria(self, field, conversion, value):
        """Convert the values of a criteria according to the conversion formulas
        provided for this field. A single criteria may expand into multiple criteria."""
        final_criteria = []
        for new_op, new_values in conversion:
            if not isinstance(new_values, (list, tuple)):
                new_values = [new_values]

            fmt_values = []
            for new_value in new_values:
                # Substitute the original value(s) into the formula
                if not isinstance(value, (list, tuple)):
                    orig_values = [value]
                else:
                    orig_values = value[:]
                new_value = sa.text(new_value)
                if new_value._bindparams:
                    value_map = {str(i): v for i, v in enumerate(orig_values)}
                    new_value = new_value.bindparams(
                        **{
                            str(i): v
                            for i, v in enumerate(orig_values)
                            if str(i) in new_value._bindparams
                        }
                    )
                fmt_values.append(new_value)
            final_criteria.append((field.name, new_op, fmt_values))
        return final_criteria

    def _add_where(self, select):
        """Add a where clause to a SQLAlchemy select"""
        if not self.criteria:
            return select

        for row in self.criteria:
            # A single criteria may be converted into multiple criteria
            # with column criteria conversions.
            field, op, value = row
            column = self._column_for_field(field.name)

            if op in FIELD_VALUE_CHECK_OPERATIONS and not field.is_valid_value(
                self.warehouse.id, value
            ):
                raise InvalidDimensionValueException(
                    "Invalid criteria value '%s' for dimension '%s'"
                    % (value, field.name)
                )

            conv = column.zillion.get_criteria_conversion(field.name, op)
            if conv:
                expr = field.get_ds_expression(column, label=False, ignore_formula=True)
                final_criteria = self._convert_criteria(field, conv, value)
            else:
                expr = self._get_field_expression(field.name, label=False)
                final_criteria = [row]

            for criteria in final_criteria:
                criterion = sa.and_(get_sqla_criterion_expr(expr, criteria))
                select = select.where(criterion)

        return select

    def _add_group_by(self, select):
        """Add a group by clause to a SQLAlchemy select"""
        if not self.dimensions:
            return select
        return select.group_by(
            *[sa.text(str(x)) for x in range(1, len(self.dimensions) + 1)]
        )

    def _add_order_by(self, select, asc=True):
        """Add an order by clause to a SQLAlchemy select"""
        if not self.dimensions:
            return select
        order_func = sa.asc
        if not asc:
            order_func = sa.desc
        return select.order_by(*[order_func(sa.text(x)) for x in self.dimensions])


class DataSourceQuerySummary(PrintMixin):
    """A summary of the execution results for a DataSourceQuery
    
    **Parameters:**
    
    * **query** - (*DataSourceQuery*) The DataSourceQuery that was executed
    * **data** - (*iterable*) The result rows
    * **duration** - (*float*) The duration of the query execution in seconds
    
    """

    repr_attrs = ["datasource_name", "rowcount", "duration"]

    def __init__(self, query, data, duration):
        self.datasource_name = query.get_datasource_name()
        self.metrics = query.metrics
        self.dimensions = query.dimensions
        self.select = query.select
        self.duration = round(duration, 4)
        self.rowcount = len(data)

    def format(self):
        """Return a formatted summary of the DataSourceQuery results"""
        sql = self._format_query()
        parts = [
            "%s" % sql,
            "\n%d rows in %.4f seconds" % (self.rowcount, self.duration),
            "Datasource: %s" % self.datasource_name,
            "Metrics: %s" % list(self.metrics),
            "Dimensions: %s" % list(self.dimensions),
        ]
        return "\n".join(parts)

    def _format_query(self):
        """Return a formatted SQL query for the select that was executed"""
        return sqlformat(sqla_compile(self.select))


class DataSourceQueryResult(PrintMixin):
    """The results for a DataSourceQuery
    
    **Parameters:**
    
    * **query** - (*DataSourceQuery*) The DataSourceQuery that was executed
    * **data** - (*iterable*) The result rows
    * **duration** - (*float*) The duration of the query execution in seconds
    
    """

    repr_attrs = ["summary"]

    def __init__(self, query, data, duration):
        self.query = query
        self.data = data
        self.summary = DataSourceQuerySummary(query, data, duration)


class BaseCombinedResult:
    """A combination of datasource query results
    
    **Parameters:**
    
    * **warehouse** - (*Warehouse*) A zillion warehouse
    * **ds_query_results** - (*list*) A list of DataSourceQueryResult objects
    * **primary_ds_dimensions** - (*list*) A list of dimensions that will be
    used to create the hash primary key of the combined result table
    * **adhoc_datasources** - (*list, optional*) A list of FieldManagers
    specific to this combined result
    
    """

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
        self.ds_dimensions, self.ds_metrics = self._get_fields()
        self.create_table()
        self.load_table()

    def get_conn(self):
        """Get a database connection to the combined result database"""
        raise NotImplementedError

    def get_cursor(self, conn):
        """Get a cursor from a database connection"""
        raise NotImplementedError

    def create_table(self):
        """Create the combined result table"""
        raise NotImplementedError

    def load_table(self):
        """Load the combined result table"""
        raise NotImplementedError

    def clean_up(self):
        """Clean up any resources that can/should be cleaned up"""
        raise NotImplementedError

    def get_final_result(
        self,
        metrics,
        dimensions,
        row_filters,
        rollup,
        pivot,
        order_by,
        limit,
        limit_first,
    ):
        """Get the final result from the combined result table"""
        raise NotImplementedError

    def _get_row_hash(self, row):
        """Get a hash representing a primary key for the row. The default
        implementation simply uses the builtin `hash` function which will only
        provide consistent results in the same python process. This will also
        impact performance for very large results.
        
        **Parameters:**
        
        * **row** - (*iterable*) An iterable of query result rows. It is assumed
        the primary datasource dimensions are the first columns in each row, and
        that the column order is consistent between rows.
        
        **Returns:**
        
        (*int*) - A hash value representing a key for this row.
        
        """
        return hash(row[: len(self.primary_ds_dimensions)])

    def _get_fields(self):
        """Returns a 2-item tuple of dimension and metric dicts"""
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

    def _get_field_names(self):
        """Get a list of field names in this combined result"""
        dims, metrics = self._get_fields()
        return list(dims.keys()) + list(metrics.keys())


class SQLiteMemoryCombinedResult(BaseCombinedResult):
    """Combine query results in an in-memory SQLite database"""

    def get_conn(self):
        """Get a SQLite memory database connection"""
        return connect(":memory:")

    def get_cursor(self, conn):
        """Get a SQLite cursor from the connection"""
        conn.row_factory = Row
        return conn.cursor()

    def create_table(self):
        """Create a table in the SQLite database to store the combined result"""
        create_sql = "CREATE TEMP TABLE %s (" % self.table_name
        column_clauses = ["row_hash BIGINT NOT NULL PRIMARY KEY"]

        for field_name, field in self.ds_dimensions.items():
            type_str = str(to_sqlite_type(field.sa_type))
            clause = "%s %s DEFAULT NULL" % (field_name, type_str)
            column_clauses.append(clause)

        for field_name, field in self.ds_metrics.items():
            type_str = str(to_sqlite_type(field.sa_type))
            clause = "%s %s DEFAULT NULL" % (field_name, type_str)
            column_clauses.append(clause)

        create_sql += ", ".join(column_clauses)
        create_sql += ") WITHOUT ROWID"
        create_sql = escape_string(create_sql)
        dbg(create_sql)

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

    def load_table(self):
        """Load the combined result table"""
        for qr in self.ds_query_results:
            for rows in chunks(qr.data, zillion_config["LOAD_TABLE_CHUNK_SIZE"]):
                insert_sql, values = self._get_bulk_insert_sql(rows)
                self.cursor.executemany(insert_sql, values)
            self.conn.commit()

    def get_final_result(
        self,
        metrics,
        dimensions,
        row_filters,
        rollup,
        pivot,
        order_by,
        limit,
        limit_first,
    ):
        """Get the final reseult from the combined result table
        
        **Parameters:**
        
        * **metrics** - (*OrderedDict*) An OrderedDict mapping metric names to
        Metric objects
        * **dimensions** - (*OrderedDict*) An OrderedDict mapping dimension
        names to Dimension objects
        * **row_filters** - (*list*) A list of criteria to filter which rows get
        returned
        * **rollup** - (*str or int*) Controls how metrics are rolled up /
        aggregated by dimension. See the Report docs for more details.
        * **pivot** - (*list*) A list of dimensions to pivot to columns
        * **order_by** - (*list*) A list of (field, asc/desc) tuples that
        control the ordering of the returned result
        * **limit** - (*int*) A limit on the number of rows returned
        * **limit_first** - (*bool, optional*) Whether to apply limits before
        rollups/ordering
        
        **Returns:**
        
        (*DataFrame*) - A DataFrame with the final report result
        
        **Notes:**
        
        The default ordering of operations is meant to roughly parallel that of
        MySQL's rollup, having, order by and limit behavior. The operations
        are applied in the following order: technicals, rollups, rounding,
        order_by, row_filters, limit, pivot. If you set `limit_first=True`
        the the row_filter and limit operations are moved ahead of the rollups:
        technicals, row_filters, limit, rollups, rounding, order_by, pivot.
        
        """
        start = time.time()
        columns = []
        dimension_aliases = []
        custom_sorts = []

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
            if dim.sorter:
                custom_sorts.append((dim.name, OrderByTypes.ASC))

        if custom_sorts and not order_by:
            # We still need to do ordering even if no order_by was specified
            # if some of the dimensions use custom sorting.
            order_by = custom_sorts

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

        sql = self._get_final_select_sql(columns, dimension_aliases)

        df = pd.read_sql(sql, self.conn, index_col=dimension_aliases or None)

        if technicals and not df.empty:
            df = self._apply_technicals(df, technicals, rounding)

        if limit_first:
            df = self._apply_limits(df, row_filters, limit, metrics, dimensions)

        if rollup and not df.empty:
            df = self._apply_rollup(df, rollup, metrics, dimensions)

        if rounding and not df.empty:
            df = df.round(rounding)

        if order_by and not (df.empty and df.index.empty):
            ob_fields = []
            ascending = []
            for row in order_by:
                field, ob_type = row
                ob_fields.append(field)
                ascending.append(True if ob_type == OrderByTypes.ASC else False)
            df = df.sort_values(by=ob_fields, ascending=ascending, key=self._sort)

        if not limit_first:
            df = self._apply_limits(df, row_filters, limit, metrics, dimensions)

        if pivot and not df.empty:
            df = df.unstack(pivot)

        dbg(df)
        dbg("Final result took %.3fs" % (time.time() - start))
        return df

    def clean_up(self):
        """Clean up the SQLite combined result table"""
        drop_sql = "DROP TABLE IF EXISTS %s " % self.table_name
        self.cursor.execute(drop_sql)
        self.conn.commit()
        self.conn.close()

    def _sort(self, series):
        """Apply custom sort logic to a pandas Series if possible"""
        field = self.warehouse.get_field(series.name)
        if not hasattr(field, "sorter"):
            return series
        if field.sorter:
            return field.sort(self.warehouse.id, series)
        return series

    def _select_all(self):
        """Helper to get all rows from the combined result table"""
        qr = self.cursor.execute("SELECT * FROM %s" % self.table_name)
        return [OrderedDict(row) for row in qr.fetchall()]

    def _get_final_select_sql(self, columns, dimension_aliases):
        """Create the final select SQL statement
        
        **Parameters:**
        
        * **columns** - (*list*) A list of column clauses
        * **dimension_aliases** - (*list*) A list of dimension column names
        
        **Returns:**
        
        (*str*) - A SQL statement
        
        """
        columns_clause = ", ".join(columns)
        order_clause = "1"
        if dimension_aliases:
            order_clause = ", ".join(["%s ASC" % d for d in dimension_aliases])
        sql = "SELECT %s FROM %s GROUP BY row_hash ORDER BY %s" % (
            columns_clause,
            self.table_name,
            order_clause,
        )
        info("\n" + sqlformat(sql))
        return sql

    def _get_bulk_insert_sql(self, rows):
        """Get a bulk SQL statement to insert the rows into the combined result
        table. This will will also create the hash primary key column for each
        row.
        
        **Parameters:**
        
        * **rows** - (*iterable*) An iterable of result rows
        
        **Returns:**
        
        (*str, list*) - A 2-item tuple containg the bulk SQL query the the
        values for parameter replacement
        
        """
        columns = list(rows[0].keys())
        placeholder = "(%s)" % (", ".join(["?"] * (1 + len(columns))))
        columns_clause = "row_hash, " + ", ".join(columns)

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
            update_clause = " ON CONFLICT(row_hash) DO UPDATE SET " + ", ".join(
                update_clauses
            )
            sql = sql + update_clause

        values = []
        for row in rows:
            row_values = [self._get_row_hash(row)]
            for value in row.values():
                # Note: sqlite cant handle Decimal values. Alternative approach:
                # https://stackoverflow.com/questions/6319409/how-to-convert-python-decimal-to-sqlite-numeric
                if isinstance(value, decimal.Decimal):
                    value = float(value)
                row_values.append(value)
            values.append(row_values)

        return escape_string(sql), values

    def _apply_row_filters(self, df, row_filters, metrics, dimensions):
        """Apply row level filters to the final result DataFrame. This uses
        pandas' `DataFrame.query` method.
        
        **Parameters:**
        
        * **df** - (*DataFrame*) The DataFrame to apply filters to
        * **row_filters** - (*list*) A list of row filter criteria. See the
        Report docs for more details.
        * **metrics** - (*OrderedDict*) A ordered mapping of metric names to
        objects
        * **dimensions** - (*OrderedDict*) A ordered mapping of dimension names
        to objects
        
        **Returns:**
        
        (*DataFrame*) - The filtered DataFrame
        
        """
        filter_parts = []
        fields = {}
        fields.update(metrics)
        fields.update(dimensions)

        for row_filter in row_filters:
            field, op, value = row_filter
            raiseifnot(
                field in fields, 'Row filter field "%s" is not in result table' % field
            )

            raiseifnot(
                op in ROW_FILTER_OPERATIONS, "Invalid row filter operation: %s" % op
            )
            if op == "=":
                op = "=="  # pandas expects this for comparison

            sa_type = type_string_to_sa_type(fields[field].type)
            py_type = sa_type.python_type

            if not isinstance(value, py_type):
                try:
                    value = py_type(value)
                except Exception as e:
                    raise ZillionException(
                        "Row filter for field '%s' has invalid value type %s and could not be converted to %s"
                        % (field, type(value), py_type)
                    )

            if isinstance(value, str):
                value = "'%s'" % value

            filter_parts.append("(%s %s %s)" % (field, op, value))

        result = df.query(" and ".join(filter_parts))
        # https://stackoverflow.com/questions/28772494/how-do-you-update-the-levels-of-a-pandas-multiindex-after-slicing-its-dataframe
        if dimensions and len(dimensions) > 1:
            result.index = result.index.remove_unused_levels()
        return result

    def _get_multi_rollup_df(self, df, rollup, dimensions, aggrs, wavgs):
        """Calculate and insert multi level rollup rows to a DataFrame. Note
        that this process will likely become a noticeable factor in performance
        as the size of the DataFrame and depth of rollups grow.
        
        **Parameters:**
        
        * **df** - (*DataFrame*) The DataFrame to add a multi-level rollup to
        * **rollup** - (*int or str*) Controls how metrics are rolled up /
        aggregated by dimension. See the Report docs for more details.
        * **dimensions** - (*OrderedDict*) An ordered mapping of dimension names
        to objects
        * **aggrs** - (*dict*) A mapping of aggregations to apply per DataFrame
        column. This will get passed to the pandas `agg` method of each group.
        * **wavgs** - (*list*) A list of metric name, weighting metric tuples to
        denote which columns require a weighted average
        
        **Returns:**
        
        (*DataFrame*) - The DataFrame with rollup rows added in. The rollup
        rows are marked in the DataFrame index with a special index label so
        they can easily be found/filtered later.
        
        """
        level_aggrs = [df]
        dim_names = list(dimensions.keys())

        for metric_name, weighting_metric in wavgs:
            # TODO: how does this behave if weights are missing?
            wavg = lambda x, wm=weighting_metric: np.average(
                x, weights=df.loc[x.index, wm]
            )
            aggrs[metric_name] = wavg

        for level in range(rollup):
            if (level + 1) == len(dimensions):
                # Unnecessary to rollup at the most granular level
                break

            grouped = df.groupby(level=list(range(0, level + 1)))
            level_aggr = grouped.agg(aggrs, skipna=True)

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

    def _apply_rollup(self, df, rollup, metrics, dimensions):
        """Apply a rollup to a result DataFrame. This is only allowed if
        dimensions are present in the report.
        
        **Parameters:**
        
        * **df** - (*DataFrame*) The dataframe to apply the rollup to
        * **rollup** - (*str or int*) Controls how metrics are rolled up /
        aggregated by dimension. See the Report docs for more details.
        * **metrics** - (*OrderedDict*) An OrderedDict mapping metric names to
        Metric objects
        * **dimensions** - (*OrderedDict*) An OrderedDict mapping dimension
        names to Dimension objects
        
        **Returns:**
        
        (*DataFrame*) - A DataFrame with the rollup rows added
        
        """
        raiseifnot(dimensions, "Can not rollup without dimensions")
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
            if metric.technical:
                # Skip rollups of technicals since they often dont make sense
                continue

            if metric.weighting_metric:
                wavgs.append((metric.name, metric.weighting_metric))
                continue

            aggr_func = PANDAS_ROLLUP_AGGR_TRANSLATION.get(
                metric.aggregation.lower(), metric.aggregation.lower()
            )
            aggrs[metric.name] = aggr_func

        totals = df.agg(aggrs, skipna=True)
        for metric_name, weighting_metric in wavgs:
            totals[metric_name] = wavg(metric_name, weighting_metric)

        apply_totals = True
        if rollup != RollupTypes.TOTALS:
            df = self._get_multi_rollup_df(df, rollup, dimensions, aggrs, wavgs)
            if rollup != len(dimensions):
                apply_totals = False

        if apply_totals:
            totals_rollup_index = (
                (ROLLUP_INDEX_LABEL,) * len(dimensions)
                if len(dimensions) > 1
                else ROLLUP_INDEX_LABEL
            )
            with pd.option_context("mode.chained_assignment", None):
                df.at[totals_rollup_index, :] = totals

        return df

    def _apply_technicals(self, df, technicals, rounding):
        """Apply technical computations on the DataFrame
        
        **Parameters:**
        
        * **df** - (*DataFrame*) The DataFrame to apply the technicals to
        * **technicals** - (*dict*) A mapping of metric names to Technical
        definitions
        * **rounding** - (*int*) The number of decimal places to round to
        
        **Returns:**
        
        (*DataFrame*) - A DataFrame that has the target metrics replaced with
        their technical computed values. Additional columns related to the
        technicals may be added in some cases as well, such as in those that
        show lower and upper bounds.
        
        """
        for metric, tech in technicals.items():
            tech.apply(df, metric, rounding=rounding)
        return df

    def _apply_limits(self, df, row_filters, limit, metrics, dimensions):
        """Apply row filters and limits to the DataFrame
        
        **Parameters:**
        
        * **df** - (*DataFrame*) The DataFrame to apply filters/limits to
        * **row_filters** - (*list*) A list of criteria to filter which rows get
        returned
        * **limit** - (*int*) A limit on the number of rows returned
        * **metrics** - (*OrderedDict*) An OrderedDict mapping metric names to
        Metric objects
        * **dimensions** - (*OrderedDict*) An OrderedDict mapping dimension
        names to Dimension objects
        
        **Returns:**
        
        (*DataFrame*) - A DataFrame that has the filters/limits applied
        
        """
        if row_filters and not df.empty:
            df = self._apply_row_filters(df, row_filters, metrics, dimensions)

        if limit and not (df.empty and df.index.empty):
            df = df.iloc[:limit]

        return df


class Report(ExecutionStateMixin):
    """Build a report against a warehouse. On init DataSource queries are built,
    but nothing is executed.
    
    **Parameters:**
    
    * **warehouse** - (*Warehouse*) A zillion warehouse object to run the report
    against
    * **metrics** - (*list, optional*) A list of metric names, or dicts in the
    case of AdHocMetrics. These will be the measures of your report, or the
    statistics you are interested in computing at the given dimension grain.
    * **dimensions** - (*list, optional*) A list of dimension names to control
    the grain of the report. You can think of dimensions similarly to the "group
    by" in a SQL query.
    * **criteria** - (*list, optional*) A list of criteria to be applied when
    querying. Each criteria in the list is represented by a 3-item list or
    tuple. See `core.CRITERIA_OPERATIONS` for all supported
    operations. Note that some operations, such as "like", have varying
    behavior by datasource dialect. Some examples:
        * ["field_a", ">", 1]
        * ["field_b", "=", "2020-04-01"]
        * ["field_c", "like", "%example%"]
        * ["field_d", "in", ["a", "b", "c"]]
    
    * **row_filters** - (*list, optional*) A list of criteria to apply at the
    final step (combined query layer) to filter which rows get returned. The
    format here is the same as for the criteria arg, though the operations are
    limited to the values of `core.ROW_FILTER_OPERATIONS`.
    * **rollup** - (*str or int, optional*) Controls how metrics are rolled up
    / aggregated by dimension depth. If not passed no rollup will be
    computed. If the special value "totals" is passed, only a final tally
    rollup row will be added. If an int, then it controls the maximum depth to
    roll up the data, starting from the most granular (last) dimension of the
    report. Note that the rollup=3 case is like adding a totals row to the
    "=2" case, as a totals row is a rollup of all dimension levels. Setting
    rollup=len(dims) is equivalent to rollup="all". For example, if you ran a
    report with dimensions ["a", "b", "c"]:
        * **rollup="totals"** - adds a single, final rollup row
        * **rollup="all"** - rolls up all dimension levels
        * **rollup=1** - rolls up the first dimension only
        * **rollup=2** - rolls up the first two dimensions
        * **rollup=3** - rolls up all three dimensions
        * Any other non-None value would raise an error
    
    * **pivot** - (*list, optional*) A list of dimensions to pivot to columns
    * **order_by** - (*list, optional*) A list of (field, asc/desc) tuples that
    control the ordering of the returned result
    * **limit** - (*int, optional*) A limit on the number of rows returned
    * **limit_first** - (*bool, optional*) Whether to apply limits before
    rollups/ordering
    * **adhoc_datasources** - (*list, optional*) A list of FieldManagers
    specific to this report
    
    **Notes:**
    
    The order_by and limit functionality is only applied on the final/combined
    result, NOT in your DataSource queries. In most cases when you are dealing
    with DataSource tables that are of a decent size you will want to make sure
    to include criteria that limit the scope of your query and/or take advantage
    of underlying table indexing. If you were to use order_by or limit without
    any criteria or dimensions, you would effectively select all rows from the
    underlying datasource table into memory (or at least try to).
    
    """

    def __init__(
        self,
        warehouse,
        metrics=None,
        dimensions=None,
        criteria=None,
        row_filters=None,
        rollup=None,
        pivot=None,
        order_by=None,
        limit=None,
        limit_first=False,
        adhoc_datasources=None,
    ):
        start = time.time()
        self.spec_id = None
        self.meta = None
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

        raiseifnot(
            self.metrics or self.dimensions,
            "One of metrics or dimensions must be specified for Report",
        )

        self.criteria = self._populate_criteria_fields(
            criteria or [], adhoc_datasources=adhoc_datasources
        )
        self.row_filters = row_filters or []

        self.rollup = None
        if rollup is not None:
            raiseifnot(dimensions, "Must specify dimensions in order to use rollup")
            if rollup not in RollupTypes:
                raiseifnot(
                    is_int(rollup) and (0 < int(rollup) <= len(dimensions)),
                    "Invalid rollup value: %s" % rollup,
                )
                self.rollup = int(rollup)
            else:
                if rollup == RollupTypes.ALL:
                    rollup = len(dimensions)
                self.rollup = rollup

        self.pivot = pivot or []
        if pivot:
            raiseifnot(
                set(self.pivot).issubset(set(self.dimensions)),
                "Pivot fields must be a subset of dimensions",
            )

        # TODO: apply order_by and limits at the datasource level
        # when possible.

        self.order_by = order_by or []
        if order_by:
            self._check_order_by(self.order_by)

        self.limit = limit or None
        if self.limit is not None:
            raiseifnot(
                isinstance(self.limit, int) and self.limit > 0,
                "Limit must be an integer > 0",
            )
        self.limit_first = limit_first

        self.adhoc_datasources = adhoc_datasources or []

        self.ds_metrics = OrderedDict()
        self.ds_dimensions = OrderedDict()

        for metric in self.metrics.values():
            self._add_ds_fields(metric)

        for dim in self.dimensions.values():
            self._add_ds_fields(dim)

        self._check_required_grain()
        self.queries = self._build_ds_queries()
        self.combined_query = None
        self.result = None

        super().__init__()
        self._set_state(ExecutionState.READY)
        dbg("Report init took %.3fs" % (time.time() - start))

    def get_params(self):
        """Get a dict of params used to create the Report"""
        used_datasources = list({q.get_datasource_name() for q in self.queries})
        datasources = [ds.get_params() for ds in self.warehouse.datasources]
        return dict(
            kwargs=dict(
                metrics=self._requested_metrics,
                dimensions=self._requested_dimensions,
                criteria=self._requested_criteria,
                row_filters=self.row_filters,
                rollup=self.rollup,
                pivot=self.pivot,
                order_by=self.order_by,
                limit=self.limit,
                limit_first=self.limit_first,
            ),
            datasources=datasources,
            used_datasources=used_datasources,
        )

    def get_json(self):
        """Get a JSON representation of the Report params"""
        return json.dumps(self.get_params())

    def save(self, meta=None):
        """Save the report spec and return the saved spec ID
        
        **Parameters:**
        
        * **meta** - (*object, optional*) A metadata object to be
        serialized as JSON and stored with the report
        
        **Returns:**
        
        (*int*) - The ID of the saved ReportSpec
        
        """
        raiseifnot(
            self.warehouse.id,
            "The Warehouse must be saved before ReportSpecs can be saved",
        )
        conn = zillion_engine.connect()
        try:
            result = conn.execute(
                ReportSpecs.insert(),
                warehouse_id=self.warehouse.id,
                params=self.get_json(),
                meta=json.dumps(meta),
            )
            spec_id = result.inserted_primary_key[0]
            raiseifnot(spec_id, "No report spec ID found")
        finally:
            conn.close()
        self.spec_id = spec_id
        self.meta = meta
        return spec_id

    def execute(self):
        """Execute the datasource queries, combine the results, and do the final
        result selection. Save the ReportResult on the result attribute"""
        start = time.time()

        self._set_state(ExecutionState.QUERYING, assert_ready=True)

        try:
            query_results = self._execute_ds_queries(self.queries)
            summaries = [x.summary for x in query_results]

            self._raise_if_killed()
            cr = self._create_combined_result(query_results)

            try:
                self._raise_if_killed()
                final_result = cr.get_final_result(
                    self.metrics,
                    self.dimensions,
                    self.row_filters,
                    self.rollup,
                    self.pivot,
                    self.order_by,
                    self.limit,
                    self.limit_first,
                )
                diff = time.time() - start
                self.result = ReportResult(
                    final_result, diff, summaries, self.metrics, self.dimensions
                )
                return self.result
            finally:
                cr.clean_up()
        finally:
            self._set_state(
                ExecutionState.READY, raise_if_killed=True, set_if_killed=True
            )

    def kill(self, soft=False, raise_if_failed=False):
        """Kill a running report
        
        **Parameters:**
        
        * **soft** - (*bool, optional*) If true, set the report state to killed
        without attempting to kill any running datasource queries.
        * **raise_if_failed** - (*bool, optional*) If true, raise
        FailedKillException if any exceptions occurred when trying to kill
        datasource queries. Otherwise a warning will be emitted.
        
        """
        info("killing report %s" % self.uuid)

        with self._get_lock():
            if self._ready:
                warn("kill called on report that isn't running")
                return

            if self._killed:
                warn("kill called on report already being killed")
                return

            if soft:
                self._set_state(ExecutionState.KILLED)
                return

            querying = self._querying  # grab state before changing it
            self._set_state(ExecutionState.KILLED)

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
                    warn("failed to kill some queries: %s" % exceptions)

    def get_grain(self):
        """Get the grain of this report, which accounts for dimension fields
        required in the requested dimensions, criteria, and formula-based
        fields."""
        if not (self.ds_dimensions or self.criteria):
            return None
        grain = set()
        if self.ds_dimensions:
            grain = grain | self.ds_dimensions.keys()
        if self.criteria:
            grain = grain | {x[0].name for x in self.criteria}
        return grain

    def get_dimension_grain(self):
        """Get the portion of the grain specific to request dimensions"""
        if not self.dimensions:
            return None
        return set(self.dimensions.keys())

    def _get_fields_dict(self, names, field_type, adhoc_datasources=None):
        """Get a dict mapping of field names to Field objects
        
        **Parameters:**
        
        * **names** - (*list*) A list of field names
        * **field_type** - (*str*) The FieldType
        * **adhoc_datasources** - (*list, optional*) A list of FieldManagers
        
        **Returns:**
        
        (*dict*) - A mapping of field names to Field objects
        
        """
        d = OrderedDict()
        for name in names or []:
            if field_type == FieldTypes.METRIC:
                field = self.warehouse.get_metric(name, adhoc_fms=adhoc_datasources)
            elif field_type == FieldTypes.DIMENSION:
                field = self.warehouse.get_dimension(name, adhoc_fms=adhoc_datasources)
            else:
                raise ZillionException("Invalid field type: %s" % field_type)
            d[field.name] = field
        return d

    def _populate_criteria_fields(self, criteria, adhoc_datasources=None):
        """Given the requested criteria, replace the field name references with
        the corresponding Field objects."""
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

    def _check_order_by(self, order_by):
        """Validate the format of the order_by specification"""
        raiseifnot(
            isinstance(order_by, (tuple, list)), "order_by must be a tuple or list"
        )
        fields = set()
        for row in order_by:
            raiseifnot(
                len(row) == 2,
                "order_by must be an iterable of (field, order type) pairs",
            )
            field, ob_type = row
            raiseifnot(ob_type in OrderByTypes, "Invalid order_by type: %s" % ob_type)
            fields.add(field)

        raiseifnot(
            fields.issubset(set(self.dimensions) | set(self.metrics)),
            "Order by fields must be a subset of Report fields",
        )

    def _add_ds_fields(self, field):
        """Add all datasource fields that are part of this field. This will add
        to either the ds_metrics or ds_dimensions attributes.
        
        **Parameters:**
        
        * **field** - (*Field*) A Field object to analyze
        
        """
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
                    raise ZillionException("Invalid field_type: %s" % field.field_type)
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
                if formula_field not in self.dimensions:
                    raise ReportException(
                        (
                            "Formula for field %s uses dimension %s that is not included in "
                            "requested report dimensions"
                        )
                        % (field.name, formula_field)
                    )
                self.ds_dimensions[formula_field] = self.warehouse.get_dimension(
                    formula_field, adhoc_fms=self.adhoc_datasources
                )
            else:
                raise ZillionException(
                    "Could not find field %s in warehouse" % formula_field
                )

    def _get_query_label(self, query_label):
        """Get a standardized label for the report query"""
        return "Report: %s | Query: %s" % (str(self.uuid), query_label)

    def _execute_ds_queries_sequential(self, queries):
        """Execute all DataSource queries in sequential order
        
        **Parameters:**
        
        * **queries** - (*list*) A list of DataSourceQuery objects
        
        **Returns:**
        
        (*list*) - A list of query execution results
        
        """
        results = []
        timeout = zillion_config["DATASOURCE_QUERY_TIMEOUT"]
        for i, query in enumerate(queries):
            self._raise_if_killed()
            label = self._get_query_label("%s / %s" % (i + 1, len(queries)))
            result = query.execute(timeout=timeout, label=label)
            results.append(result)
        return results

    def _execute_ds_queries_multithread(self, queries):
        """Execute all DataSource queries in a ThreadPoolExecutor
        
        **Parameters:**
        
        * **queries** - (*list*) A list of DataSourceQuery objects
        
        **Returns:**
        
        (*list*) - A list of query execution results
        
        """
        # TODO: If any query times out, the entire report fails. It might be
        # better if partial results could be returned.
        finished = {}
        timeout = zillion_config["DATASOURCE_QUERY_TIMEOUT"]
        workers = zillion_config.get("DATASOURCE_QUERY_WORKERS", len(queries))

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures_map = {}
            for i, query in enumerate(queries):
                label = self._get_query_label("%s / %s" % (i + 1, len(queries)))
                future = executor.submit(query.execute, timeout=timeout, label=label)
                futures_map[future] = query

            for future in as_completed(futures_map):
                data = future.result()
                query = futures_map[future]
                finished[future] = data

        return finished.values()

    def _execute_ds_queries(self, queries):
        """Execute a set of DataSource queries. The DATASOURCE_QUERY_MODE config
        var will control the execution mode.
        
        **Parameters:**
        
        * **queries** - (*list*) A list of DataSourceQuery objects
        
        **Returns:**
        
        (*list*) - A list of query execution results
        
        """
        mode = zillion_config["DATASOURCE_QUERY_MODE"]
        dbg("Executing %s datasource queries in %s mode" % (len(queries), mode))
        start = time.time()
        if mode == DataSourceQueryModes.SEQUENTIAL:
            return self._execute_ds_queries_sequential(queries)
        if mode == DataSourceQueryModes.MULTITHREAD:
            return self._execute_ds_queries_multithread(queries)
        dbg("DataSource queries took %.3fs" % (time.time() - start))
        raise ZillionException("Invalid DATASOURCE_QUERY_MODE: %s" % mode)

    def _check_required_grain(self):
        """Check that the dimension grain requirements are met for requested
        fields"""
        grain = self.get_dimension_grain() or set()
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

    def _build_ds_queries(self):
        """Build all datasource-level queries needed for this report"""
        grain = self.get_grain()
        dim_grain = self.get_dimension_grain()
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
                dbg("Metric %s is covered by existing query" % metric_name)
                continue

            try:
                table_set = self.warehouse.get_metric_table_set(
                    metric_name,
                    grain,
                    dimension_grain=dim_grain,
                    adhoc_datasources=self.adhoc_datasources,
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
                grain,
                dimension_grain=dim_grain,
                adhoc_datasources=self.adhoc_datasources,
            )
            query = DataSourceQuery(
                self.warehouse, None, self.ds_dimensions, self.criteria, table_set
            )
            queries.append(query)

        for query in queries:
            dbgsql(sqla_compile(query.select))

        return queries

    def _create_combined_result(self, ds_query_results):
        """Create a single combined result from the datasource query resultss
        
        **Parameters:**
        
        * **ds_query_results** - (*list*) A list of DataSourceQueryResult
        objects
        
        """
        start = time.time()
        result = SQLiteMemoryCombinedResult(
            self.warehouse,
            ds_query_results,
            list(self.ds_dimensions.keys()),
            adhoc_datasources=self.adhoc_datasources,
        )
        dbg("Combined result took %.3fs" % (time.time() - start))
        return result

    @classmethod
    def from_params(cls, warehouse, params, adhoc_datasources=None):
        """Build a report from a set of report params
        
        **Parameters:**
        
        * **warehouse** - (*Warehouse*) A zillion warehouse object
        * **params** - (*dict*) A dict of Report params
        * **adhoc_datasources** - (*list, optional*) A list of FieldManagers
        
        """
        used_dses = set(params.get("used_datasources", []))
        wh_dses = warehouse.datasource_names
        all_dses = set(wh_dses) | {x.name for x in (adhoc_datasources or [])}
        if not used_dses.issubset(all_dses):
            raise ReportException(
                "Report requires datasources that are not present: %s Found: %s"
                % (used_dses - all_dses, all_dses)
            )
        return Report(
            warehouse, **params["kwargs"], adhoc_datasources=adhoc_datasources
        )

    @classmethod
    def load(cls, warehouse, spec_id, adhoc_datasources=None):
        """Load a report from a spec ID
        
        **Parameters:**
        
        * **warehouse** - (*Warehouse*) A zillion warehouse object
        * **spec_id** - (*int*) A ReportSpec ID
        * **adhoc_datasources** - (*list, optional*) A list of FieldManagers
        
        """
        spec = cls._load_report_spec(warehouse, spec_id)
        if not spec:
            raise InvalidReportIdException(
                "Could not find report for spec id: %s" % spec_id
            )
        params = json.loads(spec["params"])
        meta = json.loads(spec["meta"]) if spec["meta"] else None
        result = cls.from_params(warehouse, params, adhoc_datasources=adhoc_datasources)
        result.meta = meta
        return result

    @classmethod
    def load_warehouse_id_for_report(cls, spec_id):
        """Get the Warehouse ID for a particular report spec
        
        **Parameters:**
        
        * **spec_id** - (*int*) A ReportSpec ID
        
        **Returns:**
        
        (*dict*) - A Warehouse ID
                
        """
        s = sa.select([ReportSpecs.c.warehouse_id]).where(ReportSpecs.c.id == spec_id)
        conn = zillion_engine.connect()
        try:
            result = conn.execute(s)
            row = result.fetchone()
            if not row:
                return None
            return row["warehouse_id"]
        finally:
            conn.close()

    @classmethod
    def delete(cls, warehouse, spec_id):
        """Delete a saved report spec
        
        **Parameters:**
        
        * **spec_id** - (*int*) The ID of a ReportSpec to delete
        
        """
        s = ReportSpecs.delete().where(
            sa.and_(
                ReportSpecs.c.warehouse_id == warehouse.id, ReportSpecs.c.id == spec_id
            )
        )
        conn = zillion_engine.connect()
        try:
            conn.execute(s)
        finally:
            conn.close()

    @classmethod
    def _load_report_spec(cls, warehouse, spec_id):
        """Get a ReportSpec row from a ReportSpec ID. The report spec must
        exist within the context of the given warehouse.
        
        **Parameters:**
        
        * **warehouse** - (*Warehouse*) A zillion warehouse object
        * **spec_id** - (*int*) The ID of the ReportSpec to load
        
        **Returns:**
        
        (*dict*) - A ReportSpec row
        
        """
        raiseifnot(
            warehouse.id, "trying to load ReportSpec for unspecified Warehouse ID"
        )
        s = sa.select(ReportSpecs.c).where(
            sa.and_(
                ReportSpecs.c.warehouse_id == warehouse.id, ReportSpecs.c.id == spec_id
            )
        )
        conn = zillion_engine.connect()
        try:
            result = conn.execute(s)
            row = result.fetchone()
            return row
        finally:
            conn.close()

    @classmethod
    def _load_params(cls, warehouse, spec_id):
        """Get Report params from a ReportSpec ID
        
        **Parameters:**
        
        * **warehouse** - (*Warehouse*) A zillion warehouse object
        * **spec_id** - (*int*) The ID of the ReportSpec to load params for
        
        **Returns:**
        
        (*dict*) - A dict of Report params
        
        """
        spec = cls._load_report_spec(warehouse, spec_id)
        if not spec:
            raise InvalidReportIdException(
                "Could not find report for spec id: %s" % spec_id
            )
        return json.loads(spec["params"])


class ReportResult(PrintMixin):
    """Encapsulates a report result as well as some additional helpers and
    summary statistics.
    
    **Parameters:**
    
    * **df** - (*DataFrame*) The DataFrame containing the final report result
    * **duration** - (*float*) The report execution duration in seconds
    * **query_summaries** - (*list of DataSourceQuerySummary*) Summaries of the
    underyling query results.
    * **metrics** - (*OrderedDict*) A mapping of requested metrics to Metric objects
    * **dimensions** - (*OrderedDict*) A mapping of requested dimensions to Dimension
    objects
    
    """

    repr_attrs = ["rowcount", "duration", "query_summaries"]

    @initializer
    def __init__(self, df, duration, query_summaries, metrics, dimensions):
        raiseif(metrics and (not isinstance(metrics, OrderedDict)))
        raiseif(dimensions and (not isinstance(dimensions, OrderedDict)))
        self.duration = round(duration, 4)
        self.rowcount = len(df)

    @property
    def rollup_mask(self):
        """Get a mask of rows that contain the rollup marker"""
        mask = None
        index = self.df.index
        for i in range(index.nlevels):
            if mask is None:
                mask = index.isin([ROLLUP_INDEX_LABEL], i)
            else:
                mask = mask | index.isin([ROLLUP_INDEX_LABEL], i)
        return mask

    @property
    def rollup_rows(self):
        """Get the rows of the dataframe that are rollups"""
        return self.df.loc[self.rollup_mask]

    @property
    def non_rollup_rows(self):
        """Get the rows of the dataframe that are not rollups"""
        return self.df.loc[~self.rollup_mask]

    @property
    def display_name_map(self):
        """Get the map from default to display names"""
        name_map = {v.name: v.display_name for v in self.dimensions.values()}
        for column in self.df.columns:
            if column in self.metrics:
                name_map[column] = self.metrics[column].display_name
            else:
                # Some technicals add additional columns, this ensures they
                # use a reasonable display format instead of getting ignored.
                name_map[column] = default_field_display_name(column)
        return name_map

    @property
    def df_display(self):
        """Get the rows of the dataframe with data in display format. This
        includes replacing rollup markers with display values"""
        df = self.df.rename(index={ROLLUP_INDEX_LABEL: ROLLUP_INDEX_DISPLAY_LABEL})
        if self.dimensions:
            df.index.names = [v.display_name for v in self.dimensions.values()]
        df.rename(columns=self.display_name_map, inplace=True)
        return df
