from collections import defaultdict, OrderedDict
import logging
import time

import sqlalchemy as sa

from zillion.configs import (
    WarehouseConfigSchema,
    load_warehouse_config,
    is_active,
    zillion_config,
)
from zillion.core import *
from zillion.datasource import DataSource
from zillion.field import get_table_dimensions, get_table_fields, FieldManagerMixin
from zillion.model import zillion_engine, Warehouses
from zillion.report import Report
from zillion.sql_utils import is_numeric_type, column_fullname

if zillion_config["DEBUG"]:
    default_logger.setLevel(logging.DEBUG)


class Warehouse(FieldManagerMixin):
    """A reporting warehouse that contains various datasources to run queries
    against and combine data in report results. The warehouse may contain global
    definitions for metrics and dimensions, and will also perform integrity
    checks of any added datasources.
    
    Note that the id, name, and meta attributes will only be populated when
    the Warehouse is persisted or loaded from a database. 
    
    **Parameters:**
    
    * **config** - (*dict, str, or buffer, optional*) A dict adhering to the
    WarehouseConfigSchema or a file location to load the config from
    * **datasources** - (*list, optional*) A list of DataSources that will make
    up the warehouse
    * **ds_priority** - (*list, optional*) An ordered list of datasource names
    establishing querying priority. This comes into play when part of a report
    may be satisfied by multiple datasources. Datasources earlier in this list
    will be higher priority.
    
    """

    def __init__(self, config=None, datasources=None, ds_priority=None):
        self.id = None
        self.name = None
        self.meta = None
        self._datasources = OrderedDict()
        self._metrics = {}
        self._dimensions = {}
        self._supported_dimension_cache = {}

        for ds in datasources or []:
            self.add_datasource(ds, skip_integrity_checks=True)

        if config:
            config = load_warehouse_config(config)
            self.apply_config(config, skip_integrity_checks=True)

        raiseifnot(self._datasources, "No datasources provided or found in config")

        self._add_default_display_names()
        self.run_integrity_checks()

        self.ds_priority = ds_priority or list(self._datasources.keys())

        raiseifnot(
            isinstance(self.ds_priority, list),
            (
                "Invalid format for ds_priority, must be list of datasource names: %s"
                % self.ds_priority
            ),
        )
        raiseifnot(
            len(self.ds_priority) == len(self._datasources),
            "Length mismatch between ds_priority and datasources",
        )
        for ds_name in self.ds_priority:
            raiseifnot(
                ds_name in self._datasources,
                "Datasource %s is in ds_priority but not in datasource map" % ds_name,
            )

    def __repr__(self):
        return "<%s(name=%s)> Datasources: %s" % (
            self.__class__.__name__,
            self.name,
            self.datasource_names,
        )

    @property
    def datasources(self):
        """The datasource objects in this warehouse"""
        return self._datasources.values()

    @property
    def datasource_names(self):
        """The names of datasources in this warehouse"""
        return self._datasources.keys()

    def print_info(self):
        """Print the warehouse structure"""
        print("---- Warehouse")
        print("metrics:")
        self.print_metrics(indent=2)
        print("dimensions:")
        self.print_dimensions(indent=2)

        for ds in self.datasources:
            print()
            ds.print_info()

    def get_datasource(self, name, adhoc_datasources=None):
        """Get the datasource object corresponding to this datasource name
        
        **Parameters:**
        
        * **name** - (*str*) The name of the datasource
        * **adhoc_datasources** - (*list, optional*) A list of FieldManagers
        specific to this request
        
        **Returns:**
        
        (*DataSource*) - The matching datasource object
        
        """
        if name in self._datasources:
            return self._datasources[name]

        for adhoc_datasource in adhoc_datasources or []:
            if adhoc_datasource.name == name:
                return adhoc_datasource

        raise ZillionException('Could not find datasource with name "%s"' % name)

    def get_child_field_managers(self):
        """Get a list of all datasources in this warehouse"""
        return list(self.datasources)

    def add_datasource(self, ds, skip_integrity_checks=False):
        """Add a datasource to this warehouse
        
        **Parameters:**
        
        * **ds** - (*DataSource*) The datasource object to add
        * **skip_integrity_checks** - (*bool, optional*) If True, skip warehouse
        integrity checks
        
        """
        dbg("Adding datasource %s" % ds.name)
        self._datasources[ds.name] = ds
        self._clear_supported_dimension_cache()
        if not skip_integrity_checks:
            self.run_integrity_checks()

    def remove_datasource(self, ds, skip_integrity_checks=False):
        """Remove a datasource from this config
        
        **Parameters:**
        
        * **ds** - (*DataSource*) The datasource object to remove
        * **skip_integrity_checks** - (*bool, optional*) If True, skip warehouse
        integrity checks
        
        """
        dbg("Removing datasource %s" % ds.name)
        del self._datasources[ds.name]
        self._clear_supported_dimension_cache()
        if not skip_integrity_checks:
            self.run_integrity_checks()

    def apply_config(self, config, skip_integrity_checks=False):
        """Apply a warehouse config
        
        **Parameters:**
        
        * **config** - (*dict*) A dict adhering to the WarehouseConfigSchema
        * **skip_integrity_checks** - (*bool, optional*) If True, skip warehouse
        integrity checks
        
        """
        self._create_or_update_datasources(
            config.get("datasources", {}), skip_integrity_checks=skip_integrity_checks
        )
        # TODO: this goes second in case any formula fields reference fields
        # defined or created in the datasources. It may make more sense to
        # only defer population of formula fields.
        self._populate_global_fields(config, force=True)
        self._clear_supported_dimension_cache()

    def run_integrity_checks(self, adhoc_datasources=None):
        """Run a series of integrity checks on the warehouse and its
        datasources. This will raise a WarehouseException with all failed
        checks.
        
        **Parameters:**
        
        * **adhoc_datasources** - (*list, optional*) A list of FieldManagers to
        include for this request
        
        """
        errors = []
        if adhoc_datasources:
            for ds in adhoc_datasources:
                if ds.name in self.datasource_names:
                    errors.append(
                        "Adhoc DataSource '%s' name conflicts with existing DataSource"
                        % ds.name
                    )

        errors.extend(
            self._check_reserved_field_names(adhoc_datasources=adhoc_datasources)
        )
        errors.extend(
            self._check_conflicting_fields(adhoc_datasources=adhoc_datasources)
        )
        errors.extend(self._check_fields_have_type(adhoc_datasources=adhoc_datasources))
        errors.extend(
            self._check_primary_key_dimensions(adhoc_datasources=adhoc_datasources)
        )
        errors.extend(
            self._check_weighting_metrics(adhoc_datasources=adhoc_datasources)
        )
        errors.extend(self._check_required_grain(adhoc_datasources=adhoc_datasources))
        errors.extend(
            self._check_incomplete_dimensions(adhoc_datasources=adhoc_datasources)
        )

        if errors:
            raise WarehouseException("Integrity check(s) failed.\n%s" % pf(errors))

    def load_report(self, spec_id, adhoc_datasources=None):
        """Load a report from a spec ID
        
        **Parameters:**
        
        * **spec_id** - (*int*) The ID of a report spec
        * **adhoc_datasources** - (*list, optional*) A list of FieldManagers
        specific to this request
        
        **Returns:**
        
        (*Report*) - A report built from this report spec
        
        """
        raiseifnot(
            self.id,
            "The Warehouse must be saved before ReportSpecs can be loaded for the Warehouse",
        )
        return Report.load(self, spec_id, adhoc_datasources=adhoc_datasources)

    def delete_report(self, spec_id):
        """Delete a report by spec ID
        
        **Parameters:**
        
        * **spec_id** - (*int*) The ID of a report spec to delete
        
        """
        raiseifnot(
            self.id,
            "The Warehouse must be saved before ReportSpecs can be deleted for the Warehouse",
        )
        Report.delete(self, spec_id)

    def save_report(self, meta=None, **kwargs):
        """Init a Report and save it as a ReportSpec. Note that the Warehouse
        must be saved before any ReportSpecs can be saved for the Warehouse.
        
        **Parameters:**
        
        * **meta** - (*object, optional*) A metadata object to be
        serialized as JSON and stored with the report
        * ****kwargs** - Passed through to Report
        
        **Returns:**
        
        (*Report*) - The built report with the spec ID populated
        
        """
        raiseifnot(
            self.id,
            "The Warehouse must be saved before ReportSpecs can be saved for the Warehouse",
        )
        report = Report(self, **kwargs)
        report.save(meta=meta)
        return report

    def save(self, name, config_url, meta=None):
        """Save the warehouse config and return the ID
        
        **Parameters:**

        * **name** - (*str*) A name to give the Warehouse
        * **config_url** - (*str*) A URL pointing to a config file that can
        be used to recreate the warehouse
        * **meta** - (*object, optional*) A metadata object to be
        serialized as JSON and stored with the warehouse
        
        **Returns:**
        
        (*int*) - The ID of the saved Warehouse
        
        """
        raiseifnot(name, "A unique name must be specified to save a Warehouse")
        raiseifnot(
            # TODO: better check for valid URL
            config_url and isinstance(config_url, str),
            "A config URL must be specified to save a Warehouse",
        )

        params = dict(ds_priority=self.ds_priority, config=config_url)

        conn = zillion_engine.connect()
        try:
            result = conn.execute(
                Warehouses.insert(),
                name=name,
                params=json.dumps(params),
                meta=json.dumps(meta),
            )
            wh_id = result.inserted_primary_key[0]
            raiseifnot(wh_id, "No warehouse ID found")
        finally:
            conn.close()
        self.id = wh_id
        self.meta = meta
        self.name = name
        return wh_id

    def execute(
        self,
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
        """Build and execute a Report
        
        **Returns:**
        
        (*ReportResult*) - The result of the report
        
        """
        start = time.time()

        report = Report(
            self,
            metrics=metrics,
            dimensions=dimensions,
            criteria=criteria,
            row_filters=row_filters,
            rollup=rollup,
            pivot=pivot,
            order_by=order_by,
            limit=limit,
            limit_first=limit_first,
            adhoc_datasources=adhoc_datasources,
        )
        result = report.execute()

        dbg("warehouse report took %.3fs" % (time.time() - start))
        return result

    def execute_id(self, spec_id, adhoc_datasources=None):
        """Build and execute a report from a spec ID
        
        **Parameters:**
        
        * **spec_id** - (*int*) The ID of a report spec
        * **adhoc_datasources** - (*list, optional*) A list of FieldManagers
        specific to this request
        
        **Returns:**
        
        (*ReportResult*) - The result of the report
        
        """
        start = time.time()
        report = self.load_report(spec_id, adhoc_datasources=adhoc_datasources)
        result = report.execute()
        dbg("warehouse report took %.3fs" % (time.time() - start))
        return result

    def get_metric_table_set(
        self, metric, grain, dimension_grain, adhoc_datasources=None
    ):
        """Get a TableSet that can satisfy a metric at a given grain
        
        **Parameters:**
        
        * **metric** - (*str*) A metric name
        * **grain** - (*list*) A list of dimension names representing the full
        grain required including dimension and criteria grain
        * **dimension_grain** - (*list of str*) A list of dimension names
        representing the requested dimensions for report grouping
        * **adhoc_datasources** - (*list, optional*) A list of FieldManagers for
        this request
        
        **Returns:**
        
        (*TableSet*) - A TableSet that can satisfy this request
        
        """
        dbg("metric:%s grain:%s" % (metric, grain))
        ds_metric_tables = self._get_ds_tables_with_metric(
            metric, adhoc_datasources=adhoc_datasources
        )
        ds_table_sets = self._get_ds_table_sets(
            ds_metric_tables,
            metric,
            grain,
            dimension_grain,
            adhoc_datasources=adhoc_datasources,
        )
        if not ds_table_sets:
            msg = self._generate_unsupported_grain_msg(
                grain, metric, adhoc_datasources=adhoc_datasources
            )
            raise UnsupportedGrainException(msg)
        table_set = self._choose_best_table_set(ds_table_sets)
        return table_set

    def get_dimension_table_set(self, grain, dimension_grain, adhoc_datasources=None):
        """Get a TableSet that can satisfy dimension table joins across this
        grain
        
        **Parameters:**
        
        * **grain** - (*list*) A list of dimension names representing the full
        grain required including dimension and criteria grain
        * **dimension_grain** - (*list of str*) A list of dimension names
        representing the requested dimensions for report grouping
        * **adhoc_datasources** - (*list, optional*) A list of FieldManagers for
        this request
        
        **Returns:**
        
        (*TableSet*) - A TableSet that can satisfy this request
        
        """
        dbg("grain:%s" % grain)

        table_set = None
        for dim_name in grain:
            ds_dim_tables = self._get_ds_dim_tables_with_dim(
                dim_name, adhoc_datasources=adhoc_datasources
            )
            ds_table_sets = self._get_ds_table_sets(
                ds_dim_tables,
                dim_name,
                grain,
                dimension_grain,
                adhoc_datasources=adhoc_datasources,
            )
            if not ds_table_sets:
                continue
            table_set = self._choose_best_table_set(ds_table_sets)
            break

        if not table_set:
            raise UnsupportedGrainException(
                "No dimension table set found to meet grain: %s" % grain
            )
        return table_set

    def _create_or_update_datasources(self, ds_configs, skip_integrity_checks=False):
        """Given a set of datasource configs, create or update the datasources
        on this warehouse. If a datasource exists already it will be updated by
        applying the datasource config. Otherwise this attempts to create a
        datasource from the config.
        
        **Parameters:**
        
        * **ds_configs** - (*dict*) A dict mapping datasource names to
        datasource configs
        * **skip_integrity_checks** - (*bool, optional*) If True, skip warehouse
        integrity checks
        
        """
        for ds_name in ds_configs:
            if ds_name in self.datasource_names:
                self.get_datasource(ds_name).apply_config(ds_configs[ds_name])
                continue

            ds = DataSource(ds_name, config=ds_configs[ds_name])
            self.add_datasource(ds, skip_integrity_checks=skip_integrity_checks)

    def _clear_supported_dimension_cache(self):
        """Clear the cache of supported dimensions"""
        self._supported_dimension_cache = {}

    def _check_reserved_field_names(self, adhoc_datasources=None):
        """Integrity check against reserved field names"""
        errors = []
        for field in self.get_field_names(adhoc_fms=adhoc_datasources):
            if field in RESERVED_FIELD_NAMES:
                errors.append("Field name %s is reserved" % field)
        return errors

    def _check_conflicting_fields(self, adhoc_datasources=None):
        """Integrity check for conflicting field definitions"""
        errors = []

        for field in self.get_field_names(adhoc_fms=adhoc_datasources):
            if self.has_metric(
                field, adhoc_fms=adhoc_datasources
            ) and self.has_dimension(field, adhoc_fms=adhoc_datasources):
                errors.append("Field %s is in both metrics and dimensions" % field)

            instances = self.get_field_instances(field, adhoc_fms=adhoc_datasources)

            field_type_mismatch = False
            data_type_mismatch = False
            aggregation_mismatch = False
            field_type = None
            field_aggr = None
            field_is_numeric = None

            for fm, field_def in instances.items():
                if not field_type:
                    field_type = field_def.field_type
                    field_is_numeric = is_numeric_type(field_def.sa_type)
                    field_aggr = getattr(field_def, "aggregation", None)
                    continue

                if field_def.field_type != field_type:
                    field_type_mismatch = True

                if is_numeric_type(field_def.sa_type) != field_is_numeric:
                    data_type_mismatch = True

                if field_def.field_type == FieldTypes.METRIC:
                    if field_def.aggregation != field_aggr:
                        aggregation_mismatch = True

            if field_type_mismatch:
                errors.append("Field %s is in both metrics and dimensions" % field)
            if data_type_mismatch:
                errors.append("Field %s has data type mismatches" % field)
            if aggregation_mismatch:
                errors.append("Field %s has aggregation mismatches" % field)

        return errors

    def _check_fields_have_type(self, adhoc_datasources=None):
        """Integrity check for field types"""
        errors = []

        for ds in self.get_field_managers(adhoc_fms=adhoc_datasources):
            for table in ds.metadata.tables.values():
                if not is_active(table):
                    continue

                for column in table.c:
                    if not is_active(column):
                        continue

                    for field in column.zillion.get_field_names():
                        if not (
                            self.has_metric(field, adhoc_fms=adhoc_datasources)
                            or self.has_dimension(field, adhoc_fms=adhoc_datasources)
                        ):
                            errors.append(
                                "Field %s for column %s->%s is not defined as a metric or dimension"
                                % (field, ds.name, column_fullname(column))
                            )

        return errors

    def _check_primary_key_dimensions(self, adhoc_datasources=None):
        """Integrity check for primary keys"""
        errors = []

        for ds in self.get_field_managers(adhoc_fms=adhoc_datasources):
            for table in ds.metadata.tables.values():
                if not is_active(table):
                    continue

                primary_key = table.zillion.primary_key
                table_dims = get_table_dimensions(
                    self, table, adhoc_fms=adhoc_datasources
                )

                for pk_field in primary_key:
                    if not self.has_dimension(pk_field):
                        errors.append(
                            "Primary key field is not a dimension: %s" % pk_field
                        )
                    if pk_field not in table_dims:
                        errors.append(
                            "Primary key dimension %s is not in table %s"
                            % (pk_field, table.fullname)
                        )

        return errors

    def _check_weighting_metrics(self, adhoc_datasources=None):
        """Integrity check for weighting metrics"""
        errors = []

        for metric in self.get_metrics(adhoc_fms=adhoc_datasources).values():
            if not metric.weighting_metric:
                continue

            for ds in self.get_field_managers(adhoc_fms=adhoc_datasources):
                tables = ds.get_tables_with_field(metric.name)
                if not tables:
                    continue

                for table in tables:
                    if not metric.weighting_metric in get_table_fields(table):
                        errors.append(
                            "Table %s->%s has metric %s but not weighting metric %s"
                            % (
                                ds.name,
                                table.fullname,
                                metric.name,
                                metric.weighting_metric,
                            )
                        )

        return errors

    def _check_required_grain(self, adhoc_datasources=None):
        """Integrity check for required_grain settings"""
        errors = []

        for metric in self.get_metrics(adhoc_fms=adhoc_datasources).values():
            if not metric.required_grain:
                continue

            for field in metric.required_grain:
                if not self.has_dimension(field, adhoc_fms=adhoc_datasources):
                    errors.append(
                        "Metric %s references unknown dimension %s in required_grain"
                        % (metric.name, field)
                    )

        for ds in self.get_field_managers(adhoc_fms=adhoc_datasources):
            for table in ds.metadata.tables.values():
                if not is_active(table):
                    continue

                for column in table.c:
                    if not is_active(column):
                        continue
                    if not column.zillion.required_grain:
                        continue

                    for field in column.zillion.required_grain:
                        if not self.has_dimension(field, adhoc_fms=adhoc_datasources):
                            errors.append(
                                "Column %s->%s references unknown dimension %s in required_grain"
                                % (ds.name, column_fullname(column), field)
                            )

        return errors

    def _check_incomplete_dimensions(self, adhoc_datasources=None):
        """Integrity check for incomplete_dimensions settings"""
        errors = []

        for ds in self.get_field_managers(adhoc_fms=adhoc_datasources):
            for table in ds.metadata.tables.values():
                if not is_active(table):
                    continue

                if not table.zillion.incomplete_dimensions:
                    continue

                for field in table.zillion.incomplete_dimensions:
                    if not self.has_dimension(field, adhoc_fms=adhoc_datasources):
                        errors.append(
                            "Table %s->%s references unknown dimension %s in incomplete_dimensions"
                            % (ds.name, table.fullname, field)
                        )

        return errors

    def _get_supported_dimensions_for_metric(
        self, metric, use_cache=True, adhoc_datasources=None
    ):
        """Get a set of all supported dimensions for a metric
        
        **Parameters:**
        
        * **metric** - (*str or Metric*) A metric name or Metric object
        * **use_cache** - (*bool, optional*) If True, try to pull the result
        from the supported dimension cache
        * **adhoc_datasources** - (*list, optional*) A list of FieldManagers
        specific to this request
        
        **Returns:**
        
        (*set*) - A set of dims supported by this metric
        
        """
        dims = set()
        metric = self.get_metric(metric, adhoc_fms=adhoc_datasources)

        if use_cache and metric.name in self._supported_dimension_cache:
            return self._supported_dimension_cache[metric]

        for ds in self.datasources:
            ds_tables = ds.get_tables_with_field(metric.name)
            used_tables = set()

            for ds_table in ds_tables:
                if ds_table.fullname not in used_tables:
                    dims |= get_table_dimensions(
                        self, ds_table, adhoc_fms=adhoc_datasources
                    )
                    used_tables.add(ds_table.fullname)

                desc_tables = ds.find_descendent_tables(ds_table)
                for desc_table in desc_tables:
                    if desc_table not in used_tables:
                        dims |= get_table_dimensions(
                            self, ds.get_table(desc_table), adhoc_fms=adhoc_datasources
                        )
                        used_tables.add(desc_table)

        self._supported_dimension_cache[metric] = dims
        return dims

    def _get_supported_dimensions(self, metrics, adhoc_datasources=None):
        """Get all of the supported dimensions shared among these metrics
        
        **Parameters:**
        
        * **metrics** - (*list*) A list of metric names or Metric objects
        * **adhoc_datasources** - (*list, optional*) A list of FieldManagers
        specific to this request
        
        **Returns:**
        
        (*set*) - A set of dims supported by all of these metrics
        
        """
        dims = set()
        for metric in metrics:
            supported_dims = self._get_supported_dimensions_for_metric(
                metric, adhoc_datasources=adhoc_datasources
            )
            dims = (dims & supported_dims) if len(dims) > 0 else supported_dims
        return dims

    def _get_ds_tables_with_metric(self, metric, adhoc_datasources=None):
        """Get a list of tables in each datasource that provide this metric
        
        **Parameters:**
        
        * **metric** - (*str*) The name of a metric
        * **adhoc_datasources** - (*list, optional*) A list of FieldManagers
        specific to this request
        
        **Returns:**
        
        (*dict*) - A dict mapping datasource names to a list of tables
        supporting this metric
        
        """
        ds_tables = defaultdict(list)
        count = 0
        for ds in self.get_field_managers(adhoc_fms=adhoc_datasources):
            tables = ds.get_tables_with_field(metric)
            if not tables:
                continue
            ds_tables[ds.name] = tables
            count += 1
        dbg(
            "found %d datasources, %d columns for metric %s"
            % (len(ds_tables), count, metric)
        )
        return ds_tables

    def _get_ds_dim_tables_with_dim(self, dim, adhoc_datasources=None):
        """Get a list of tables in each datasource that provide this dimension
        
        **Parameters:**
        
        * **dim** - (*str*) The name of a dimension
        * **adhoc_datasources** - (*list, optional*) A list of FieldManagers
        specific to this request
        
        **Returns:**
        
        (*dict*) - A dict mapping datasource names to a list of tables
        supporting this dimension
        
        """
        ds_tables = defaultdict(list)
        count = 0
        for ds in self.get_field_managers(adhoc_fms=adhoc_datasources):
            tables = ds.get_dim_tables_with_dim(dim)
            if not tables:
                continue
            ds_tables[ds.name] = tables
            count += 1
        dbg(
            "found %d datasources, %d columns for dim %s" % (len(ds_tables), count, dim)
        )
        return ds_tables

    def _get_ds_table_sets(
        self, ds_tables, field, grain, dimension_grain, adhoc_datasources=None
    ):
        """Get a list of TableSets that can satisfy the grain in each datasource
        
        **Parameters:**
        
        * **ds_tables** - (*dict*) A mapping of datasource names to tables
        containing the field
        * **field** - (*str*) A field name that is contained in the datasource
        tables
        * **grain** - (*list*) A list of dimension names representing the full
        grain required including dimension and criteria grain
        * **dimension_grain** - (*list of str*) A list of dimension names
        representing the requested dimensions for report grouping
        * **adhoc_datasources** - (*list, optional*) A list of FieldManagers for
        this request
        
        **Returns:**
        
        (*dict*) - A dict mapping datasource names to possible TableSets that
        satisfy the field/grain requirements
        
        """
        ds_table_sets = {}
        for ds_name, ds_tables_with_field in ds_tables.items():
            ds = self.get_datasource(ds_name, adhoc_datasources=adhoc_datasources)
            possible_table_sets = ds.find_possible_table_sets(
                ds_tables_with_field, field, grain, dimension_grain
            )
            if not possible_table_sets:
                continue
            ds_table_sets[ds_name] = possible_table_sets
        dbg(ds_table_sets)
        return ds_table_sets

    def _choose_best_datasource(self, ds_names):
        """Choose the best datasource to use. Currently this will used the
        ds_priority attribute to make the decision. If that is not defined it
        will just take the first datasource in the list. There is room for
        improvement by taking into account expected query performance.
        
        **Parameters:**
        
        * **ds_names** - (*list*) A list of datasource names
        
        **Returns:**
        
        (*str*) - The name of the best datasource to use
        
        """
        if self.ds_priority:
            for ds_name in self.ds_priority:
                if ds_name in ds_names:
                    return ds_name

        info("No datasource priorities established, choosing first option")
        raiseifnot(ds_names, "No datasource names provided")
        return ds_names[0]

    def _choose_best_table_set(self, ds_table_sets):
        """Choose the best TableSet to use among possible options. This will
        first choose the best datasource among the available options, and then
        the best TableSet within that. Currently the best TableSet is chosen
        simply as the one with the fewest number of tables in its join.
        
        **Parameters:**
        
        * **ds_table_sets** - (*dict*) A dict mapping datasource names to lists
        of possible TableSets
        
        **Returns:**
        
        (*TableSet*) - The best available TableSet
        
        """
        ds_name = self._choose_best_datasource(list(ds_table_sets.keys()))
        if len(ds_table_sets[ds_name]) > 1:
            # TODO: table set priorities based on expected query performance
            info(
                "Picking smallest of %d available table sets"
                % len(ds_table_sets[ds_name])
            )
        return sorted(ds_table_sets[ds_name], key=len)[0]

    def _generate_unsupported_grain_msg(self, grain, metric, adhoc_datasources=None):
        """Generate a messaged that aims to help pinpoint why a metric can not
        meet a specific grain
        
        **Parameters:**
        
        * **grain** - (*list*) A list of dimensions
        * **metric** - (*str*) A metric name
        * **adhoc_datasources** - (*list, optional*) A list of FieldManagers for
        this report
        
        **Returns:**
        
        (*str*) - A message explaining the unsupported grain issue
        
        """
        grain = grain or set()
        supported = self._get_supported_dimensions_for_metric(
            metric, adhoc_datasources=adhoc_datasources
        )
        unsupported = grain - supported
        if unsupported:
            msg = (
                "metric %s can not meet grain %s due to unsupported dimensions: %s"
                % (metric, grain, unsupported)
            )
        else:
            msg = "metric %s can not meet grain %s in any single datasource" % (
                metric,
                grain,
            )
        return msg

    @classmethod
    def load(cls, id):
        """Load a Warehouse from a Warehouse ID
        
        **Parameters:**
        
        * **id** - (*int*) A Warehouse ID
        
        **Returns:**
        
        (*Warehouse*) - A Warehouse object
        
        """
        wh = cls._load_warehouse(id)
        if not wh:
            raise InvalidWarehouseIdException(
                "Could not find Warehouse for id: %s" % id
            )

        params = json.loads(wh["params"])
        meta = json.loads(wh["meta"]) if wh["meta"] else None
        result = Warehouse(**params)
        result.meta = meta
        result.name = wh.name
        result.id = id
        return result

    @classmethod
    def load_warehouse_for_report(cls, spec_id):
        """Load the warehouse corresponding to the ReportSpec
        
        **Parameters:**
        
        * **spec_id** - (*int*) A ReportSpec ID
        
        **Returns:**
        
        (*Warehouse*) - A Warehouse object
        
        """
        wh_id = Report.load_warehouse_id_for_report(spec_id)
        raiseifnot(wh_id, "No warehouse ID found for spec ID %s" % spec_id)
        return cls.load(wh_id)

    @classmethod
    def load_report_and_warehouse(cls, spec_id):
        """Load a Report and Warehouse from a ReportSpec. The Warehouse
        will be populated on the returned Report object.
        
        **Parameters:**
        
        * **spec_id** - (*int*) A ReportSpec ID
        
        **Returns:**
        
        (*Report*) - A Report built from this report spec
        
        """
        wh = cls.load_warehouse_for_report(spec_id)
        return wh.load_report(spec_id)

    @classmethod
    def delete(cls, id):
        """Delete a saved warehouse. Note that this does not delete
        any report specs that reference this warehouse ID.
        
        **Parameters:**
        
        * **id** - (*int*) The ID of a Warehouse to delete
        
        """
        s = Warehouses.delete().where(Warehouses.c.id == id)
        conn = zillion_engine.connect()
        try:
            conn.execute(s)
        finally:
            conn.close()

    @classmethod
    def _load_warehouse(cls, id):
        """Get a Warehouse row from a Warehouse ID
        
        **Parameters:**
        
        * **id** - (*int*) The ID of the Warehouse to load
        
        **Returns:**
        
        (*dict*) - A Warehouse row
                
        """
        s = sa.select(Warehouses.c).where(Warehouses.c.id == id)
        conn = zillion_engine.connect()
        try:
            result = conn.execute(s)
            row = result.fetchone()
            return row
        finally:
            conn.close()
