from collections import defaultdict, OrderedDict
import copy
import datetime
import logging
import random
import time

import networkx as nx
import pandas as pd
import sqlalchemy as sa
from tlbx import (
    dbg,
    info,
    pf,
    st,
    rmfile,
    initializer,
    get_string_format_args,
    iter_or,
    powerset,
    PrintMixin,
    MappingMixin,
)

from zillion.configs import (
    AdHocFieldSchema,
    AdHocMetricSchema,
    WarehouseConfigSchema,
    ColumnInfo,
    TableInfo,
    MetricConfigSchema,
    DimensionConfigSchema,
    parse_technical_string,
    is_valid_field_name,
    zillion_config,
    DATASOURCE_ALLOWABLE_CHARS,
)
from zillion.core import (
    UnsupportedGrainException,
    InvalidFieldException,
    MaxFormulaDepthException,
    WarehouseException,
    TableTypes,
)
from zillion.datasource import DataSource, AdHocDataSource, datasource_from_config
from zillion.field import (
    Field,
    Metric,
    Dimension,
    FormulaMetric,
    create_metric,
    create_dimension,
    get_table_metrics,
    get_table_dimensions,
    get_table_fields,
    FieldManagerMixin,
)
from zillion.report import Report
from zillion.sql_utils import (
    infer_aggregation_and_rounding,
    aggregation_to_sqla_func,
    is_probably_metric,
    sqla_compile,
    column_fullname,
)

if zillion_config["DEBUG"]:
    logging.getLogger().setLevel(logging.DEBUG)


class Warehouse(FieldManagerMixin):
    def __init__(
        self, config=None, datasources=None, ds_priority=None, if_exists="fail"
    ):
        """
        if_exists: only applies when an adhoc datasource will be created
        """
        self.datasources = OrderedDict()
        self._metrics = {}
        self._dimensions = {}
        self._supported_dimension_cache = {}
        self._created_adhoc_datasources = set()

        for ds in datasources or []:
            self.add_datasource(ds, skip_integrity_checks=True)

        if config:
            config = WarehouseConfigSchema().load(config)
            self.apply_config(config, skip_integrity_checks=True, if_exists=if_exists)

        assert self.datasources, "No datasources provided or found in config"
        self.run_integrity_checks()

        self.ds_priority = ds_priority or list(self.datasources.keys())

        assert isinstance(self.ds_priority, list), (
            "Invalid format for ds_priority, must be list of datasource names: %s"
            % self.ds_priority
        )
        assert len(self.ds_priority) == len(
            self.datasources
        ), "Length mismatch between ds_priority and datasources"
        for ds_name in self.ds_priority:
            assert ds_name in self.datasources, (
                "Datasource %s is in ds_priority but not in datasource map" % ds_name
            )

    def __repr__(self):
        return "<%s> Datasources: %s" % (
            self.__class__.__name__,
            self.get_datasource_names(),
        )

    def clean_up(self):
        for ds_name, ds in self.datasources.items():
            if ds_name in self._created_adhoc_datasources:
                info("Cleaning up warehouse adhoc ds %s" % ds)
                ds.clean_up()

    def print_info(self):
        print("---- Warehouse")
        print("metrics:")
        self.print_metrics(indent=2)
        print("dimensions:")
        self.print_dimensions(indent=2)

        for ds in self.get_datasources():
            print()
            ds.print_info()

    def get_datasources(self):
        return self.datasources.values()

    def get_datasource_names(self):
        return self.datasources.keys()

    def get_datasource(self, name, adhoc_datasources=None):
        if name in self.datasources:
            return self.datasources[name]

        for adhoc_datasource in adhoc_datasources or []:
            if adhoc_datasource.name == name:
                return adhoc_datasource

        assert False, 'Could not find datasource with name "%s"' % name

    def get_child_field_managers(self):
        return list(self.get_datasources())

    def add_datasource(self, ds, skip_integrity_checks=False):
        dbg("Adding datasource %s" % ds.name)
        self.datasources[ds.name] = ds
        if not skip_integrity_checks:
            self.run_integrity_checks()

    def remove_datasource(self, ds):
        dbg("Removing datasource %s" % ds.name)
        del self.datasources[ds.name]

    def create_or_update_datasources(
        self, ds_configs, skip_integrity_checks=False, if_exists="fail"
    ):
        for ds_name in ds_configs:
            if ds_name in self.datasources:
                self.datasources[ds_name].apply_config(ds_configs[ds_name])
                continue

            ds = datasource_from_config(
                ds_name, ds_configs[ds_name], if_exists=if_exists
            )
            if isinstance(ds, AdHocDataSource):
                # We track this so we can clean up later
                self._created_adhoc_datasources.add(ds.name)
            self.add_datasource(ds, skip_integrity_checks=skip_integrity_checks)

    def apply_config(self, config, skip_integrity_checks=False, if_exists="fail"):
        self.create_or_update_datasources(
            config.get("datasources", {}),
            skip_integrity_checks=skip_integrity_checks,
            if_exists=if_exists,
        )
        self.populate_global_fields(config, force=True)

    def _check_conflicting_fields(self, adhoc_datasources=None):
        # TODO: in addition to checking metric vs dimension settings
        # we could add type comparisons (make sure its always num or str)
        errors = []

        for field in self.get_field_names(adhoc_fms=adhoc_datasources):
            if self.has_metric(
                field, adhoc_fms=adhoc_datasources
            ) and self.has_dimension(field, adhoc_fms=adhoc_datasources):
                errors.append("Field %s is in both metrics and dimensions" % field)

        return errors

    def _check_fields_have_type(self, adhoc_datasources=None):
        errors = []

        for ds in self.get_field_managers(adhoc_fms=adhoc_datasources):
            for table in ds.metadata.tables.values():
                if not table.zillion:
                    continue

                for column in table.c:
                    if not column.zillion:
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
        errors = []

        for ds in self.get_field_managers(adhoc_fms=adhoc_datasources):
            for table in ds.metadata.tables.values():
                if not table.zillion:
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

        return errors

    def run_integrity_checks(self, adhoc_datasources=None):
        errors = []
        if adhoc_datasources:
            for ds in adhoc_datasources:
                if ds.name in self.datasources:
                    errors.append(
                        "Adhoc DataSource '%s' name conflicts with existing DataSource"
                        % ds.name
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
        if errors:
            raise WarehouseException("Integrity check(s) failed.\n%s" % pf(errors))

    def get_supported_dimensions_for_metric(
        self, metric, use_cache=True, adhoc_datasources=None
    ):
        dims = set()
        metric = self.get_metric(metric, adhoc_fms=adhoc_datasources)

        if use_cache and metric.name in self._supported_dimension_cache:
            # XXX TODO: When to clear cache?
            return self._supported_dimension_cache[metric]

        for ds in self.get_datasources():
            ds_tables = ds.get_tables_with_field(metric.name)
            used_tables = set()

            for ds_table in ds_tables:
                if ds_table.fullname not in used_tables:
                    dims |= get_table_dimensions(
                        self, ds_table, adhoc_fms=adhoc_datasources
                    )
                    used_tables.add(ds_table.fullname)

                desc_tables = nx.descendants(ds.graph, ds_table.fullname)
                for desc_table in desc_tables:
                    if desc_table not in used_tables:
                        dims |= get_table_dimensions(
                            self, ds.get_table(desc_table), adhoc_fms=adhoc_datasources
                        )
                        used_tables.add(desc_table)

        self._supported_dimension_cache[metric] = dims
        return dims

    def get_supported_dimensions(self, metrics, adhoc_datasources=None):
        dims = set()
        for metric in metrics:
            supported_dims = self.get_supported_dimensions_for_metric(
                metric, adhoc_datasources=adhoc_datasources
            )
            dims = (dims & supported_dims) if len(dims) else supported_dims
        return dims

    def get_ds_tables_with_metric(self, metric, adhoc_datasources=None):
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

    def get_ds_dim_tables_with_dim(self, dim, adhoc_datasources=None):
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

    def get_ds_table_sets(self, ds_tables, field, grain, adhoc_datasources=None):
        """Returns all table sets that can satisfy grain in each datasource"""
        ds_table_sets = {}
        for ds_name, ds_tables_with_field in ds_tables.items():
            ds = self.get_datasource(ds_name, adhoc_datasources=adhoc_datasources)
            possible_table_sets = ds.find_possible_table_sets(
                ds_tables_with_field, field, grain
            )
            if not possible_table_sets:
                continue
            ds_table_sets[ds_name] = possible_table_sets
        dbg(ds_table_sets)
        return ds_table_sets

    def choose_best_data_source(self, ds_names):
        if self.ds_priority:
            for ds_name in self.ds_priority:
                if ds_name in ds_names:
                    return ds_name

        # TODO: eventually it would be nice to choose a datasource if:
        #  A) Its historically been faster
        #  B) All of the requested data can be pulled from one datasource
        info("No datasource priorities established, choosing first option")
        assert ds_names, "No datasource names provided"
        return ds_names[0]

    def choose_best_table_set(self, ds_table_sets):
        ds_name = self.choose_best_data_source(list(ds_table_sets.keys()))
        if len(ds_table_sets[ds_name]) > 1:
            # TODO: establish table set priorities based on expected query performance?
            info(
                "Picking smallest of %d available table sets"
                % len(ds_table_sets[ds_name])
            )
        return sorted(ds_table_sets[ds_name], key=lambda x: len(x))[0]

    def generate_unsupported_grain_msg(self, grain, metric, adhoc_datasources=None):
        """
        This assumes you are in a situation where you are sure the metric can not
        meet the grain and want to generate a helpful message pinpointing the
        issue.  If the metric actually supports all dimensions, the conclusion
        is that it just doesn't support them all in a single datasource and
        thus can't meet the grain.
        """
        supported = self.get_supported_dimensions_for_metric(
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

    def get_metric_table_set(self, metric, grain, adhoc_datasources=None):
        dbg("metric:%s grain:%s" % (metric, grain))
        ds_metric_tables = self.get_ds_tables_with_metric(
            metric, adhoc_datasources=adhoc_datasources
        )
        ds_table_sets = self.get_ds_table_sets(
            ds_metric_tables, metric, grain, adhoc_datasources=adhoc_datasources
        )
        if not ds_table_sets:
            msg = self.generate_unsupported_grain_msg(
                grain, metric, adhoc_datasources=adhoc_datasources
            )
            raise UnsupportedGrainException(msg)
        table_set = self.choose_best_table_set(ds_table_sets)
        return table_set

    def get_dimension_table_set(self, grain, adhoc_datasources=None):
        """
        This is meant to be used in cases where no metrics are requested. We only
        allow it to look at dim tables since the assumption is joining to a metric
        table to explore dimensions doesn't make sense and would have poor performance.
        """
        dbg("grain:%s" % grain)

        table_set = None
        for dim_name in grain:
            ds_dim_tables = self.get_ds_dim_tables_with_dim(
                dim_name, adhoc_datasources=adhoc_datasources
            )
            ds_table_sets = self.get_ds_table_sets(
                ds_dim_tables, dim_name, grain, adhoc_datasources=adhoc_datasources
            )
            if not ds_table_sets:
                continue
            table_set = self.choose_best_table_set(ds_table_sets)
            break

        if not table_set:
            raise UnsupportedGrainException(
                "No dimension table set found to meet grain: %s" % grain
            )
        return table_set

    def load_report(self, report_id, adhoc_datasources=None):
        report = Report.load(self, report_id, adhoc_datasources=adhoc_datasources)
        return report

    def delete_report(self, report_id):
        report = Report.delete(report_id)
        return report

    def save_report(self, **kwargs):
        report = Report(self, **kwargs)
        report.save()
        return report

    def execute(
        self,
        metrics=None,
        dimensions=None,
        criteria=None,
        row_filters=None,
        rollup=None,
        pivot=None,
        adhoc_datasources=None,
    ):
        start = time.time()

        report = Report(
            self,
            metrics=metrics,
            dimensions=dimensions,
            criteria=criteria,
            row_filters=row_filters,
            rollup=rollup,
            pivot=pivot,
            adhoc_datasources=adhoc_datasources,
        )
        result = report.execute()

        dbg("warehouse report took %.3fs" % (time.time() - start))
        return result

    def execute_id(self, report_id, adhoc_datasources=None):
        start = time.time()
        report = self.load_report(report_id, adhoc_datasources=adhoc_datasources)
        result = report.execute()
        dbg("warehouse report took %.3fs" % (time.time() - start))
        return result
