import copy
import traceback

from tlbx import dbg, st, Script, Arg

from zillion.configs import load_warehouse_config
from zillion.core import (TableTypes,
                          UnsupportedGrainException,
                          InvalidFieldException)
from zillion.sql_utils import contains_aggregation
from zillion.report import ROLLUP_INDEX_LABEL, ROLLUP_TOTALS
from zillion.warehouse import (DataSource,
                               AdHocDataSource,
                               Warehouse)
from test_utils import TestBase, run_tests, create_adhoc_datatable, get_testdb_url

TEST_CONFIG = load_warehouse_config('test_config.json')

def init_datasources():
    ds1 = DataSource('testdb1', get_testdb_url('testdb1'), reflect=True)
    ds2 = DataSource('testdb2', get_testdb_url('testdb2'), reflect=True)
    # ds2 will end up with a higher priority
    return [ds2, ds1]

def get_adhoc_datasource():
    column_defs = {
        'partner_name': {
            'fields': ['partner_name'],
            'type': str,
        },
        'adhoc_fact': {
            'fields': ['adhoc_fact'],
            'type': float,
        }
    }

    size = 10
    dt = create_adhoc_datatable('adhoc_table1', TableTypes.FACT, column_defs, ['partner_name'], size)
    adhoc_ds = AdHocDataSource([dt])
    return adhoc_ds

class TestZillion(TestBase):
    def setUp(self):
        self.datasources = init_datasources()
        self.ds_priority = [ds.name for ds in self.datasources]
        self.config = copy.deepcopy(TEST_CONFIG)

    def tearDown(self):
        del self.datasources
        self.config = None

    def getWarehouse(self):
        return Warehouse(self.datasources, config=self.config, ds_priority=self.ds_priority)

    def testWarehouseInit(self):
        wh = self.getWarehouse()
        self.assertTrue(wh.dimension_tables)
        self.assertTrue(wh.dimensions)

    def testWarehouseNoConfig(self):
        wh = Warehouse(self.datasources)
        self.assertFalse(wh.dimensions)

    def testWarehouseNoConfigHasZillionInfo(self):
        for table in self.datasources[0].metadata.tables.values():
            table.info['zillion'] = {'type': 'fact', 'active': True, 'autocolumns':True}
        wh = Warehouse(self.datasources)
        self.assertTrue(wh.dimensions)

    def testTableConfigOverride(self):
        self.config['datasources']['testdb1']['tables']['campaigns']['type'] = 'fact'
        wh = self.getWarehouse()
        self.assertIn('campaigns', wh.fact_tables['testdb1'])

    def testColumnConfigOverride(self):
        table_config = self.config['datasources']['testdb1']['tables']['sales']
        table_config['columns']['lead_id']['active'] = False
        wh = self.getWarehouse()
        self.assertNotIn('sales.lead_id', wh.dimensions['lead_id'].get_column_names('testdb1'))

    def testContainsAggregation(self):
        sql_with_aggr = [
            'select sum(column) from table',
            'select avg(column) from table',
            'sum(column)',
            'avg(column)',
        ]

        sql_without_aggr = [
            'select column from table',
            'column',
            'a + b',
            '(a) + (b)',
            '(a + b)',
            'sum',
            'sum + avg',
        ]

        for sql in sql_with_aggr:
            self.assertTrue(contains_aggregation(sql))

        for sql in sql_without_aggr:
            self.assertFalse(contains_aggregation(sql))

    def testGetDimensionTableSet(self):
        wh = self.getWarehouse()
        possible = [
            {'partner_id', 'partner_name'},
            {'campaign_name', 'partner_name'},
        ]

        impossible = [
            {'lead_id', 'partner_id'},
            {'sale_id', 'lead_id', 'campaign_name', 'partner_name'}
        ]

        for grain in possible:
            try:
                wh.get_dimension_table_set(grain)
                # TODO: assert specific table set?
            except UnsupportedGrainException:
                print(traceback.format_exc())
                self.fail('Could not satisfy grain: %s' % grain)

        for grain in impossible:
            with self.assertRaises(UnsupportedGrainException):
                wh.get_dimension_table_set(grain)

    def testGetFactTableSet(self):
        wh = self.getWarehouse()
        possible = [
            ('leads', {'partner_id', 'partner_name'}),
            ('leads', {'campaign_name', 'partner_name'}),
            ('revenue', {'campaign_name', 'partner_name', 'lead_id'}),
        ]

        impossible = [
            ('leads', {'sale_id'}),
        ]

        for fact, grain in possible:
            try:
                wh.get_fact_table_set(fact, grain)
                # TODO: assert specific table set?
            except UnsupportedGrainException:
                print(traceback.format_exc())
                self.fail('Could not satisfy fact %s at grain: %s' % (fact, grain))

        for fact, grain in impossible:
            with self.assertRaises(UnsupportedGrainException):
                wh.get_fact_table_set(fact, grain)

    def testGetSupportedDimensions(self):
        wh = self.getWarehouse()
        facts = ['leads', 'sales_quantity']
        dims = wh.get_supported_dimensions(facts)
        self.assertTrue(dims & {'campaign_name', 'partner_name'})
        self.assertFalse(dims & {'sale_id'})

    def testReport(self):
        wh = self.getWarehouse()
        facts = ['revenue', 'sales_quantity']
        dimensions = ['partner_name', 'campaign_name']
        criteria = [('campaign_name', '!=', 'Campaign 2B')]
        row_filters = [('revenue', '>', 11)]
        rollup = ROLLUP_TOTALS
        result = wh.execute(facts, dimensions=dimensions, criteria=criteria,
                            row_filters=row_filters, rollup=rollup)
        dbg(result)
        self.assertTrue(result)

    def testImpossibleReport(self):
        wh = self.getWarehouse()
        facts = ['leads']
        dimensions = ['sale_id']
        criteria = [('campaign_name', '!=', 'Campaign 2B')]
        with self.assertRaises(UnsupportedGrainException):
            result = wh.execute(facts, dimensions=dimensions, criteria=criteria)

    def testReportPivot(self):
        wh = self.getWarehouse()
        facts = ['revenue', 'sales_quantity']
        dimensions = ['partner_name', 'campaign_name']
        criteria = [('campaign_name', '!=', 'Campaign 2B')]
        row_filters = [('revenue', '>', 11)]
        rollup = ROLLUP_TOTALS
        pivot = ['partner_name']
        result = wh.execute(facts, dimensions=dimensions, criteria=criteria,
                            row_filters=row_filters, rollup=rollup, pivot=pivot)
        self.assertTrue(result)

    def testReportMovingAverageFact(self):
        wh = self.getWarehouse()
        facts = [
            'revenue',
            'revenue_ma_5',
        ]
        # TODO: it doesnt make sense to use these dimensions, but no date/time
        # dims have been added as of the time of creating this test.
        dimensions = ['partner_name', 'campaign_name']
        criteria = [('campaign_name', '!=', 'Campaign 2B')]
        row_filters = [('revenue', '>', 8)]
        rollup = ROLLUP_TOTALS
        result = wh.execute(facts, dimensions=dimensions, criteria=criteria,
                            row_filters=row_filters, rollup=rollup)
        self.assertTrue(result)

    def testReportMovingAverageAdHocFact(self):
        wh = self.getWarehouse()
        facts = [
            'revenue',
            {'formula': '{revenue}', 'technical': 'MA-5', 'name': 'my_revenue_ma_5'},
        ]
        # TODO: it doesnt make sense to use these dimensions, but no date/time
        # dims have been added as of the time of creating this test.
        dimensions = ['partner_name', 'campaign_name']
        criteria = [('campaign_name', '!=', 'Campaign 2B')]
        row_filters = [('revenue', '>', 8)]
        rollup = ROLLUP_TOTALS
        result = wh.execute(facts, dimensions=dimensions, criteria=criteria,
                            row_filters=row_filters, rollup=rollup)
        self.assertTrue(result)

    def testReportMovingAverageFormulaFact(self):
        wh = self.getWarehouse()
        facts = [
            'revenue',
            'rpl',
            'rpl_ma_5',
        ]
        # TODO: it doesnt make sense to use these dimensions, but no date/time
        # dims have been added as of the time of creating this test.
        dimensions = ['partner_name', 'campaign_name']
        criteria = [('campaign_name', '!=', 'Campaign 2B')]
        row_filters = [('revenue', '>', 8)]
        rollup = ROLLUP_TOTALS
        result = wh.execute(facts, dimensions=dimensions, criteria=criteria,
                            row_filters=row_filters, rollup=rollup)
        self.assertTrue(result)

    def testReportCumSumFact(self):
        wh = self.getWarehouse()
        facts = [
            'revenue',
            'revenue_sum_5',
        ]
        # TODO: it doesnt make sense to use these dimensions, but no date/time
        # dims have been added as of the time of creating this test.
        dimensions = ['partner_name', 'campaign_name']
        criteria = [('campaign_name', '!=', 'Campaign 2B')]
        row_filters = None
        rollup = ROLLUP_TOTALS
        result = wh.execute(facts, dimensions=dimensions, criteria=criteria,
                            row_filters=row_filters, rollup=rollup)
        self.assertTrue(result)

    def testReportBollingerFact(self):
        wh = self.getWarehouse()
        facts = [
            'revenue',
            'revenue_boll_5',
        ]
        # TODO: it doesnt make sense to use these dimensions, but no date/time
        # dims have been added as of the time of creating this test.
        dimensions = ['partner_name', 'campaign_name']
        criteria = [('campaign_name', '!=', 'Campaign 2B')]
        row_filters = None
        rollup = ROLLUP_TOTALS
        result = wh.execute(facts, dimensions=dimensions, criteria=criteria,
                            row_filters=row_filters, rollup=rollup)
        self.assertTrue(result)

    def testReportNoDimensions(self):
        wh = self.getWarehouse()
        facts = ['revenue', 'sales_quantity']
        criteria = [('campaign_name', '=', 'Campaign 2B')]
        result = wh.execute(facts, criteria=criteria)
        self.assertTrue(result)

    def testReportNoFacts(self):
        wh = self.getWarehouse()
        facts = []
        dimensions = ['partner_name', 'campaign_name']
        result = wh.execute(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportNullCriteria(self):
        wh = self.getWarehouse()
        facts = ['revenue']
        dimensions = ['partner_name']
        criteria = [('campaign_name', '!=', None)]
        result = wh.execute(facts, dimensions=dimensions, criteria=criteria)
        self.assertTrue(result)

    def testReportCountFact(self):
        wh = self.getWarehouse()
        facts = ['leads']
        dimensions = ['campaign_name']
        result = wh.execute(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportAliasFact(self):
        wh = self.getWarehouse()
        facts = ['revenue_avg']
        dimensions = ['partner_name']
        result = wh.execute(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportAliasDimension(self):
        wh = self.getWarehouse()
        facts = ['revenue']
        dimensions = ['lead_id']
        result = wh.execute(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportMultipleQueries(self):
        wh = self.getWarehouse()
        facts = ['revenue', 'leads']
        dimensions = ['partner_name']
        result = wh.execute(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportFormulaFact(self):
        wh = self.getWarehouse()
        facts = ['rpl', 'revenue', 'leads']
        dimensions = ['partner_name']
        result = wh.execute(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportNestedFormulaFact(self):
        wh = self.getWarehouse()
        facts = ['rpl_squared', 'rpl_unsquared', 'rpl', 'leads']
        dimensions = ['partner_name']
        result = wh.execute(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportDSDimensionFormula(self):
        wh = self.getWarehouse()
        facts = ['sales']
        dimensions = ['revenue_decile']
        result = wh.execute(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportDSFactFormula(self):
        wh = self.getWarehouse()
        facts = ['revenue', 'revenue_ds']
        dimensions = ['partner_name']
        result = wh.execute(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportNonExistentFact(self):
        wh = self.getWarehouse()
        facts = ['sales1234']
        dimensions = ['campaign_id']
        result = False
        try:
            wh.execute(facts, dimensions=dimensions)
        except InvalidFieldException:
            result = True
        self.assertTrue(result)

    def testReportWeightedFormulaFact(self):
        wh = self.getWarehouse()
        facts = ['rpl_weighted', 'rpl', 'sales_quantity', 'revenue', 'leads']
        dimensions = ['partner_name']
        result = wh.execute(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportWeightedDSFactFormula(self):
        wh = self.getWarehouse()
        facts = ['revenue_avg', 'revenue_avg_ds_weighted']
        dimensions = ['partner_name']
        result = wh.execute(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportWeightedFact(self):
        wh = self.getWarehouse()
        facts = ['sales_quantity', 'revenue_avg', 'revenue', 'leads']
        dimensions = ['partner_name']
        result = wh.execute(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportWeightedFactWithRollup(self):
        wh = self.getWarehouse()
        facts = ['sales_quantity', 'revenue_avg', 'leads']
        dimensions = ['partner_name']
        rollup = ROLLUP_TOTALS
        result = wh.execute(facts, dimensions=dimensions, rollup=rollup)
        self.assertTrue(result)

    def testReportWeightedFactWithMultiRollup(self):
        wh = self.getWarehouse()
        facts = ['sales_quantity', 'revenue_avg', 'leads']
        dimensions = ['partner_name', 'campaign_name', 'lead_id']
        rollup = 2
        result = wh.execute(facts, dimensions=dimensions, rollup=rollup)
        self.assertTrue(result)

    def testReportMultiDimension(self):
        wh = self.getWarehouse()
        facts = ['leads', 'sales']
        dimensions = ['partner_name', 'lead_id']
        result = wh.execute(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportRollup(self):
        wh = self.getWarehouse()
        facts = ['revenue']
        dimensions = ['partner_name', 'campaign_name']
        criteria = [('campaign_name', '!=', 'Campaign 2B')]
        rollup = ROLLUP_TOTALS
        result = wh.execute(facts, dimensions=dimensions, criteria=criteria, rollup=rollup)
        revenue = result.rollup_rows().iloc[-1]['revenue']
        revenue_sum = result.non_rollup_rows().sum()['revenue']
        self.assertEqual(revenue, revenue_sum)

    def testReportMultiRollup(self):
        wh = self.getWarehouse()
        facts = ['revenue']
        dimensions = ['partner_name', 'campaign_name', 'lead_id']
        criteria = [('campaign_name', '!=', 'Campaign 2B')]
        rollup = 3
        result = wh.execute(facts, dimensions=dimensions, criteria=criteria, rollup=rollup)
        revenue = result.rollup_rows().iloc[-1]['revenue']
        revenue_sum = result.non_rollup_rows().sum()['revenue']
        self.assertEqual(revenue, revenue_sum)

    def testReportMultiRollupPivot(self):
        wh = self.getWarehouse()
        facts = ['revenue']
        dimensions = ['partner_name', 'campaign_name', 'lead_id']
        criteria = [('campaign_name', '!=', 'Campaign 2B')]
        rollup = 3
        pivot = ['campaign_name']
        result = wh.execute(facts, dimensions=dimensions, criteria=criteria, rollup=rollup, pivot=pivot)
        self.assertTrue(result)

    def testReportAdHocDimension(self):
        wh = self.getWarehouse()
        facts = ['leads', 'sales']
        dimensions = ['partner_name',
                      'lead_id',
                      {'formula':'{lead_id} > 3', 'name':'testdim'}]
        result = wh.execute(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportAdHocFact(self):
        wh = self.getWarehouse()
        facts = ['revenue', {'formula': '{revenue} > 3*{lead_id}', 'name': 'testfact'}]
        dimensions = ['partner_name', 'lead_id']
        result = wh.execute(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportAdHocDataSource(self):
        wh = self.getWarehouse()
        facts = ['revenue', 'adhoc_fact']
        dimensions = ['partner_name']
        adhoc_ds = get_adhoc_datasource()
        result = wh.execute(facts, dimensions=dimensions, adhoc_datasources=[adhoc_ds])
        self.assertTrue(result)

    def testDateConversionReport(self):
        wh = self.getWarehouse()
        facts = ['revenue']
        dimensions = [
            'datetime',
            'hour_of_day',
        ]
        criteria = [('campaign_name', '!=', 'Campaign 2B')]
        result = wh.execute(facts, dimensions=dimensions, criteria=criteria)
        dbg(result)
        self.assertTrue(result)

    def testReportDataSourcePriority(self):
        wh = self.getWarehouse()
        facts = ['revenue', 'leads', 'sales']
        dimensions = ['partner_name']
        report = wh.build_report(facts=facts, dimensions=dimensions)
        self.assertTrue(report.queries[0].get_datasource_name() == 'testdb2')

    def testReportMultiDataSource(self):
        wh = self.getWarehouse()
        facts = ['revenue', 'leads', 'sales', 'revenue_avg']
        dimensions = ['partner_name']
        report = wh.build_report(facts=facts, dimensions=dimensions)
        self.assertTrue(len(report.queries) == 2)
        result = report.execute()
        dbg(result)
        self.assertTrue(result)

    def testReportSaveAndLoad(self):
        wh = self.getWarehouse()
        facts = ['revenue', 'leads', 'sales']
        dimensions = ['partner_name']
        report = wh.build_report(facts=facts, dimensions=dimensions)
        report_id = report.save()
        result = wh.execute_id(report_id)
        self.assertTrue(result)

    def testReportAdHocDataSourceSaveAndLoad(self):
        wh = self.getWarehouse()
        facts = ['revenue', 'leads', 'adhoc_fact']
        dimensions = ['partner_name']
        adhoc_ds = get_adhoc_datasource()
        wh.add_adhoc_datasources([adhoc_ds])
        report = wh.build_report(facts=facts, dimensions=dimensions)
        report_id = report.save()
        result = wh.execute_id(report_id)
        self.assertTrue(result)

    # TODO: this is failing, doesnt seem to be loading adhoc DS correctly
    # def testReportMissingAdHocDataSourceSaveAndLoad(self):
    #     wh = self.getWarehouse()
    #     facts = ['revenue', 'leads', 'adhoc_fact']
    #     dimensions = ['partner_name']
    #     adhoc_ds = get_adhoc_datasource()
    #     wh.add_adhoc_datasources([adhoc_ds])
    #     report = wh.build_report(facts=facts, dimensions=dimensions)
    #     report_id = report.save()
    #     wh.remove_adhoc_datasources([adhoc_ds])
    #     result = wh.execute_id(report_id)
    #     # TODO: should clean up report IDs when done
    #     self.assertTrue(result)

    def testReportFromId(self):
        wh = self.getWarehouse()
        result = wh.execute_id(6)
        self.assertTrue(result)

@Script(Arg('testnames', type=str, nargs='*', help='Names of tests to run'),
        Arg('--debug', action='store_true'))
def main(testnames, debug):
    run_tests(TestZillion, testnames, debug)

if __name__ == '__main__':
    main()
