import copy
import traceback

import climax
from toolbox import dbg, st, testcli

from sqlaw.configs import load_warehouse_config
from sqlaw.core import (TableTypes,
                        UnsupportedGrainException,
                        InvalidFieldException)
from sqlaw.sql_utils import contains_aggregation
from sqlaw.report import ROLLUP_INDEX_LABEL, ROLLUP_TOTALS
from sqlaw.warehouse import (DataSource,
                             AdHocDataSource,
                             Warehouse)
from test_utils import TestBase, run_tests, create_adhoc_datatable

TESTDB_CONFIG = load_warehouse_config('testdb_config.json')

def init_datasources():
    ds = DataSource('testdb', 'sqlite:///testdb', reflect=True)
    return [ds]

class TestSQLAW(TestBase):
    def setUp(self):
        self.datasources = init_datasources()
        self.config = copy.deepcopy(TESTDB_CONFIG)

    def tearDown(self):
        del self.datasources
        self.config = None

    def testWarehouseInit(self):
        wh = Warehouse(self.datasources, config=self.config)
        self.assertTrue(wh.dimension_tables)
        self.assertTrue(wh.dimensions)

    def testWarehouseNoConfig(self):
        wh = Warehouse(self.datasources)
        self.assertFalse(wh.dimensions)

    def testWarehouseNoConfigHasSQLAWInfo(self):
        for table in self.datasources[0].metadata.tables.values():
            table.info['sqlaw'] = {'type': 'fact', 'active': True, 'autocolumns':True}
        wh = Warehouse(self.datasources)
        self.assertTrue(wh.dimensions)

    def testTableConfigOverride(self):
        self.config['datasources']['testdb']['tables']['campaigns']['type'] = 'fact'
        wh = Warehouse(self.datasources, config=self.config)
        self.assertIn('campaigns', wh.fact_tables['testdb'])

    def testColumnConfigOverride(self):
        table_config = self.config['datasources']['testdb']['tables']['sales']
        table_config['columns']['lead_id']['active'] = False
        wh = Warehouse(self.datasources, config=self.config)
        self.assertNotIn('sales.lead_id', wh.dimensions['lead_id'].get_column_names('testdb'))

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
        wh = Warehouse(self.datasources, config=self.config)
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
        wh = Warehouse(self.datasources, config=self.config)
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
        wh = Warehouse(self.datasources, config=self.config)
        facts = ['leads', 'sales_quantity']
        dims = wh.get_supported_dimensions(facts)
        self.assertTrue(dims & {'campaign_name', 'partner_name'})
        self.assertFalse(dims & {'sale_id'})

    def testReport(self):
        wh = Warehouse(self.datasources, config=self.config)
        facts = ['revenue', 'sales_quantity']
        dimensions = ['partner_name', 'campaign_name']
        criteria = [('campaign_name', '!=', 'Campaign 2B')]
        row_filters = [('revenue', '>', 11)]
        rollup = ROLLUP_TOTALS
        result = wh.report(facts, dimensions=dimensions, criteria=criteria,
                           row_filters=row_filters, rollup=rollup)
        dbg(result)
        self.assertTrue(result)

    def testImpossibleReport(self):
        wh = Warehouse(self.datasources, config=self.config)
        facts = ['leads']
        dimensions = ['sale_id']
        criteria = [('campaign_name', '!=', 'Campaign 2B')]
        with self.assertRaises(UnsupportedGrainException):
            result = wh.report(facts, dimensions=dimensions, criteria=criteria)

    def testReportPivot(self):
        wh = Warehouse(self.datasources, config=self.config)
        facts = ['revenue', 'sales_quantity']
        dimensions = ['partner_name', 'campaign_name']
        criteria = [('campaign_name', '!=', 'Campaign 2B')]
        row_filters = [('revenue', '>', 11)]
        rollup = ROLLUP_TOTALS
        pivot = ['partner_name']
        result = wh.report(facts, dimensions=dimensions, criteria=criteria,
                           row_filters=row_filters, rollup=rollup, pivot=pivot)
        self.assertTrue(result)

    def testReportMovingAverageFact(self):
        wh = Warehouse(self.datasources, config=self.config)
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
        result = wh.report(facts, dimensions=dimensions, criteria=criteria,
                           row_filters=row_filters, rollup=rollup)
        self.assertTrue(result)

    def testReportMovingAverageAdHocFact(self):
        wh = Warehouse(self.datasources, config=self.config)
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
        result = wh.report(facts, dimensions=dimensions, criteria=criteria,
                           row_filters=row_filters, rollup=rollup)
        self.assertTrue(result)

    def testReportMovingAverageFormulaFact(self):
        wh = Warehouse(self.datasources, config=self.config)
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
        result = wh.report(facts, dimensions=dimensions, criteria=criteria,
                           row_filters=row_filters, rollup=rollup)
        self.assertTrue(result)

    def testReportCumSumFact(self):
        wh = Warehouse(self.datasources, config=self.config)
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
        result = wh.report(facts, dimensions=dimensions, criteria=criteria,
                           row_filters=row_filters, rollup=rollup)
        self.assertTrue(result)

    def testReportBollingerFact(self):
        wh = Warehouse(self.datasources, config=self.config)
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
        result = wh.report(facts, dimensions=dimensions, criteria=criteria,
                           row_filters=row_filters, rollup=rollup)
        self.assertTrue(result)

    def testReportNoDimensions(self):
        wh = Warehouse(self.datasources, config=self.config)
        facts = ['revenue', 'sales_quantity']
        criteria = [('campaign_name', '=', 'Campaign 2B')]
        result = wh.report(facts, criteria=criteria)
        self.assertTrue(result)

    def testReportNoFacts(self):
        wh = Warehouse(self.datasources, config=self.config)
        facts = []
        dimensions = ['partner_name', 'campaign_name']
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportNullCriteria(self):
        wh = Warehouse(self.datasources, config=self.config)
        facts = ['revenue']
        dimensions = ['partner_name']
        criteria = [('campaign_name', '!=', None)]
        result = wh.report(facts, dimensions=dimensions, criteria=criteria)
        self.assertTrue(result)

    def testReportCountFact(self):
        wh = Warehouse(self.datasources, config=self.config)
        facts = ['leads']
        dimensions = ['campaign_name']
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportAliasFact(self):
        wh = Warehouse(self.datasources, config=self.config)
        facts = ['revenue_avg']
        dimensions = ['partner_name']
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportAliasDimension(self):
        wh = Warehouse(self.datasources, config=self.config)
        facts = ['revenue']
        dimensions = ['lead_id']
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportMultipleQueries(self):
        wh = Warehouse(self.datasources, config=self.config)
        facts = ['revenue', 'leads']
        dimensions = ['partner_name']
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportFormulaFact(self):
        wh = Warehouse(self.datasources, config=self.config)
        facts = ['rpl', 'revenue', 'leads']
        dimensions = ['partner_name']
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportNestedFormulaFact(self):
        wh = Warehouse(self.datasources, config=self.config)
        facts = ['rpl_squared', 'rpl_unsquared', 'rpl', 'leads']
        dimensions = ['partner_name']
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportDSDimensionFormula(self):
        wh = Warehouse(self.datasources, config=self.config)
        facts = ['sales']
        dimensions = ['revenue_decile']
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportDSFactFormula(self):
        wh = Warehouse(self.datasources, config=self.config)
        facts = ['revenue', 'revenue_ds']
        dimensions = ['partner_name']
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportNonExistentFact(self):
        wh = Warehouse(self.datasources, config=self.config)
        facts = ['sales1234']
        dimensions = ['campaign_id']
        result = False
        try:
            wh.report(facts, dimensions=dimensions)
        except InvalidFieldException:
            result = True
        self.assertTrue(result)

    def testReportWeightedFormulaFact(self):
        wh = Warehouse(self.datasources, config=self.config)
        facts = ['rpl_weighted', 'rpl', 'sales_quantity', 'revenue', 'leads']
        dimensions = ['partner_name']
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportWeightedDSFactFormula(self):
        wh = Warehouse(self.datasources, config=self.config)
        facts = ['revenue_avg', 'revenue_avg_ds_weighted']
        dimensions = ['partner_name']
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportWeightedFact(self):
        wh = Warehouse(self.datasources, config=self.config)
        facts = ['sales_quantity', 'revenue_avg', 'revenue', 'leads']
        dimensions = ['partner_name']
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportWeightedFactWithRollup(self):
        wh = Warehouse(self.datasources, config=self.config)
        facts = ['sales_quantity', 'revenue_avg', 'leads']
        dimensions = ['partner_name']
        rollup = ROLLUP_TOTALS
        result = wh.report(facts, dimensions=dimensions, rollup=rollup)
        self.assertTrue(result)

    def testReportWeightedFactWithMultiRollup(self):
        wh = Warehouse(self.datasources, config=self.config)
        facts = ['sales_quantity', 'revenue_avg', 'leads']
        dimensions = ['partner_name', 'campaign_name', 'lead_id']
        rollup = 2
        result = wh.report(facts, dimensions=dimensions, rollup=rollup)
        self.assertTrue(result)

    def testReportMultiDimension(self):
        wh = Warehouse(self.datasources, config=self.config)
        facts = ['leads', 'sales']
        dimensions = ['partner_name', 'lead_id']
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportRollup(self):
        wh = Warehouse(self.datasources, config=self.config)
        facts = ['revenue']
        dimensions = ['partner_name', 'campaign_name']
        criteria = [('campaign_name', '!=', 'Campaign 2B')]
        rollup = ROLLUP_TOTALS
        result = wh.report(facts, dimensions=dimensions, criteria=criteria, rollup=rollup)
        revenue = result.rollup_rows().iloc[-1]['revenue']
        revenue_sum = result.non_rollup_rows().sum()['revenue']
        self.assertEqual(revenue, revenue_sum)

    def testReportMultiRollup(self):
        wh = Warehouse(self.datasources, config=self.config)
        facts = ['revenue']
        dimensions = ['partner_name', 'campaign_name', 'lead_id']
        criteria = [('campaign_name', '!=', 'Campaign 2B')]
        rollup = 3
        result = wh.report(facts, dimensions=dimensions, criteria=criteria, rollup=rollup)
        revenue = result.rollup_rows().iloc[-1]['revenue']
        revenue_sum = result.non_rollup_rows().sum()['revenue']
        self.assertEqual(revenue, revenue_sum)

    def testReportMultiRollupPivot(self):
        wh = Warehouse(self.datasources, config=self.config)
        facts = ['revenue']
        dimensions = ['partner_name', 'campaign_name', 'lead_id']
        criteria = [('campaign_name', '!=', 'Campaign 2B')]
        rollup = 3
        pivot = ['campaign_name']
        result = wh.report(facts, dimensions=dimensions, criteria=criteria, rollup=rollup, pivot=pivot)
        self.assertTrue(result)

    def testReportAdHocDimension(self):
        wh = Warehouse(self.datasources, config=self.config)
        facts = ['leads', 'sales']
        dimensions = ['partner_name',
                      'lead_id',
                      {'formula':'{lead_id} > 3', 'name':'testdim'}]
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportAdHocFact(self):
        wh = Warehouse(self.datasources, config=self.config)
        facts = ['revenue', {'formula': '{revenue} > 3*{lead_id}', 'name': 'testfact'}]
        dimensions = ['partner_name', 'lead_id']
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportAdHocDataSource(self):
        wh = Warehouse(self.datasources, config=self.config)
        facts = ['revenue', 'adhoc_fact']
        dimensions = ['partner_name']

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

        result = wh.report(facts, dimensions=dimensions, adhoc_datasources=[adhoc_ds])
        self.assertTrue(result)

    def testDateConversionReport(self):
        wh = Warehouse(self.datasources, config=self.config)
        facts = ['revenue']
        dimensions = [
            'datetime',
            'hour_of_day',
        ]
        criteria = [('campaign_name', '!=', 'Campaign 2B')]
        result = wh.report(facts, dimensions=dimensions, criteria=criteria)
        dbg(result)
        self.assertTrue(result)

@climax.command(parents=[testcli])
@climax.argument('testnames', type=str, nargs='*', help='Names of tests to run')
def main(testnames, debug):
    run_tests(TestSQLAW, testnames, debug)

if __name__ == '__main__':
    main()
