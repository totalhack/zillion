import climax
import copy
from sqlalchemy import create_engine, MetaData

from sqlaw.configs import load_config
from sqlaw.utils import dbg, st, testcli
from sqlaw.warehouse import (DataSourceMap,
                             Warehouse,
                             ROLLUP_INDEX_LABEL,
                             ROLLUP_TOTALS,
                             InvalidFieldException)
from test_utils import TestBase, run_tests

TESTDB_CONFIG = load_config('testdb_config.json')

def init_datasource_map():
    datasource_map = DataSourceMap()
    engine = create_engine('sqlite:///testdb')
    metadata = MetaData()
    metadata.bind = engine
    metadata.reflect()
    datasource_map['testdb'] = metadata
    return datasource_map

class TestSQLAW(TestBase):
    def setUp(self):
        self.ds_map = init_datasource_map()
        self.config = copy.deepcopy(TESTDB_CONFIG)

    def tearDown(self):
        del self.ds_map
        self.config = None

    def testWarehouseInit(self):
        wh = Warehouse(self.ds_map, config=self.config)
        self.assertTrue(wh.dimension_tables)
        self.assertTrue(wh.dimensions)

    # TODO: need to rewrite this test
    #def testTableConfigOverride(self):
    #    self.config['datasources']['testdb']['tables']['sales']['type'] = 'dimension'
    #    wh = Warehouse(self.ds_map, config=self.config)
    #    self.assertIn('sales', wh.dimension_tables['testdb'])

    # TODO: need to rewrite this test
    #def testColumnConfigOverride(self):
    #    table_config = self.config['datasources']['testdb']['tables']['sales']
    #    table_config['columns']['revenue']['active'] = False
    #    wh = Warehouse(self.ds_map, config=self.config)
    #    self.assertNotIn('revenue', wh.facts)

    def testReport(self):
        wh = Warehouse(self.ds_map, config=self.config)
        facts = ['revenue', 'sales_quantity']
        dimensions = ['partner_name', 'campaign_name']
        criteria = [('campaign_name', '!=', 'Campaign 2B')]
        row_filters = [('revenue', '>', 11)]
        rollup = ROLLUP_TOTALS
        result = wh.report(facts, dimensions=dimensions, criteria=criteria,
                           row_filters=row_filters, rollup=rollup)
        self.assertTrue(result)

    def testReportMovingAverageFact(self):
        wh = Warehouse(self.ds_map, config=self.config)
        facts = [
            'revenue',
            'revenue_ma_5',
            #{'formula': '{revenue}', 'technical': 'MA-5', 'name': 'revenue_ma_5'},
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
        wh = Warehouse(self.ds_map, config=self.config)
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
        wh = Warehouse(self.ds_map, config=self.config)
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
        wh = Warehouse(self.ds_map, config=self.config)
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
        wh = Warehouse(self.ds_map, config=self.config)
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
        wh = Warehouse(self.ds_map, config=self.config)
        facts = ['revenue', 'sales_quantity']
        criteria = [('campaign_name', '=', 'Campaign 2B')]
        result = wh.report(facts, criteria=criteria)
        self.assertTrue(result)

    def testReportNoFacts(self):
        wh = Warehouse(self.ds_map, config=self.config)
        facts = []
        dimensions = ['partner_name', 'campaign_name']
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportNullCriteria(self):
        wh = Warehouse(self.ds_map, config=self.config)
        facts = ['revenue']
        dimensions = ['partner_name']
        criteria = [('campaign_name', '!=', None)]
        result = wh.report(facts, dimensions=dimensions, criteria=criteria)
        self.assertTrue(result)

    def testReportCountFact(self):
        wh = Warehouse(self.ds_map, config=self.config)
        facts = ['leads']
        dimensions = ['campaign_name']
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportAliasFact(self):
        wh = Warehouse(self.ds_map, config=self.config)
        facts = ['revenue_avg']
        dimensions = ['partner_name']
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportAliasDimension(self):
        wh = Warehouse(self.ds_map, config=self.config)
        facts = ['revenue']
        dimensions = ['lead_id']
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportMultipleQueries(self):
        wh = Warehouse(self.ds_map, config=self.config)
        facts = ['revenue', 'leads']
        dimensions = ['partner_name']
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportFormulaFact(self):
        wh = Warehouse(self.ds_map, config=self.config)
        facts = ['rpl', 'revenue', 'leads']
        dimensions = ['partner_name']
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportNestedFormulaFact(self):
        wh = Warehouse(self.ds_map, config=self.config)
        facts = ['rpl_squared', 'rpl_unsquared', 'rpl', 'leads']
        dimensions = ['partner_name']
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportDSDimensionFormula(self):
        wh = Warehouse(self.ds_map, config=self.config)
        facts = ['sales']
        dimensions = ['revenue_decile']
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportDSFactFormula(self):
        wh = Warehouse(self.ds_map, config=self.config)
        facts = ['revenue', 'revenue_ds']
        dimensions = ['partner_name']
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportNonExistentFact(self):
        wh = Warehouse(self.ds_map, config=self.config)
        facts = ['sales1234']
        dimensions = ['campaign_id']
        result = False
        try:
            wh.report(facts, dimensions=dimensions)
        except InvalidFieldException:
            result = True
        self.assertTrue(result)

    def testReportWeightedFormulaFact(self):
        wh = Warehouse(self.ds_map, config=self.config)
        facts = ['rpl_weighted', 'rpl', 'sales_quantity', 'revenue', 'leads']
        dimensions = ['partner_name']
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportWeightedDSFactFormula(self):
        wh = Warehouse(self.ds_map, config=self.config)
        facts = ['revenue_avg', 'revenue_avg_ds_weighted']
        dimensions = ['partner_name']
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportWeightedFact(self):
        wh = Warehouse(self.ds_map, config=self.config)
        facts = ['sales_quantity', 'revenue_avg', 'revenue', 'leads']
        dimensions = ['partner_name']
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportWeightedFactWithRollup(self):
        wh = Warehouse(self.ds_map, config=self.config)
        facts = ['sales_quantity', 'revenue_avg', 'leads']
        dimensions = ['partner_name']
        rollup = ROLLUP_TOTALS
        result = wh.report(facts, dimensions=dimensions, rollup=rollup)
        self.assertTrue(result)

    def testReportWeightedFactWithMultiRollup(self):
        wh = Warehouse(self.ds_map, config=self.config)
        facts = ['sales_quantity', 'revenue_avg', 'leads']
        dimensions = ['partner_name', 'campaign_name', 'lead_id']
        rollup = 2
        result = wh.report(facts, dimensions=dimensions, rollup=rollup)
        self.assertTrue(result)

    def testReportMultiDimension(self):
        wh = Warehouse(self.ds_map, config=self.config)
        facts = ['leads', 'sales']
        dimensions = ['partner_name', 'lead_id']
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportRollup(self):
        wh = Warehouse(self.ds_map, config=self.config)
        facts = ['revenue']
        dimensions = ['partner_name', 'campaign_name']
        criteria = [('campaign_name', '!=', 'Campaign 2B')]
        rollup = ROLLUP_TOTALS
        result = wh.report(facts, dimensions=dimensions, criteria=criteria, rollup=rollup)
        revenue = result.rollup_rows().iloc[-1]['revenue']
        revenue_sum = result.non_rollup_rows().sum()['revenue']
        self.assertEqual(revenue, revenue_sum)

    def testReportMultiRollup(self):
        wh = Warehouse(self.ds_map, config=self.config)
        facts = ['revenue']
        dimensions = ['partner_name', 'campaign_name', 'lead_id']
        criteria = [('campaign_name', '!=', 'Campaign 2B')]
        rollup = 3
        result = wh.report(facts, dimensions=dimensions, criteria=criteria, rollup=rollup)
        revenue = result.rollup_rows().iloc[-1]['revenue']
        revenue_sum = result.non_rollup_rows().sum()['revenue']
        self.assertEqual(revenue, revenue_sum)

    def testReportAdHocDimension(self):
        wh = Warehouse(self.ds_map, config=self.config)
        facts = ['leads', 'sales']
        dimensions = ['partner_name',
                      'lead_id',
                      {'formula':'{lead_id} > 3', 'name':'testdim'}]
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

    def testReportAdHocFact(self):
        wh = Warehouse(self.ds_map, config=self.config)
        facts = ['revenue', {'formula': '{revenue} > 3*{lead_id}', 'name': 'testfact'}]
        dimensions = ['partner_name', 'lead_id']
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

@climax.command(parents=[testcli])
@climax.argument('testnames', type=str, nargs='*', help='Names of tests to run')
def main(testnames, debug):
    run_tests(TestSQLAW, testnames, debug)

if __name__ == '__main__':
    main()
