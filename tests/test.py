import climax
from sqlalchemy import create_engine, MetaData

from sqlaw.configs import load_config
from sqlaw.utils import dbg, st, testcli
from sqlaw.warehouse import DataSourceMap, Warehouse
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
        self.config = TESTDB_CONFIG.copy()

    def tearDown(self):
        del self.ds_map
        self.config = None

    def testWarehouseInit(self):
        wh = Warehouse(self.ds_map, config=self.config)
        self.assertTrue(wh.dimension_tables)
        self.assertTrue(wh.dimensions)

    def testTableConfigOverride(self):
        self.config['datasources']['testdb']['tables']['sales']['type'] = 'dimension'
        wh = Warehouse(self.ds_map, config=self.config)
        self.assertIn('sales', wh.dimension_tables['testdb'])

    def testColumnConfigOverride(self):
        table_config = self.config['datasources']['testdb']['tables']['sales']
        table_config['columns'] = {'revenue': {'type':'dimension'}}
        wh = Warehouse(self.ds_map, config=self.config)
        self.assertIn('revenue', wh.dimensions)

    def testReport(self):
        wh = Warehouse(self.ds_map, config=self.config)
        facts = ['revenue', 'sales.quantity']
        dimensions = ['partner_name', 'campaign_name']
        criteria = [('campaign_name', '!=', 'Campaign 2B')]
        row_filters = [('revenue', '>', 11)]
        rollup = True
        result = wh.report(facts, dimensions=dimensions, criteria=criteria, row_filters=row_filters, rollup=rollup)
        self.assertTrue(result)

    def testReportNoDimensions(self):
        wh = Warehouse(self.ds_map, config=self.config)
        facts = ['revenue', 'sales.quantity']
        dimensions = None
        criteria = [('campaign_name', '!=', 'Campaign 2B')]
        result = wh.report(facts, dimensions=dimensions, criteria=criteria)
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
        dimensions = ['partner_name']
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

@climax.command(parents=[testcli])
@climax.argument('testnames', type=str, nargs='*', help='Names of tests to run')
def main(testnames, debug):
    run_tests(TestSQLAW, testnames, debug)

if __name__ == '__main__':
    main()
