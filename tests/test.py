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
        result = wh.report(facts, dimensions=dimensions)
        self.assertTrue(result)

@climax.command(parents=[testcli])
@climax.argument('testnames', type=str, nargs='*', help='Names of tests to run')
def main(testnames, debug):
    run_tests(TestSQLAW, testnames, debug)

if __name__ == '__main__':
    main()
