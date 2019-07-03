import copy
import contextlib
import cProfile
import pstats
import time

import climax
from toolbox import dbg, st, testcli

from sqlaw.configs import load_warehouse_config
from sqlaw.core import TableTypes
from sqlaw.warehouse import DataSource, AdHocDataSource, Warehouse
from test_utils import TestBase, run_tests, create_adhoc_datatable

TESTDB_CONFIG = load_warehouse_config('testdb_config.json')

@contextlib.contextmanager
def profiled(pattern=None):
    pr = cProfile.Profile()
    pr.enable()
    yield
    pr.disable()
    stats = pstats.Stats(pr)
    stats.sort_stats('cumulative')
    dbg('Top 10 calls by cumulative time:')
    stats.print_stats(10)
    if pattern:
        stats.sort_stats('time')
        dbg('Top 10 %s calls by function time:' % pattern)
        stats.print_stats(pattern, 10)

def init_datasources():
    ds = DataSource('testdb', 'sqlite:///testdb', reflect=True)
    return [ds]

def get_adhoc_ds(size):
    facts = [
        'adhoc_fact1',
        'adhoc_fact2',
        'adhoc_fact3',
        'adhoc_fact4',
    ]
    dimensions = [
        'partner_name',
        'campaign_name',
        'lead_id',
    ]

    column_defs = {
        'partner_name': {
            'fields': ['partner_name'],
            'type': str,
        },
        'campaign_name': {
            'fields': ['campaign_name'],
            'type': str,
        },
        'lead_id': {
            'fields': ['lead_id'],
            'type': int,
        },
        'adhoc_fact1': {
            'fields': ['adhoc_fact1'],
            'type': float,
        },
        'adhoc_fact2': {
            'fields': ['adhoc_fact2'],
            'type': float,
        },
        'adhoc_fact3': {
            'fields': ['adhoc_fact3'],
            'type': float,
        },
        'adhoc_fact4': {
            'fields': ['adhoc_fact4'],
            'type': float,
        },

    }

    start = time.time()
    dt = create_adhoc_datatable('adhoc_table1', TableTypes.FACT, column_defs, ['partner_name'], size)
    adhoc_ds = AdHocDataSource([dt])
    dbg('Created AdHocDataSource in %.3fs' % (time.time() - start))
    return facts, dimensions, adhoc_ds

class TestSQLAWPerformance(TestBase):
    def setUp(self):
        self.datasources = init_datasources()
        self.config = copy.deepcopy(TESTDB_CONFIG)

    def tearDown(self):
        del self.datasources
        self.config = None

    def testPerformance(self):
        wh = Warehouse(self.datasources, config=self.config)
        facts, dimensions, adhoc_ds = get_adhoc_ds(1E5)
        with profiled('sqlaw'):
            result = wh.report(facts,
                               dimensions=dimensions,
                               adhoc_datasources=[adhoc_ds])
        self.assertTrue(result)

    def testPerformanceMultiRollup(self):
        wh = Warehouse(self.datasources, config=self.config)
        facts, dimensions, adhoc_ds = get_adhoc_ds(1E5)
        rollup = 2
        with profiled('sqlaw'):
            result = wh.report(facts,
                               dimensions=dimensions,
                               rollup=rollup,
                               adhoc_datasources=[adhoc_ds])
        self.assertTrue(result)


@climax.command(parents=[testcli])
@climax.argument('testnames', type=str, nargs='*', help='Names of tests to run')
def main(testnames, debug):
    run_tests(TestSQLAWPerformance, testnames, debug)

if __name__ == '__main__':
    main()
