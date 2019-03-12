import climax
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine, MetaData

from sqlaw.utils import st, json, testcli
from test_utils import TestBase, run_tests

class TestSQLAW(TestBase):
    def setUp(self):
        self.engine = create_engine('sqlite:///testdb')
        self.metadata = MetaData()
        self.metadata.bind = self.engine
        self.metadata.reflect()

    def tearDown(self):
        pass

    def testNothing(self):
        pass

@climax.command(parents=[testcli])
@climax.argument('testnames', type=str, nargs='*', help='Names of tests to run')
def main(testnames, debug):
    run_tests(TestSQLAW, testnames, debug)

if __name__ == '__main__':
    main()
