import copy
import random
import unittest

from tlbx import st, dbg, random_string

from sqlaw.warehouse import AdHocDataTable

DEFAULT_TEST_DB = 'testdb1'

class TestBase(unittest.TestCase):
    DEBUG = False

    def run(self, result=None):
        if self.DEBUG and (result.failures or result.errors):
            if result.failures:
                dbg(result.failures)
            if result.errors:
                dbg(result.errors)
            st()
        super(TestBase, self).run(result)

def run_tests(testclass, testnames, debug=False):
    testclass.DEBUG = debug
    if testnames:
        suite = unittest.TestSuite()
        for testname in testnames:
            suite.addTest(testclass(testname))
    else:
        suite = unittest.TestLoader().loadTestsFromTestCase(testclass)
    unittest.TextTestRunner(verbosity=2).run(suite)

def create_adhoc_data(column_defs, size):
    data = []

    def get_random_value(coltype):
        if coltype == str:
            return random_string()
        elif coltype == int:
            return random.randint(0, 1E2)
        elif coltype == float:
            return random.random() * 1E2
        else:
            assert False, 'Unsupported column type: %s' % coltype

    for i in range(0, int(size)):
        row = dict()
        for column_name, column_def in column_defs.items():
            row[column_name] = get_random_value(column_def.get('type', str))
        data.append(row)

    return data

def create_adhoc_datatable(name, table_type, column_defs, primary_key, size, parent=None):
    data = create_adhoc_data(column_defs, size)
    column_defs = copy.deepcopy(column_defs)
    for column_name, column_def in column_defs.items():
        if 'type' in column_def:
            # The column schema doesn't allow this column
            del column_def['type']
    dt = AdHocDataTable(name, table_type, primary_key, data, columns=column_defs, parent=parent)
    return dt

def get_testdb_url(dbname=DEFAULT_TEST_DB):
    return 'sqlite:///%s' % dbname
