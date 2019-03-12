import unittest

from sqlaw.utils import st, dbg

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
