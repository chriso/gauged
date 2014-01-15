'''
Gauged - https://github.com/chriso/gauged
Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
'''

import gauged, unittest, sys, gc
from ConfigParser import ConfigParser
from test import (TestGauged, TestStructures, TestLRU, TestDriver,
    TestDSN, TestResult)

# Get the list of test drivers
config = ConfigParser()
config.read('test_drivers.cfg')
test_drivers = config.items('drivers')

# Parse command line options
argv = set(sys.argv)
quick_tests = '--quick' in argv    # Use only the first driver (in-memory SQLite)
raise_on_error = '--raise' in argv # Stop and dump a trace when a test driver fails
run_forever = '--forever' in argv  # Run the tests forever (to check for leaks)
drivers_only = '--drivers' in argv # Run driver tests only
verbose = '--verbose' in argv      # Increase test runner verbosity

if quick_tests:
    test_drivers = test_drivers[:1]

def run_tests():

    suite = unittest.TestSuite()

    test_class = driver = None

    # Test each driver in gauged/drivers. We need to dynamically subclass
    # the TestDriver class here - the unittest module doesn't allow us to
    # use __init__ to pass in driver arguments or a driver instance
    for driver, dsn in test_drivers:
        try:
            gauged_instance = gauged.Gauged(dsn)
            # Empty the data store
            gauged_instance.driver.drop_schema()
            gauged_instance.sync()
            gauged_instance.driver.create_schema()
            # Test the driver class
            test_class = type('Test%s' % driver, (TestDriver,), {})
            test_class.driver = gauged_instance.driver
            suite.addTest(unittest.makeSuite(test_class))
            # Test gauged/gauged.py using the driver
            if not drivers_only:
                test_class = type('TestGaugedWith%s' % driver, (TestGauged,), {})
                test_class.driver = gauged_instance.driver
                suite.addTest(unittest.makeSuite(test_class))
        except Exception as e:
            print 'Skipping %s tests (%s). Check test_driver.cfg' % (driver[:-6], str(e).rstrip())
            if raise_on_error:
                raise

    if test_class is None:
        msg = 'No drivers available for unit tests, check configuration in test_driver.cfg'
        raise RuntimeWarning(msg)

    # Test the remaining classes
    if not drivers_only:
        suite.addTest(unittest.makeSuite(TestStructures))
        suite.addTest(unittest.makeSuite(TestLRU))
        suite.addTest(unittest.makeSuite(TestDSN))
        suite.addTest(unittest.makeSuite(TestResult))

    # Setup the test runner
    verbosity = 2 if verbose else 1
    if run_forever:
        verbosity = 0
    test_runner = unittest.TextTestRunner(verbosity=verbosity)

    # Run the tests
    while True:
        result = test_runner.run(suite)
        if result.errors or result.failures:
            exit(1)
        if not run_forever:
            break
        gc.collect()

if __name__ == '__main__':
    run_tests()
