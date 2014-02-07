'''
Gauged
https://github.com/chriso/gauged (MIT Licensed)
Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
'''

from types import StringType
from time import time
from warnings import warn
from .writer import Writer
from .context import Context
from .drivers import get_driver, SQLiteDriver
from .utilities import Time
from .aggregates import Aggregate
from .config import Config
from .errors import (GaugedVersionMismatchError, GaugedBlockSizeMismatch,
    GaugedSchemaError)
from .version import __version__

class Gauged(object):
    '''Read and write gauge data'''

    VERSION = __version__

    SECOND = Time.SECOND
    MINUTE = Time.MINUTE
    HOUR = Time.HOUR
    DAY = Time.DAY
    WEEK = Time.WEEK
    NOW = Time()

    ERROR = Writer.ERROR
    IGNORE = Writer.IGNORE
    REWRITE = Writer.REWRITE

    AGGREGATES = Aggregate.ALL
    MIN = Aggregate.MIN
    MAX = Aggregate.MAX
    SUM = Aggregate.SUM
    MEAN = Aggregate.MEAN
    STDDEV = Aggregate.STDDEV
    PERCENTILE = Aggregate.PERCENTILE
    MEDIAN = Aggregate.MEDIAN
    COUNT = Aggregate.COUNT

    def __init__(self, driver=None, config=None, **kwargs):
        in_memory = driver is None
        if in_memory:
            driver = SQLiteDriver.MEMORY
        if type(driver) == StringType:
            driver = get_driver(driver)
        if config is None:
            config = Config()
        if len(kwargs):
            config.update(**kwargs)
        self.driver = driver
        self.config = config
        self.valid_schema = False
        if in_memory:
            self.sync()

    @property
    def writer(self):
        '''Create a new writer instance'''
        self.check_schema()
        return Writer(self.driver, self.config)

    def value(self, key, timestamp=None, namespace=None):
        '''Get the value of a gauge at the specified time'''
        return self.make_context(key=key, end=timestamp,
            namespace=namespace).value()

    def aggregate(self, key, aggregate, start=None, end=None,
            namespace=None, percentile=None):
        '''Get an aggregate of all gauge data stored in the specified date range'''
        return self.make_context(key=key, aggregate=aggregate, start=start,
            end=end, namespace=namespace,
            percentile=percentile).aggregate()

    def value_series(self, key, start=None, end=None, interval=None,
            namespace=None, cache=None):
        '''Get a time series of gauge values'''
        return self.make_context(key=key, start=start, end=end,
            interval=interval, namespace=namespace, cache=cache).value_series()

    def aggregate_series(self, key, aggregate, start=None, end=None,
            interval=None, namespace=None, cache=None, percentile=None):
        '''Get a time series of gauge aggregates'''
        return self.make_context(key=key, aggregate=aggregate, start=start,
            end=end, interval=interval, namespace=namespace, cache=cache,
            percentile=percentile).aggregate_series()

    def keys(self, prefix=None, limit=None, offset=None, namespace=None):
        '''Get gauge keys'''
        return self.make_context(prefix=prefix, limit=limit, offset=offset,
            namespace=namespace).keys()

    def namespaces(self):
        '''Get a list of namespaces'''
        return self.driver.get_namespaces()

    def statistics(self, start=None, end=None, namespace=None):
        '''Get write statistics for the specified namespace and date range'''
        return self.make_context(start=start, end=end,
            namespace=namespace).statistics()

    def sync(self):
        '''Create the necessary schema'''
        self.driver.create_schema()
        self.driver.set_metadata({
            'current_version': Gauged.VERSION,
            'initial_version': Gauged.VERSION,
            'block_size': self.config.block_size,
            'resolution': self.config.resolution,
            'created_at': long(time() * 1000)
        }, replace=False)

    def metadata(self):
        '''Get gauged metadata'''
        try:
            metadata = self.driver.all_metadata()
        except: # pylint: disable=W0702
            metadata = {}
        return metadata

    def migrate(self):
        '''Migrate an old Gauged schema to the current version. This is
        just a placeholder for now'''
        self.driver.set_metadata({ 'current_version': Gauged.VERSION })

    def make_context(self, **kwargs):
        '''Create a new context for reading data'''
        self.check_schema()
        return Context(self.driver, self.config, **kwargs)

    def check_schema(self):
        '''Check the schema exists and matches configuration'''
        if self.valid_schema:
            return
        config = self.config
        metadata = self.metadata()
        if 'current_version' not in metadata:
            raise GaugedSchemaError('Gauged schema not found, try a gauged.sync()')
        if metadata['current_version'] != Gauged.VERSION:
            msg = 'The schema is version %s while this Gauged is version %s. '
            msg += 'Try upgrading Gauged and/or running gauged.migrate()'
            msg = msg % (metadata['current_version'], Gauged.VERSION)
            raise GaugedVersionMismatchError(msg)
        expected_block_size = '%s/%s' % (config.block_size, config.resolution)
        block_size = '%s/%s' % (metadata['block_size'], metadata['resolution'])
        if block_size != expected_block_size:
            msg = 'Expected %s and got %s' % (expected_block_size, block_size)
            warn(msg, GaugedBlockSizeMismatch)
        self.valid_schema = True
