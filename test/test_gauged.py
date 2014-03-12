'''
Gauged - https://github.com/chriso/gauged
Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
'''

import datetime, random
from math import ceil, floor, sqrt
from time import time, sleep
from warnings import filterwarnings
from gauged import Gauged, Writer, Config
from gauged.errors import (GaugedKeyOverflowError, GaugedDateRangeError, GaugedAppendOnlyError,
    GaugedIntervalSizeError, GaugedNaNError, GaugedUseAfterFreeError,
    GaugedVersionMismatchError, GaugedBlockSizeMismatch, GaugedSchemaError)
from gauged.structures import SparseMap, FloatArray
from types import LongType, IntType
from .test_case import TestCase

filterwarnings('ignore', category=GaugedBlockSizeMismatch)

class TestGauged(TestCase):
    '''Test higher-level functionality in gauged.py'''

    # Override this with an instantiated driver
    driver = None

    def setUp(self):
        SparseMap.ALLOCATIONS = 0
        FloatArray.ALLOCATIONS = 0
        Writer.ALLOCATIONS = 0
        self.driver.clear_schema()

    def tearDown(self):
        self.assertEqual(SparseMap.ALLOCATIONS, 0)
        self.assertEqual(FloatArray.ALLOCATIONS, 0)
        self.assertEqual(Writer.ALLOCATIONS, 0)

    def test_keys(self):
        gauged = Gauged(self.driver)
        with gauged.writer as writer:
            writer.add('foobar', 1, timestamp=1000)
            writer.add('foobaz', 1, timestamp=1000)
            writer.add('bar', 1, timestamp=1000, namespace=1)
        self.assertListEqual(gauged.keys(), ['foobar', 'foobaz'])
        self.assertListEqual(gauged.keys(prefix='bar'), [])
        self.assertListEqual(gauged.keys(namespace=1), ['bar'])
        self.assertListEqual(gauged.keys(namespace=1, prefix='bar'), ['bar'])
        self.assertListEqual(gauged.keys(limit=1), ['foobar'])
        self.assertListEqual(gauged.keys(limit=1, offset=1), ['foobaz'])

    def test_memory_driver(self):
        gauged = Gauged()
        with gauged.writer as writer:
            writer.add('foo', 1, timestamp=1000)
        self.assertEqual(gauged.value('foo', timestamp=1000), 1)

    def test_add_with_whitelist(self):
        gauged = Gauged(self.driver, key_whitelist=['a'])
        with gauged.writer as writer:
            writer.add({ 'a': 123, 'b': 456 }, timestamp=2000)
        self.assertEqual(gauged.value('a', timestamp=2000), 123)
        self.assertEqual(gauged.value('b', timestamp=2000), None)

    def test_invalid_resolution(self):
        with self.assertRaises(ValueError):
            Gauged(self.driver, resolution=1000, block_size=1500)

    def test_statistics(self):
        gauged = Gauged(self.driver, resolution=1000, block_size=10000)
        with gauged.writer as writer:
            writer.add('bar', 123, timestamp=10000)
            writer.add('bar', 123, timestamp=15000, namespace=1)
            writer.add({ 'foo': 123, 'bar': 456 }, timestamp=20000)
        stats = gauged.statistics()
        self.assertEqual(stats.data_points, 3)
        self.assertEqual(stats.byte_count, 24)
        stats = gauged.statistics(end=20000)
        # note: statistics() rounds to the nearest block boundary
        self.assertEqual(stats.data_points, 1)
        self.assertEqual(stats.byte_count, 8)
        stats = gauged.statistics(namespace=1)
        self.assertEqual(stats.data_points, 1)
        self.assertEqual(stats.byte_count, 8)

    def test_invalid_connection_string(self):
        with self.assertRaises(ValueError):
            Gauged('foobar://localhost')

    def test_parse_query(self):
        gauged = Gauged(self.driver)
        with gauged.writer as writer:
            result = writer.parse_query('foo=bar&baz=&foobar&%3Ckey%3E=value%3D%3')
            self.assertDictEqual(dict(result), {'<key>':'value=%3', 'foo':'bar', 'baz': ''})

    def test_constant_accessibility(self):
        gauged = Gauged(self.driver)
        self.assertTrue(type(Gauged.HOUR) == IntType)
        self.assertTrue(type(gauged.HOUR) == IntType)
        self.assertTrue(type(Gauged.NOW) == LongType)
        self.assertTrue(type(gauged.NOW) == LongType)

    def test_gauge(self):
        gauged = Gauged(self.driver, block_size=50000, gauge_nan=Gauged.IGNORE)
        self.assertEqual(gauged.value('foobar'), None)
        with gauged.writer as writer:
            writer.add('foobar', 200, timestamp=23000)
        self.assertEqual(gauged.value('foobar'), 200)
        self.assertEqual(gauged.value('foobar', timestamp=22000), None)
        with gauged.writer as writer:
            writer.add({ 'foobar': 300, 'invalid': 'nan' }, timestamp=50000)
        self.assertEqual(gauged.value('foobar'), 300)
        self.assertEqual(gauged.value('foobar', 30000), 200)
        timestamp = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=60)
        self.assertEqual(gauged.value('foobar', timestamp), 300)
        with gauged.writer as writer:
            writer.add({ 'foobar': 350 }, timestamp=90000)
            writer.add('foobar', 100, timestamp=120000)
            writer.add('bar', 150, timestamp=130000)
        self.assertItemsEqual(gauged.keys(), [ 'foobar', 'bar' ])
        self.assertEqual(gauged.value('foobar'), 100)
        with gauged.writer as writer:
            writer.add('foobar', 500, timestamp=150000)
        self.assertEqual(gauged.value('foobar'), 500)
        with gauged.writer as writer:
            writer.add('foobar', 1500, timestamp=10000, namespace=1)
        self.assertEqual(gauged.value('foobar', namespace=1), 1500)
        self.assertEqual(gauged.value('foobar'), 500)
        with gauged.writer as writer:
            writer.clear_from(100000)
        self.assertEqual(gauged.value('foobar'), 350)
        with self.assertRaises(GaugedDateRangeError):
            self.assertEqual(gauged.value('foobar', timestamp=-10000000000000), None)

    def test_gauge_nan(self):
        gauged = Gauged(self.driver, block_size=50000)
        with gauged.writer as writer:
            with self.assertRaises(GaugedNaNError):
                writer.add('foobar', 'baz')
        self.assertEqual(gauged.value('foobar'), None)
        gauged = Gauged(self.driver, block_size=50000, gauge_nan=Gauged.IGNORE)
        with gauged.writer as writer:
            writer.add('foobar', 'baz')
        self.assertEqual(gauged.value('foobar'), None)

    def test_gauge_long_key(self):
        gauged = Gauged(self.driver)
        long_key = ''
        for _ in xrange(self.driver.MAX_KEY + 1):
            long_key += 'a'
        with gauged.writer as writer:
            with self.assertRaises(GaugedKeyOverflowError):
                writer.add(long_key, 10)
        gauged = Gauged(self.driver, key_overflow=Gauged.IGNORE)
        with gauged.writer as writer:
            writer.add(long_key, 10)

    def test_aggregate(self):
        gauged = Gauged(self.driver, block_size=10000)
        self.assertEqual(gauged.aggregate('foobar', Gauged.SUM), None)
        with gauged.writer as writer:
            writer.add('foobar', 50, timestamp=10000)
            writer.add('foobar', 150, timestamp=15000)
            writer.add('foobar', 250, timestamp=20000)
            writer.add('foobar', 350, timestamp=40000)
            writer.add('foobar', 70, timestamp=60000)
        self.assertEqual(gauged.aggregate('foobar', Gauged.MIN, start=11000), 70)
        self.assertEqual(gauged.aggregate('foobar', Gauged.MIN, start=11000, end=55000), 150)
        self.assertEqual(gauged.aggregate('foobar', Gauged.SUM), 870)
        self.assertEqual(gauged.aggregate('foobar', Gauged.MIN), 50)
        self.assertEqual(gauged.aggregate('foobar', Gauged.MAX), 350)
        result = gauged.aggregate('foobar', Gauged.STDDEV)
        self.assertAlmostEqual(result, 112.7120224, places=5)
        result = gauged.aggregate('foobar', Gauged.PERCENTILE, percentile=50)
        self.assertEqual(result, 150)
        result = gauged.aggregate('foobar', Gauged.MEDIAN)
        self.assertEqual(result, 150)
        result = gauged.aggregate('foobar', Gauged.PERCENTILE, percentile=90)
        self.assertEqual(result, 310)
        result = gauged.aggregate('foobar', Gauged.COUNT)
        self.assertEqual(result, 5)
        start = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=10)
        end = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=20)
        self.assertEqual(gauged.aggregate('foobar', Gauged.MEAN, start=start, end=end), 100)
        with self.assertRaises(ValueError):
            gauged.aggregate('foobar', Gauged.PERCENTILE, percentile=-1)
        with self.assertRaises(ValueError):
            gauged.aggregate('foobar', Gauged.PERCENTILE, percentile=101)
        with self.assertRaises(ValueError):
            gauged.aggregate('foobar', Gauged.PERCENTILE, percentile=float('nan'))
        with self.assertRaises(ValueError):
            gauged.aggregate('foobar', 'unknown')

    def test_series(self):
        gauged = Gauged(self.driver, block_size=10000)
        self.assertEqual(len(gauged.value_series('foobar', start=0, end=10000).values), 0)
        with gauged.writer as writer:
            writer.add('foobar', 50, timestamp=10000)
            writer.add('foobar', 150, timestamp=15000)
            writer.add('foobar', 250, timestamp=20000)
            writer.add('foobar', 350, timestamp=40000)
            writer.add('foobar', 70, timestamp=60000)
        series = gauged.value_series('foobar', start=0, end=80000, interval=10000)
        self.assertListEqual(series.values, [ 50, 250, 250, 350, 350, 70 ])
        series = gauged.value_series('foobar', interval=10000)
        self.assertListEqual(series.values, [ 50, 250, 250, 350, 350, 70 ])
        series = gauged.value_series('foobar', start=0, end=80000, interval=10000, namespace=1)
        self.assertListEqual(series.values, [])
        with self.assertRaises(GaugedIntervalSizeError):
            gauged.value_series('foobar', interval=0)
        with self.assertRaises(GaugedDateRangeError):
            gauged.value_series('foobar', start=500, end=300)
        with self.assertRaises(GaugedDateRangeError):
            gauged.value_series('foobar', start=-100000000000000)
        with self.assertRaises(GaugedDateRangeError):
            gauged.value_series('foobar', end=-Gauged.NOW-10000)
        self.assertListEqual(gauged.value_series('foobar', start=100000).values, [])

    def test_series_caching(self):
        gauged = Gauged(self.driver, block_size=10000, min_cache_interval=1)
        with gauged.writer as writer:
            writer.add('foobar', 50, timestamp=10000)
            writer.add('foobar', 150, timestamp=15000)
            writer.add('foobar', 250, timestamp=20000)
            writer.add('foobar', 350, timestamp=40000)
            writer.add('foobar', 70, timestamp=60000)
            writer.add('bar', 70, timestamp=60000)
        series = gauged.value_series('foobar', start=0, end=60000, interval=10000)
        self.assertListEqual(series.values, [ 50, 250, 250, 350, 350 ])
        series = gauged.value_series('bar', start=0, end=60000, interval=10000)
        self.assertListEqual(series.values, [])
        gauged = Gauged(self.driver, block_size=10000, overwrite_blocks=True, min_cache_interval=1)
        with gauged.writer as writer:
            writer.add('foobar', 150, timestamp=10000)
            writer.add('foobar', 253, timestamp=15000)
            writer.add('foobar', 351, timestamp=20000)
            writer.add('foobar', 450, timestamp=40000)
            writer.add('foobar', 170, timestamp=60000)
        series = gauged.value_series('foobar', start=0, end=60000, interval=10000)
        self.assertListEqual(series.values, [ 50, 250, 250, 350, 350 ])
        series = gauged.value_series('bar', start=0, end=60000, interval=10000)
        self.assertListEqual(series.values, [])
        series = gauged.value_series('foobar', start=0, end=60000, interval=10000, cache=False)
        self.assertListEqual(series.values, [ 150, 351, 351, 450, 450 ])
        self.driver.remove_cache(0)
        series = gauged.value_series('foobar', start=0, end=60000, interval=10000)
        self.assertListEqual(series.values, [ 150, 351, 351, 450, 450 ])

    def test_series_aggregate(self):
        gauged = Gauged(self.driver, block_size=10000)
        self.assertEqual(len(gauged.aggregate_series('foobar', Gauged.SUM).values), 0)
        with gauged.writer as writer:
            writer.add('foobar', 50, timestamp=10000)
            writer.add('foobar', 150, timestamp=15000)
            writer.add('foobar', 120, timestamp=20000)
            writer.add('foobar', 30, timestamp=25000)
            writer.add('foobar', 40, timestamp=30000)
            writer.add('foobar', 10, timestamp=35000)
            writer.add('bar', 10, timestamp=40000)
        series = gauged.aggregate_series('foobar', Gauged.SUM,
            start=10000, end=40000, interval=10000)
        self.assertListEqual(series.values, [200, 150, 50])
        series = gauged.aggregate_series('foobar', Gauged.SUM,
            start=10000, end=40000, interval=10000, namespace=1)
        self.assertListEqual(series.values, [])
        series = gauged.aggregate_series('foobar', Gauged.SUM,
            start=10000, end=32000, interval=10000)
        self.assertListEqual(series.values, [200, 150, 40])
        series = gauged.aggregate_series('foobar', Gauged.COUNT,
            start=10000, end=50000, interval=10000)
        self.assertListEqual(series.values, [2, 2, 2, 0])
        series = gauged.aggregate_series('foobar', Gauged.MIN,
            start=12000, end=42000, interval=10000)
        self.assertListEqual(series.values, [120, 30, 10])
        series = gauged.aggregate_series('foobar', Gauged.MAX,
            start=12000, end=42000, interval=10000)
        self.assertListEqual(series.values, [150, 40, 10])

    def test_aggregate_series_caching(self):
        gauged = Gauged(self.driver, block_size=10000, min_cache_interval=1)
        series = gauged.aggregate_series('foobar', Gauged.MEAN,
            start=0, end=60000, interval=10000)
        self.assertListEqual(series.values, [])
        with gauged.writer as writer:
            writer.add('bar', 50, timestamp=0)
            writer.add('foobar', 50, timestamp=10000)
            writer.add('foobar', 150, timestamp=15000)
            writer.add('foobar', 250, timestamp=20000)
            writer.add('foobar', 350, timestamp=40000)
            writer.add('foobar', 70, timestamp=60000)
        series = gauged.aggregate_series('foobar', Gauged.MEAN,
            start=0, end=60000, interval=10000)
        self.assertListEqual(series.values, [ None, 100, 250, None, 350, None ])
        gauged = Gauged(self.driver, block_size=10000, overwrite_blocks=True, min_cache_interval=1)
        with gauged.writer as writer:
            writer.add('foobar', 150, timestamp=10000)
            writer.add('foobar', 253, timestamp=15000)
            writer.add('foobar', 351, timestamp=20000)
            writer.add('foobar', 450, timestamp=40000)
            writer.add('foobar', 170, timestamp=60000)
        series = gauged.aggregate_series('foobar', Gauged.MEAN,
            start=0, end=60000, interval=10000)
        self.assertListEqual(series.values, [ None, 100, 250, None, 350, None ])
        series = gauged.aggregate_series('foobar', Gauged.MEAN,
            start=0, end=60000, interval=10000, cache=False)
        self.assertListEqual(series.values, [ None, 201.5, 351, None, 450, None ])
        self.driver.remove_cache(0)
        series = gauged.aggregate_series('foobar', Gauged.MEAN,
            start=0, end=60000, interval=10000)
        self.assertListEqual(series.values, [ None, 201.5, 351, None, 450, None ])
        self.assertListEqual(series.timestamps, [ 0, 10000, 20000, 30000, 40000, 50000 ])

    def test_no_data(self):
        gauged = Gauged(self.driver)
        self.assertEqual(len(gauged.namespaces()), 0)
        self.assertEqual(len(gauged.value_series('foo')), 0)
        self.assertEqual(len(gauged.aggregate_series('foo', Gauged.SUM)), 0)
        self.assertEqual(gauged.value('foo'), None)
        self.assertEqual(gauged.aggregate('foo', Gauged.SUM), None)
        self.assertEqual(len(gauged.keys()), 0)
        stats = gauged.statistics()
        for attr in ['data_points', 'byte_count']:
            self.assertEqual(getattr(stats, attr), 0)

    def test_interval_size_error(self):
        gauged = Gauged(self.driver, resolution=1000, block_size=10000)
        with gauged.writer as writer:
            writer.add('likes', 10)
        with self.assertRaises(GaugedIntervalSizeError):
            gauged.value_series('likes', interval=1)
        with self.assertRaises(GaugedIntervalSizeError):
            gauged.aggregate_series('likes', Gauged.SUM, interval=1)
        with self.assertRaises(GaugedIntervalSizeError):
            gauged.aggregate_series('likes', Gauged.MEAN, interval=1)

    def test_invalid_config_key(self):
        with self.assertRaises(ValueError):
            Gauged(self.driver, foobar=None)

    def test_invalid_context_key(self):
        with self.assertRaises(ValueError):
            Gauged(self.driver, defaults={'foobar':None})

    def test_metadata_on_create(self):
        gauged = Gauged(self.driver)
        metadata = gauged.driver.all_metadata()
        self.assertEqual(metadata['current_version'], Gauged.VERSION)
        self.assertEqual(metadata['initial_version'], Gauged.VERSION)
        self.assertIn('created_at', metadata)

    def test_version_mismatch(self):
        self.driver.set_metadata({ 'current_version': 'foo' })
        gauged = Gauged(self.driver)
        with self.assertRaises(GaugedVersionMismatchError):
            with gauged.writer:
                pass
        gauged.migrate()

    def test_accepting_data_as_string(self):
        gauged = Gauged(self.driver, resolution=1000, block_size=10000,
            key_overflow=Gauged.IGNORE, gauge_nan=Gauged.IGNORE)
        with gauged.writer as writer:
            writer.add('foo=123.456&bar=-15.98&qux=0&invalid=foobar\n', timestamp=20000)
        self.assertAlmostEqual(123.456, gauged.value('foo', timestamp=20000), 5)
        self.assertAlmostEqual(-15.98, gauged.value('bar', timestamp=20000), 5)
        self.assertEqual(gauged.value('qux', timestamp=20000), 0)
        self.assertEqual(gauged.value('invalid', timestamp=20000), None)
        stats = gauged.statistics()
        self.assertEqual(stats.data_points, 3)
        self.assertEqual(stats.byte_count, 24)
        stats = gauged.statistics(end=25000)
        self.assertEqual(stats.data_points, 3)
        self.assertEqual(stats.byte_count, 24)
        gauged = Gauged(self.driver, resolution=1000, block_size=10000,
            key_overflow=Gauged.IGNORE)
        with self.assertRaises(GaugedNaNError):
            with gauged.writer as writer:
                writer.add(u'foo=123.456&bar=-15.98&qux=0&invalid=foobar\n', timestamp=20000)

    def test_context_defaults(self):
        gauged = Gauged(self.driver, resolution=1000, block_size=10000)
        with gauged.writer as writer:
            writer.add('bar', 123, timestamp=10000)
            writer.add('foo', 456, timestamp=20000)
        self.assertListEqual(gauged.keys(), [ 'bar', 'foo' ])
        gauged = Gauged(self.driver, resolution=1000, block_size=10000, defaults={
            'limit': 1
        })
        self.assertListEqual(gauged.keys(), [ 'bar' ])

    def test_look_behind(self):
        gauged = Gauged(self.driver, resolution=1000, block_size=10000, max_look_behind=10000)
        with gauged.writer as writer:
            writer.add('foo', 123, timestamp=10000)
            writer.add('bar', 123, timestamp=40000)
        self.assertEqual(gauged.value('foo', timestamp=10000), 123)
        self.assertEqual(gauged.value('foo', timestamp=20000), 123)
        self.assertEqual(gauged.value('foo', timestamp=30000), None)

    def test_block_slicing(self):
        gauged = Gauged(self.driver, resolution=1000, block_size=10000)
        with gauged.writer as writer:
            writer.add('foo', 100, timestamp=11000)
            writer.add('foo', 200, timestamp=23000)
        self.assertEqual(gauged.aggregate('foo', Gauged.MEAN, start=10000, end=30000), 150)
        self.assertEqual(gauged.aggregate('foo', Gauged.MEAN, start=11000, end=24000), 150)
        self.assertEqual(gauged.aggregate('foo', Gauged.MEAN, start=11000, end=23000), 100)
        self.assertEqual(gauged.aggregate('foo', Gauged.STDDEV, start=11000, end=23000), 0)

    def test_coercing_non_string_keys(self):
        gauged = Gauged(self.driver, resolution=1000, block_size=10000)
        with gauged.writer as writer:
            writer.add(1234, 100, timestamp=10000)
            writer.add(True, 100, timestamp=10000)
            writer.add(u'foo', 100, timestamp=10000)
        self.assertEqual(gauged.value('1234', timestamp=10000), 100)
        self.assertEqual(gauged.value('True', timestamp=10000), 100)
        self.assertEqual(gauged.value('foo', timestamp=10000), 100)

    def test_flush_flushes_last_complete_block(self):
        gauged = Gauged(self.driver, resolution=1000, block_size=10000)
        with gauged.writer as writer:
            writer.add('foo', 1, timestamp=1000)
            writer.add('foo', 2, timestamp=2000)
            writer.add('foo', 3, timestamp=3000)
            self.assertEqual(gauged.value('foo', timestamp=3000), None)
            writer.flush()
            self.assertEqual(gauged.value('foo', timestamp=3000), 2)
        self.assertEqual(gauged.value('foo', timestamp=3000), 3)

    def test_auto_flush(self):
        gauged = Gauged(self.driver, resolution=1000, block_size=10000, flush_seconds=0.001)
        with gauged.writer as writer:
            writer.add('foo', 1, timestamp=1000)
            self.assertEqual(gauged.value('foo', timestamp=2000), None)
            sleep(0.002)
            self.assertEqual(gauged.value('foo', timestamp=2000), None)
            # note: the next write triggers the flush() of all prior writes
            writer.add('foo', 2, timestamp=2000)
            sleep(0.002)
            self.assertEqual(gauged.value('foo', timestamp=2000), 1)
        self.assertEqual(gauged.value('foo', timestamp=2000), 2)

    def test_use_after_free_error(self):
        gauged = Gauged(self.driver, resolution=1000, block_size=10000)
        with gauged.writer as writer:
            pass
        with self.assertRaises(GaugedUseAfterFreeError):
            writer.flush()
        with self.assertRaises(GaugedUseAfterFreeError):
            writer.add('foo', 123, timestamp=1000)
        with self.assertRaises(GaugedUseAfterFreeError):
            list(writer.parse_query('foo=bar'))

    def test_add_debug(self):
        context = dict(called=False)
        gauged = Gauged(self.driver)
        def mock_debug(*_):
            context['called'] = True
        with gauged.writer as writer:
            writer.debug = mock_debug
            writer.add('foo', 123, timestamp=10000, debug=True)
            writer.add('foo=123', timestamp=10000, debug=True)
        self.assertEqual(gauged.value('foo', timestamp=10000), None)
        self.assertTrue(context['called'])

    def test_append_only_violation(self):
        gauged = Gauged(self.driver)
        with gauged.writer as writer:
            writer.add('foo', 123, timestamp=2000)
            with self.assertRaises(GaugedAppendOnlyError):
                writer.add('foo', 456, timestamp=1000)
        self.assertEqual(gauged.value('foo', timestamp=1000), None)
        self.assertEqual(gauged.value('foo', timestamp=2000), 123)

    def test_ignore_append_only_violation(self):
        gauged = Gauged(self.driver, append_only_violation=Gauged.IGNORE)
        with gauged.writer as writer:
            writer.add('foo', 123, timestamp=2000)
            writer.add('foo', 456, timestamp=1000)
        self.assertEqual(gauged.value('foo', timestamp=1000), None)
        self.assertEqual(gauged.value('foo', timestamp=2000), 123)

    def test_rewrite_append_only_violation(self):
        gauged = Gauged(self.driver, append_only_violation=Gauged.REWRITE)
        with gauged.writer as writer:
            writer.add('foo', 123, timestamp=2000)
            writer.add('foo', 456, timestamp=1000)
        self.assertEqual(gauged.value('foo', timestamp=1000), None)
        self.assertEqual(gauged.value('foo', timestamp=2000), 456)

    def test_interim_flushing(self):
        gauged = Gauged(self.driver, resolution=1000, block_size=10000)
        with gauged.writer as writer:
            writer.add('foo', 123, timestamp=1000)
            writer.flush()
            writer.add('bar', 456, timestamp=2000)
            writer.flush()
            writer.add('baz', 789, timestamp=10000)
        self.assertEqual(gauged.value('foo', timestamp=10000), 123)
        self.assertEqual(gauged.value('bar', timestamp=10000), 456)
        self.assertEqual(gauged.value('baz', timestamp=10000), 789)

    def test_resume_from(self):
        gauged = Gauged(self.driver, resolution=1000, block_size=10000)
        with gauged.writer as writer:
            self.assertEqual(writer.resume_from(), 0)
            writer.add('foo', 123, timestamp=10000)
            writer.add('foo', 456, timestamp=15000)
            writer.add('foo', 789, timestamp=20000)
        with gauged.writer as writer:
            self.assertEqual(writer.resume_from(), 21000)
        with gauged.writer as writer:
            writer.add('foo', 123, timestamp=25000)
            writer.add('foo', 456, timestamp=30000)
            writer.add('foo', 789, timestamp=35000)
        with gauged.writer as writer:
            self.assertEqual(writer.resume_from(), 36000)
        gauged = Gauged(self.driver, resolution=1000, block_size=10000, writer_name='foobar')
        with gauged.writer as writer:
            self.assertEqual(writer.resume_from(), 0)
            writer.add('foo', 123, timestamp=10000)
        with gauged.writer as writer:
            self.assertEqual(writer.resume_from(), 11000)
        gauged = Gauged(self.driver, resolution=1000, block_size=10000)
        with gauged.writer as writer:
            self.assertEqual(writer.resume_from(), 36000)

    def test_clear_from(self):
        gauged = Gauged(self.driver, resolution=1000, block_size=10000)
        with gauged.writer as writer:
            writer.add('foo', 1, timestamp=10000)
            writer.add('foo', 2, timestamp=20000)
            writer.add('foo', 3, timestamp=30000)
            writer.add('foo', 4, timestamp=40000, namespace=1)
        with gauged.writer as writer:
            self.assertEqual(writer.resume_from(), 41000)
        self.assertEqual(gauged.value('foo', timestamp=40000), 3)
        self.assertEqual(gauged.value('foo', timestamp=40000, namespace=1), 4)
        with gauged.writer as writer:
            with self.assertRaises(ValueError):
                # note: must be cleared from nearest block boundary (20000)
                writer.clear_from(25000)
            writer.clear_from(20000)
        self.assertEqual(gauged.value('foo', timestamp=40000), 1)
        self.assertEqual(gauged.value('foo', timestamp=40000, namespace=1), None)
        with gauged.writer as writer:
            self.assertEqual(writer.resume_from(), 21000)
            writer.add('foo', 5, timestamp=30000)
            writer.add('foo', 6, timestamp=30000, namespace=1)
        self.assertEqual(gauged.value('foo', timestamp=40000), 5)
        self.assertEqual(gauged.value('foo', timestamp=40000, namespace=1), 6)

    def test_gauged_init_store(self):
        gauged = Gauged('sqlite+foo://')
        self.assertEqual(len(gauged.metadata()), 0)
        with self.assertRaises(GaugedSchemaError):
            gauged.keys()

    def test_config_instance(self):
        config = Config(append_only_violation=Gauged.REWRITE,
            resolution=1000, block_size=10000)
        gauged = Gauged(self.driver, config=config)
        with gauged.writer as writer:
            writer.add('foo', 1, timestamp=2000)
            writer.add('foo', 2, timestamp=1000)
        self.assertEqual(gauged.value('foo', timestamp=2000), 2)

    def test_relative_dates(self):
        now = long(time() * 1000)
        gauged = Gauged(self.driver, defaults={'start':-4*Gauged.DAY})
        with gauged.writer as writer:
            writer.add('foo', 1, timestamp=now-6*Gauged.DAY)
            writer.add('foo', 2, timestamp=now-5*Gauged.DAY)
            writer.add('foo', 3, timestamp=now-3*Gauged.DAY)
            writer.add('foo', 4, timestamp=now-2*Gauged.DAY)
            writer.add('foo', 5, timestamp=now-Gauged.DAY)
        self.assertEqual(gauged.value('foo'), 5)
        self.assertEqual(gauged.value('foo', timestamp=-1.5*Gauged.DAY), 4)

    def test_fuzzy(self, decimal_places=4, max_values=3):
        def random_values(n, minimum, maximum, decimals):
            return [ round(random.random() * (maximum - minimum) + minimum, decimals) \
                for _ in xrange(n) ]
        def percentile(values, percentile):
            if not len(values):
                return float('nan')
            values = sorted(values)
            rank = float(len(values) - 1) * percentile / 100
            nearest_rank = int(floor(rank))
            result = values[nearest_rank]
            if (ceil(rank) != nearest_rank):
                result += (rank - nearest_rank) * (values[nearest_rank + 1] - result)
            return result
        def stddev(values):
            total = len(values)
            mean = float(sum(values)) / total
            sum_of_squares = sum((elem - mean) ** 2 for elem in values)
            return sqrt(float(sum_of_squares) / total)
        for resolution in (100, 500, 1000):
            for n in xrange(1, max_values):
                for end in (1000, 10000):
                    gauged = Gauged(self.driver, block_size=1000, resolution=resolution)
                    gauged.driver.clear_schema()
                    values = random_values(n, -100, 100, 2)
                    with gauged.writer as writer:
                        timestamps = sorted(random_values(n, 0, end, 0))
                        for value, timestamp in zip(values, timestamps):
                            writer.add('foo', value, timestamp=int(timestamp))
                    self.assertAlmostEqual(sum(values), gauged.aggregate('foo', Gauged.SUM),
                        places=decimal_places)
                    self.assertAlmostEqual(min(values), gauged.aggregate('foo', Gauged.MIN),
                        places=decimal_places)
                    self.assertAlmostEqual(max(values), gauged.aggregate('foo', Gauged.MAX),
                        places=decimal_places)
                    self.assertAlmostEqual(len(values), gauged.aggregate('foo', Gauged.COUNT),
                        places=decimal_places)
                    mean = float(sum(values)) / len(values)
                    self.assertAlmostEqual(mean, gauged.aggregate('foo', Gauged.MEAN),
                        places=decimal_places)
                    self.assertAlmostEqual(stddev(values), gauged.aggregate('foo', Gauged.STDDEV),
                        places=decimal_places)
                    self.assertAlmostEqual(percentile(values, 50), gauged.aggregate('foo', Gauged.MEDIAN),
                        places=decimal_places)
                    self.assertAlmostEqual(percentile(values, 98), gauged.aggregate('foo',
                        Gauged.PERCENTILE, percentile=98), places=decimal_places)
