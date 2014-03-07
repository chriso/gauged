'''
Gauged - https://github.com/chriso/gauged
Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
'''

import re
from datetime import datetime
from gauged.results import Statistics, TimeSeries
from .test_case import TestCase

class TestResult(TestCase):
    '''Test result wrappers'''

    def test_statistics_repr(self):
        stat = Statistics(1, 2, 3, 4, 5)
        self.assertEqual(str(stat), 'Statistics(namespace=1, start=2, end=3, ' \
            'data_points=4, byte_count=5)')

    def test_tuple_list_init(self):
        series = TimeSeries([ (1, 2), (3, 4), (5, 6) ])
        self.assertListEqual(series.timestamps, [1, 3, 5])
        self.assertListEqual(series.values, [2, 4, 6])
        self.assertEqual(len(series), 3)

    def test_dict_init(self):
        series = TimeSeries({ 1: 2, 3: 4, 5: 6 })
        self.assertListEqual(series.timestamps, [1, 3, 5])
        self.assertListEqual(series.values, [2, 4, 6])
        self.assertEqual(len(series), 3)

    def test_accessors(self):
        points = [ (1234000, 54), (5678000, 100) ]
        series = TimeSeries(points)
        dates = series.dates
        for date in dates:
            self.assertTrue(isinstance(date, datetime))
        self.assertListEqual(series.dates, [datetime.fromtimestamp(1234),
            datetime.fromtimestamp(5678)])
        self.assertListEqual(series.timestamps, [1234000, 5678000])
        self.assertListEqual(series.values, [54, 100])

    def test_initial_sort(self):
        points = [ (3, 54), (2, 100), (4, 32) ]
        series = TimeSeries(points)
        self.assertListEqual(series.timestamps, [2, 3, 4])
        self.assertListEqual(series.values, [100, 54, 32])

    def test_interval(self):
        series = TimeSeries([])
        self.assertEqual(series.interval, None)
        series = TimeSeries([ (1, 2) ])
        self.assertEqual(series.interval, None)
        series = TimeSeries([ (1, 2), (3, 4) ])
        self.assertEqual(series.interval, 2)

    def test_map(self):
        series = TimeSeries([ (1, 2), (3, 4), (5, 6) ])
        double = series.map(lambda y: y * 2)
        self.assertTrue(isinstance(double, TimeSeries))
        self.assertListEqual([ (1, 4), (3, 8), (5, 12) ], double.points)

    def test_indexing(self):
        series = TimeSeries([ (1, 3), (2, 3), (3, 3) ])
        self.assertEqual(series[1], 3)
        self.assertEqual(series[2], 3)
        with self.assertRaises(KeyError):
            series[4]

    def test_iteration(self):
        points = [ (1, 2), (3, 4), (5, 6) ]
        series = TimeSeries(points)
        self.assertListEqual([ s for s in series ], points)

    def test_map_return_type(self):
        series = TimeSeries([ (1, 2), (3, 4), (5, 6) ])
        double = series.map(lambda y: y * 2)
        self.assertTrue(isinstance(double, TimeSeries))
        self.assertListEqual([ (1, 4), (3, 8), (5, 12) ], double.points)

    def test_add(self):
        a = TimeSeries([ (1, 3), (2, 3), (3, 3) ])
        b = TimeSeries([ (0, 1), (1, 1), (2, 1), (3, 1), (4, 1) ])
        c = a + b
        self.assertTrue(isinstance(c, TimeSeries))
        self.assertListEqual(c.points, [ (1, 4), (2, 4), (3, 4) ])
        c = c + 5
        self.assertTrue(isinstance(c, TimeSeries))
        self.assertListEqual(c.points, [ (1, 9), (2, 9), (3, 9) ])

    def test_add_update(self):
        a = TimeSeries([ (1, 3), (2, 3), (3, 3) ])
        b = TimeSeries([ (0, 1), (1, 1), (2, 1), (3, 1), (4, 1) ])
        a += b
        self.assertListEqual(a.points, [ (1, 4), (2, 4), (3, 4) ])
        a += 5
        self.assertListEqual(a.points, [ (1, 9), (2, 9), (3, 9) ])

    def test_sub(self):
        a = TimeSeries([ (1, 3), (2, 3), (3, 3) ])
        b = TimeSeries([ (0, 1), (1, 1), (2, 1), (3, 1), (4, 1) ])
        c = a - b
        self.assertTrue(isinstance(c, TimeSeries))
        self.assertListEqual(c.points, [ (1, 2), (2, 2), (3, 2) ])
        c = c - 1
        self.assertTrue(isinstance(c, TimeSeries))
        self.assertListEqual(c.points, [ (1, 1), (2, 1), (3, 1) ])

    def test_sub_update(self):
        a = TimeSeries([ (1, 3), (2, 3), (3, 3) ])
        b = TimeSeries([ (0, 1), (1, 1), (2, 1), (3, 1), (4, 1) ])
        a -= b
        self.assertListEqual(a.points, [ (1, 2), (2, 2), (3, 2) ])
        a -= 1
        self.assertListEqual(a.points, [ (1, 1), (2, 1), (3, 1) ])

    def test_mul(self):
        a = TimeSeries([ (1, 3), (2, 3), (3, 3) ])
        b = TimeSeries([ (0, 2), (1, 3), (2, 2), (3, 2), (4, 1) ])
        c = a * b
        self.assertTrue(isinstance(c, TimeSeries))
        self.assertListEqual(c.points, [ (1, 9), (2, 6), (3, 6) ])
        c = c * 2
        self.assertTrue(isinstance(c, TimeSeries))
        self.assertListEqual(c.points, [ (1, 18), (2, 12), (3, 12) ])

    def test_mul_update(self):
        a = TimeSeries([ (1, 3), (2, 3), (3, 3) ])
        b = TimeSeries([ (0, 2), (1, 3), (2, 2), (3, 2), (4, 1) ])
        a *= b
        self.assertListEqual(a.points, [ (1, 9), (2, 6), (3, 6) ])
        a *= 2
        self.assertListEqual(a.points, [ (1, 18), (2, 12), (3, 12) ])

    def test_div(self):
        a = TimeSeries([ (1, 3), (2, 4), (3, 3) ])
        b = TimeSeries([ (0, 2), (1, 3), (2, 2), (3, 2), (4, 1) ])
        c = a / b
        self.assertTrue(isinstance(c, TimeSeries))
        self.assertListEqual(c.points, [ (1, 1), (2, 2), (3, 1.5) ])
        c = c / 2
        self.assertTrue(isinstance(c, TimeSeries))
        self.assertListEqual(c.points, [ (1, 0.5), (2, 1), (3, 0.75) ])

    def test_div_update(self):
        a = TimeSeries([ (1, 3), (2, 4), (3, 3) ])
        b = TimeSeries([ (0, 2), (1, 3), (2, 2), (3, 2), (4, 1) ])
        a /= b
        self.assertListEqual(a.points, [ (1, 1), (2, 2), (3, 1.5) ])
        a /= 0.5
        self.assertListEqual(a.points, [ (1, 2), (2, 4), (3, 3) ])

    def test_pow(self):
        a = TimeSeries([ (1, 3), (2, 3), (3, 3) ])
        b = TimeSeries([ (0, 2), (1, 3), (2, 2), (3, 1), (4, 1) ])
        c = a ** b
        self.assertTrue(isinstance(c, TimeSeries))
        self.assertListEqual(c.points, [ (1, 27), (2, 9), (3, 3) ])
        c = c ** 2
        self.assertTrue(isinstance(c, TimeSeries))
        self.assertListEqual(c.points, [ (1, 729), (2, 81), (3, 9) ])

    def test_abs(self):
        a = TimeSeries([ (1, -3), (2, 3.3), (3, -5) ])
        a = abs(a)
        self.assertTrue(isinstance(a, TimeSeries))
        self.assertListEqual(a.values, [ 3, 3.3, 5 ])

    def test_round(self):
        a = TimeSeries([ (1, -0.3), (2, 3.3), (3, 1.1) ])
        a = a.round()
        self.assertTrue(isinstance(a, TimeSeries))
        self.assertListEqual(a.values, [ 0, 3, 1 ])

    def test_pow_update(self):
        a = TimeSeries([ (1, 3), (2, 3), (3, 3) ])
        b = TimeSeries([ (0, 2), (1, 3), (2, 2), (3, 1), (4, 1) ])
        a **= b
        self.assertListEqual(a.points, [ (1, 27), (2, 9), (3, 3) ])
        a **= 2
        self.assertListEqual(a.points, [ (1, 729), (2, 81), (3, 9) ])

    def test_time_series_repr(self):
        a = TimeSeries([ (10000, 500), (20000, 1234), (30000, 12345678) ])
        lines = repr(a).split('\n')
        self.assertEqual(lines[0], '                        Value')
        self.assertTrue(re.match(r'^19(?:69|70)-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:10       500$', lines[1]))
        self.assertTrue(re.match(r'^19(?:69|70)-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:20      1234$', lines[2]))
        self.assertTrue(re.match(r'^19(?:69|70)-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:30  12345678$', lines[3]))
        self.assertEqual(str(TimeSeries([])), 'TimeSeries([])')
