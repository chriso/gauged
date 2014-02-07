'''
Gauged
https://github.com/chriso/gauged (MIT Licensed)
Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
'''

from types import DictType
from ..utilities import table_repr, to_datetime

class TimeSeries(object):
    '''A representation of a time series with a fixed interval'''

    def __init__(self, points):
        '''Initialise the time series. `points` is expected to be either a list of
        tuples where each tuple represents a point (timestamp, value), or a dict where
        the keys are timestamps. Timestamps are expected to be in milliseconds'''
        if type(points) == DictType:
            points = points.items()
        self.points = sorted(points)

    @property
    def timestamps(self):
        '''Get all timestamps from the series'''
        return [ point[0] for point in self.points ]

    @property
    def dates(self):
        '''Get all dates from the time series as `datetime` instances'''
        return [ to_datetime(timestamp) for timestamp, _ in self.points ]

    @property
    def values(self):
        '''Get all values from the time series'''
        return [ point[1] for point in self.points ]

    @property
    def interval(self):
        if len(self.points) <= 1:
            return None
        return self.points[1][0] - self.points[0][0]

    def map(self, fn):
        '''Run a map function across all y points in the series'''
        return TimeSeries([ (x, fn(y)) for x, y in self.points ])

    def __abs__(self):
        return TimeSeries([ (x, abs(y)) for x, y in self.points ])

    def __round__(self, n=0):
        return TimeSeries([ (x, round(y, n)) for x, y in self.points ])

    def round(self, n=0):
        # Manual delegation for v2.x
        return self.__round__(n)

    def __add__(self, operand):
        if not isinstance(operand, TimeSeries):
            return TimeSeries([ ( x, y + operand ) for x, y in self.points ])
        lookup = dict(operand.points)
        return TimeSeries([ ( x, y + lookup[x] ) for x, y in self.points if x in lookup ])

    def __iadd__(self, operand):
        if not isinstance(operand, TimeSeries):
            self.points = [ ( x, y + operand ) for x, y in self.points ]
        else:
            lookup = dict(operand.points)
            self.points = [ ( x, y + lookup[x] ) for x, y in self.points if x in lookup ]
        return self

    def __sub__(self, operand):
        if not isinstance(operand, TimeSeries):
            return TimeSeries([ ( x, y - operand ) for x, y in self.points ])
        lookup = dict(operand.points)
        return TimeSeries([ ( x, y - lookup[x] ) for x, y in self.points if x in lookup ])

    def __isub__(self, operand):
        if not isinstance(operand, TimeSeries):
            self.points = [ ( x, y - operand ) for x, y in self.points ]
        else:
            lookup = dict(operand.points)
            self.points = [ ( x, y - lookup[x] ) for x, y in self.points if x in lookup ]
        return self

    def __mul__(self, operand):
        if not isinstance(operand, TimeSeries):
            return TimeSeries([ ( x, y * operand ) for x, y in self.points ])
        lookup = dict(operand.points)
        return TimeSeries([ ( x, y * lookup[x] ) for x, y in self.points if x in lookup ])

    def __imul__(self, operand):
        if not isinstance(operand, TimeSeries):
            self.points = [ ( x, y * operand ) for x, y in self.points ]
        else:
            lookup = dict(operand.points)
            self.points = [ ( x, y * lookup[x] ) for x, y in self.points if x in lookup ]
        return self

    def __div__(self, operand):
        if not isinstance(operand, TimeSeries):
            return TimeSeries([ ( x, float(y) / operand ) for x, y in self.points ])
        lookup = dict(operand.points)
        return TimeSeries([ ( x, float(y) / lookup[x] ) for x, y in self.points if x in lookup ])

    def __idiv__(self, operand):
        if not isinstance(operand, TimeSeries):
            self.points = [ ( x, float(y) / operand ) for x, y in self.points ]
        else:
            lookup = dict(operand.points)
            self.points = [ ( x, float(y) / lookup[x] ) for x, y in self.points if x in lookup ]
        return self

    def __pow__(self, operand):
        if not isinstance(operand, TimeSeries):
            return TimeSeries([ ( x, y ** operand ) for x, y in self.points ])
        lookup = dict(operand.points)
        return TimeSeries([ ( x, y ** lookup[x] ) for x, y in self.points if x in lookup ])

    def __ipow__(self, operand):
        if not isinstance(operand, TimeSeries):
            self.points = [ ( x, y ** operand ) for x, y in self.points ]
        else:
            lookup = dict(operand.points)
            self.points = [ ( x, y ** lookup[x] ) for x, y in self.points if x in lookup ]
        return self

    def __getitem__(self, x):
        return dict(self.points)[x]

    def __iter__(self):
        return iter(self.points)

    def __len__(self):
        return len(self.points)

    def __repr__(self):
        if not len(self.points):
            return 'TimeSeries([])'
        data = {}
        columns = [ 'Value' ]
        rows = []
        for date, value in self.points:
            date = to_datetime(date).isoformat(' ')
            rows.append(date)
            data[date] = { 'Value': value }
        return table_repr(columns, rows, data)
