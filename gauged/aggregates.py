'''
Gauged
https://github.com/chriso/gauged (GPL Licensed)
Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
'''

class Aggregate(object):

    FIRST = 'first'
    LAST = 'last'
    SUM = 'sum'
    MIN = 'min'
    MAX = 'max'
    MEAN = 'mean'
    STDDEV = 'stddev'
    PERCENTILE = 'percentile'
    MEDIAN = 'median'
    COUNT = 'count'

    ALL = set([ FIRST, LAST, SUM, MIN, MAX, MEAN,
        STDDEV, PERCENTILE, MEDIAN, COUNT ])
