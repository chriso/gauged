'''
Gauged
https://github.com/chriso/gauged (MIT Licensed)
Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
'''

class Statistics(object):
    '''A wrapper for the result of a statistics() operation'''

    __slots__ = [ 'namespace', 'start', 'end', 'data_points', 'byte_count' ]

    def __init__(self, namespace=0, start=0, end=0, data_points=0, byte_count=0):
        self.namespace = namespace
        self.start = start
        self.end = end
        self.data_points = data_points
        self.byte_count = byte_count

    def __repr__(self):
        instance = 'Statistics(namespace=%s, start=%s, end=%s, '
        instance += 'data_points=%s, byte_count=%s)'
        return instance % (self.namespace, self.start, self.end,
            self.data_points, self.byte_count)
