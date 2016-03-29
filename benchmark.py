"""
Gauged - https://github.com/chriso/gauged
Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
"""

from random import random
from math import floor, ceil
from time import time
from calendar import timegm
from datetime import datetime, timedelta
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from gauged import Gauged


def abbreviate(suffixes, cutoff):
    def abbreviate_(number, decimals=1):
        position = 0
        while position < len(suffixes) and abs(number) >= cutoff:
            number = round(number / cutoff, decimals)
            position += 1
        if floor(number) == ceil(number):
            number = int(number)
        return str(number) + suffixes[position]
    return abbreviate_

abbreviate_number = abbreviate(['', 'K', 'M', 'B'], 1000)
abbreviate_bytes = abbreviate(['B', 'KB', 'MB', 'GB', 'TB'], 1024)

# Parse CLI options
benchmark = ArgumentParser(usage='%(prog)s [OPTIONS]',
                           formatter_class=ArgumentDefaultsHelpFormatter)
benchmark.add_argument(
    '-n', '--number', type=int, default=1000000,
    help='How many measurements to store')
benchmark.add_argument(
    '-t', '--days', type=int, default=365,
    help='How many days to spread the measurements over')
benchmark.add_argument(
    '-d', '--driver', default='sqlite://',
    help='Where to store the data (defaults to SQLite in-memory)')
benchmark.add_argument(
    '-b', '--block-size', type=int, default=Gauged.DAY,
    help='The block size to use')
benchmark.add_argument(
    '-r', '--resolution', type=int, default=Gauged.SECOND,
    help='The resolution to use')
options = vars(benchmark.parse_args())

# Setup the Gauged instance
gauged = Gauged(options['driver'], block_size=options['block_size'],
                resolution=options['resolution'], key_overflow=Gauged.IGNORE,
                gauge_nan=Gauged.IGNORE)
gauged.sync()

print 'Writing to %s (block_size=%s, resolution=%s)' % \
    (options['driver'], options['block_size'], options['resolution'])

# Get the start and end timestamp
end = datetime.now()
start = end - timedelta(days=options['days'])
start_timestamp = timegm(start.timetuple())
end_timestamp = timegm(end.timetuple())

number = abbreviate_number(options['number'])

print 'Spreading %s measurements to key "foobar" over %s days' % \
    (number, options['days'])

# Benchmark writes
measurements = options['number']
span = end_timestamp - start_timestamp
start = time()
with gauged.writer as writer:
    data = ['foobar', 0]
    gauges = [data]
    add = writer.add
    for timestamp in xrange(start_timestamp, end_timestamp,
                            span // measurements):
        data[1] = random()
        add(gauges, timestamp=timestamp*1000)
elapsed = time() - start

print 'Wrote %s measurements in %s seconds (%s/s)' % \
    (number, round(elapsed, 3), abbreviate_number(measurements / elapsed))

statistics = gauged.statistics()
byte_count = statistics.byte_count
print 'Gauge data uses %s (%s per measurement)' % \
    (abbreviate_bytes(byte_count),
     abbreviate_bytes(byte_count / float(measurements)))

# Read benchmarks
for aggregate in ('min', 'max', 'sum', 'count', 'mean', 'stddev', 'median'):
    start = time()
    gauged.aggregate('foobar', aggregate)
    elapsed = time() - start
    print '%s() in %ss (read %s measurements/s)' % \
        (aggregate, round(elapsed, 3),
         abbreviate_number(measurements / elapsed))
