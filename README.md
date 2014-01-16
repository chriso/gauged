## Gauged

[![tests][travis]][travis-builds]

A fast, append-only storage layer for gauges, counters, timers and other numeric data types that change over time.

Features:

- Support for sparse data (unlike the fixed-size RRDtool).
- Cache-aware data structures and algorithms for speed and memory-efficiency - comfortably handle 100+ million data points on a single node.
- Efficient range queries and roll-ups of any size down to the configurable resolution of 1 second.
- Use either `MySQL`, `PostgreSQL` or `SQLite` as a backend.

## Installation

The library can be installed with `easy_install` or `pip`

```bash
$ pip install gauged
```

Python 2.7.x (CPython or PyPy) is required.

## Example

Writing

```python
from gauged import Gauged

gauged = Gauged('mysql://root@localhost/gauged')

with gauged.writer as writer:
    writer.add({ 'requests': 1, 'response_time': 0.45, 'memory_usage': 145.6 })
    writer.add({ 'requests': 1, 'response_time': 0.25, 'memory_usage': 148.3 })
```

Reading

```python
# Count the total number of requests
requests = gauged.aggregate('requests', Gauged.SUM)

# Count the number of requests between 2014/01/01 and 2014/01/08
requests = gauged.aggregate('requests', Gauged.SUM, start=datetime(2014, 1, 1),
    end=datetime(2014, 1, 8))

# Get the 95th percentile response time from the past week
response_time = gauged.aggregate('response_time', Gauged.PERCENTILE,
    percentile=95, start=-Gauged.WEEK)

# Get latest memory usage
memory_usage = gauged.value('memory_usage')
```

Plotting (using [matplotlib][matplotlib])

```python
import pylab
series = gauged.aggregate_series('requests', gauged.SUM, interval=gauged.DAY,
    start=-gauged.WEEK)
pylab.plot(series.dates, series.values, label='Requests per day for the past week')
pylab.show()
```

## Documentation

See the [documentation][documentation] or [technical overview][technical-overview].

## Tests

You can run the test suite using an in-memory driver with `make check-quick`.

To run the full suite, first edit the configuration in `test_drivers.cfg` so that PostgreSQL and Mysql both point to existing (and empty) databases, then run

```bash
$ make check
```

You can run coverage analysis with `make coverage` and run a lint tool `make lint`.

## Benchmarks

Use `python benchmark [OPTIONS]` to run benchmarks using an in-memory database. Your mileage will vary once you add I/O

```bash
$ make build && python benchmark.py --number 1000000 --days 365
Writing to sqlite:// (block_size=86400000, resolution=1000)
Spreading 1M measurements to key "foobar" over 365 days
Wrote 1M measurements in 4.789 seconds (208.8K/s) (rss: 12.3MB)
Gauge data uses 7.6MB (8B per measurement)
min() in 0.022s (read 45.1M measurements/s) (rss: 12.5MB)
max() in 0.022s (read 45.3M measurements/s) (rss: 12.5MB)
sum() in 0.022s (read 44.8M measurements/s) (rss: 12.5MB)
count() in 0.022s (read 45.3M measurements/s) (rss: 12.5MB)
mean() in 0.027s (read 36.9M measurements/s) (rss: 12.5MB)
stddev() in 0.044s (read 22.7M measurements/s) (rss: 23.5MB)
median() in 0.06s (read 16.7M measurements/s) (rss: 27.4MB)
```

## License

GPLv3


[travis]: https://api.travis-ci.org/chriso/gauged.png?branch=master
[travis-builds]: https://travis-ci.org/chriso/gauged
[technical-overview]: https://github.com/chriso/gauged/blob/master/docs/technical-overview.md
[documentation]: https://github.com/chriso/gauged/blob/master/docs/documentation.md
[matplotlib]: http://matplotlib.org/
