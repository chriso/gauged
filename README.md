## Gauged

[![tests][travis]][travis-builds]

A fast, append-only storage layer for gauges, counters, timers and other numeric data types that change over time.

Features:

- Comfortably handle billions of data points on a single node.
- Support for sparse data (unlike the fixed-size RRDtool).
- Cache-oblivious data structures and algorithms for speed and memory-efficiency.
- Efficient range queries and roll-ups of any size down to the configurable resolution of 1 second.
- Use either **MySQL**, **PostgreSQL** or **SQLite** as a backend.
- Runs on Mac, Linux & Windows

## Installation

The library can be installed with **easy_install** or **pip**

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
    writer.add({ 'requests': 1, 'response_time': 0.25, 'cpu_usage': 148.3, 'api_requests': 3 })
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

Use `make build` followed by `python benchmark [OPTIONS]` to run benchmarks using a SQLite-based in-memory database. Your mileage will vary once you add I/O.

**python benchmark.py --number 1000000 --days 365**

```
Writing to sqlite:// (block_size=86400000, resolution=1000)
Spreading 1M measurements to key "foobar" over 365 days
Wrote 1M measurements in 5.388 seconds (185.6K/s) (rss: 12.7MB)
Gauge data uses 7.6MB (8B per measurement)
min() in 0.024s (read 41.9M measurements/s) (rss: 12.8MB)
max() in 0.023s (read 43.4M measurements/s) (rss: 12.8MB)
sum() in 0.023s (read 42.7M measurements/s) (rss: 12.8MB)
count() in 0.024s (read 42.1M measurements/s) (rss: 12.8MB)
mean() in 0.028s (read 36.1M measurements/s) (rss: 12.8MB)
stddev() in 0.05s (read 20.1M measurements/s) (rss: 12.8MB)
median() in 0.06s (read 16.8M measurements/s) (rss: 27.7MB)
```

**python benchmark.py --number 100000000 --days 365**

```
Writing to sqlite:// (block_size=86400000, resolution=1000)
Spreading 100M measurements to key "foobar" over 365 days
Wrote 100M measurements in 405.925 seconds (246.4K/s) (rss: 21.7MB)
Gauge data uses 502.2MB (5.26601144B per measurement)
min() in 0.818s (read 122.3M measurements/s) (rss: 21.7MB)
max() in 0.79s (read 126.6M measurements/s) (rss: 21.7MB)
sum() in 0.785s (read 127.4M measurements/s) (rss: 21.7MB)
count() in 0.766s (read 130.5M measurements/s) (rss: 21.7MB)
mean() in 0.891s (read 112.3M measurements/s) (rss: 21.7MB)
stddev() in 1.697s (read 58.9M measurements/s) (rss: 21.7MB)
median() in 3.547s (read 28.2M measurements/s) (rss: 1007.9MB)
```

## License (MIT)

```
Copyright (c) 2014 Chris O'Hara <cohara87@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```

[travis]: https://api.travis-ci.org/chriso/gauged.png?branch=master
[travis-builds]: https://travis-ci.org/chriso/gauged
[technical-overview]: https://github.com/chriso/gauged/blob/master/docs/technical-overview.md
[documentation]: https://github.com/chriso/gauged/blob/master/docs/documentation.md
[matplotlib]: http://matplotlib.org/
