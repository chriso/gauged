## Backends

MySQL requires either [mysql-python][mysql-python] or [pymysql][pymysql]

```python
gauged = Gauged('mysql://root@localhost/gauged')
```

PostgreSQL requires [psycopg2][psycopg2]

```python
gauged = Gauged('postgresql://postgres@localhost/gauged')
```

SQLite uses the bindings compiled with your interpreter

```python
gauged = Gauged('sqlite:////tmp/gauged.db')
```

Omit the URL to use a SQLite-based in-memory database

```python
gauged = Gauged()
```

On first run you'll need to create the schema

```python
gauged.sync()
```

## Writing data

The library has no concept of data types when writing. Rather, you store your counter, gauge and timer data in the same way, as a `(key, float_value)` pair, and then use the appropriate method when reading the data back

```python
with gauged.writer as writer:
    writer.add({ 'requests': 1, 'response_time': 0.45, 'memory_usage': 145.6 }, timestamp=1389747759902)
    writer.add({ 'requests': 1, 'response_time': 0.25, 'memory_usage': 148.3 }, timestamp=1389747760456)
```

The timestamp can be omitted (it defaults to now). Note that Gauged is append-only; data must be written in chronological order.

You can also write data to separate namespaces

```python
with gauged.writer as writer:
    writer.add('requests', 1, namespace=1)
    writer.add('requests', 1, namespace=2)
```

For more information, see the [technical overview][technical-overview].

## Reading data

##### gauged.aggregate(key, aggregate, start=None, end=None, namespace=None, percentile=None)

Fetch all values associated with the key during the specified date range (`[start, end)`), and then aggregate them using one of `Gauged.MIN`, `Gauged.MAX`, `Gauged.SUM`, `Gauged.COUNT`, `Gauged.MEAN`, `Gauged.MEDIAN`, `Gauged.STDDEV` or `Gauged.PERCENTILE`.

The `start` and `end` parameters can be either a timestamp in milliseconds, a `datetime` instance or a negative timestamp in milliseconds which is interpreted as relative to now. If omitted, both parameters default to the boundaries of all data that exists in the `namespace`. Note that the `Gauged.SECOND`, `Gauged.MINUTE`, `Gauged.HOUR`, `Gauged.DAY`, `GAUGED.WEEK` and `Gauged.NOW` constants can also be used.

Here's some examples

```python
# Count the total number of requests
requests = gauged.aggregate('requests', Gauged.SUM)

# Count the number of requests between 2014/01/01 and 2014/01/08
requests = gauged.aggregate('requests', Gauged.SUM, start=datetime(2014, 1, 1),
    end=datetime(2014, 1, 8))

# Get the 95th percentile response time from the past week
response_time = gauged.aggregate('response_time', Gauged.PERCENTILE,
    percentile=95, start=-Gauged.WEEK)
```

##### gauged.aggregate_series(key, aggregate, interval=Gauged.DAY, **kwargs)

The time series variant of `aggregate()`. This method takes the same kwargs as `aggregate()` and also accepts an `interval` in milliseconds.

This method returns a `TimeSeries` instance. See [gauged/results/time_series.py][time_series.py] for the result API.

The method is approximately equal to

```python
from gauged.results import TimeSeries
points = []
for timestamp in xrange(start, end, interval):
    aggregate = gauged.aggregate(key, aggregate, start=timestamp, end=timestamp+interval)
    points.append(( timestamp, aggregate ))
result = TimeSeries(points)
```

##### gauged.value(key, timestamp=None, namespace=None)

Read the value of a key at the specified time (defaults to now). Unlike `aggregate()` which looks at all values between the two timestamps, this method starts at the specified timestamp (defaults to now if omitted) and then goes back in time until a measurement for the specified key is found. The config key `max_look_behind` determines how far the method will look before returning `None`.

```python
facebook_likes = gauged.value('facebook_likes', timestamp=datetime(2014, 1, 23))
```

##### gauged.value_series(key, start=None, end=None, interval=Gauged.DAY, namespace=None)

The time series variant of `value()` which reads the `value()` of a key at each `interval` steps in the range `[start, end)`.

##### gauged.keys(prefix=None, limit=None, offset=None, namespace=None)

Get a list of keys, optionally filtered by namespace or prefix.

##### gauged.namespaces()

Get a list of namespaces.

##### gauged.statistics(start=None, end=None, namespace=None)

Get write statistics for the specified namespace during the specified date range. The statistics include number of data points and the number of bytes they consume. See [gauged/results/statistics.py][statistics.py] for the result API.

## Plotting

The data can be plotted easily with [matplotlib][matplotlib]

```python
import pylab

series = gauged.aggregate_series('requests', gauged.SUM, interval=gauged.DAY,
    start=-gauged.WEEK)
pylab.plot(series.dates, series.values, label='Requests per day for the past week')
pylab.show()
```

## Configuration

Configuration should be passed to the constructor

```python
gauged = Gauged('mysql://root@localhost/gauged', **config)
```

Configuration keys

- **key_whitelist** - a list of allowed keys. Default is `None`, i.e. allow all keys.
- **flush_seconds** - whether to periodically flush data when writing, e.g. `10` would cause a flush every 10 seconds. Default is `0` (don't flush).
- **namespace** - the default namespace to read and write to. Defaults to `0`.
- **key_overflow** - what to do when the key size is greater than the backend allows, either `Gauged.ERROR` (default) or `Gauged.IGNORE`.
- **gauge_nan** - what to do when attempting to write a `NaN` value, either `Gauged.ERROR` (default) or `Gauged.IGNORE`.
- **append_only_violation** - what to do when writes aren't done in chronological order, either `Gauged.ERROR` (default), `Gauged.IGNORE`, or `Gauged.REWRITE` which rewrites out-of-order timestamps in order to maintain the chronological constraint.
- **max_look_behind** - how far a `value(key, timestamp)` call will traverse when looking for the nearest measurement before `timestamp`. Default is `Gauged.WEEK`.
- **min_cache_interval** - time series calls with intervals smaller than this will not be cached. Default is `Gauged.HOUR`.
- **max_interval_steps** - throw an error if the number of interval steps is greater than this. Default is `31 * 24`.
- **block_size** - see the [technical overview][technical-overview]. Defaults to `Gauged.DAY`.
- **resolution** - see the [technical overview][technical-overview]. Defaults to `Gauged.SECOND`.


[mysql-python]: http://mysql-python.sourceforge.net/
[pymysql]: https://github.com/PyMySQL/PyMySQL
[psycopg2]: http://initd.org/psycopg/
[technical-overview]: https://github.com/chriso/gauged/blob/master/docs/technical-overview.md
[time_series.py]: https://github.com/chriso/gauged/blob/master/gauged/results/time_series.py
[statistics.py]: https://github.com/chriso/gauged/blob/master/gauged/results/statistics.py
[matplotlib]: http://matplotlib.org/
