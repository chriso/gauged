'''
Gauged
https://github.com/chriso/gauged (GPL Licensed)
Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
'''

from hashlib import sha1
from calendar import timegm
from datetime import date
from time import time
from .structures import SparseMap
from .aggregates import Aggregate
from .utilities import to_bytes
from .results import Statistics, TimeSeries
from .errors import GaugedDateRangeError, GaugedIntervalSizeError

class Context(object):

    def __init__(self, driver, config, **context):
        self.driver = driver
        self.config = config
        self.namespace = context.pop('namespace')
        if self.namespace is None:
            self.namespace = config.namespace
        self.context = config.defaults.copy()
        first, last = self.driver.block_offset_bounds(self.namespace)
        self.no_data = last is None
        context['min_block'] = long(first or 0)
        context['max_block'] = long(last or 0)
        for key, value in context.iteritems():
            if value is not None:
                self.context[key] = value
        self.check_timestamps()

    def keys(self):
        context = self.context
        return self.driver.keys(self.namespace, prefix=context['prefix'],
            limit=context['limit'], offset=context['offset'])

    def statistics(self):
        context = self.context
        start, end = context['start'], context['end']
        block_size = self.config.block_size
        start_block = start // block_size
        end_block, end_array = divmod(end, block_size)
        if not end_array:
            end_block -= 1
        start = start_block * block_size
        end = (end_block + 1) * block_size
        namespace = self.namespace
        stats = self.driver.get_namespace_statistics(namespace,
            start_block, end_block)
        return Statistics(namespace, start, end, stats[0], stats[1])

    def value(self, timestamp=None):
        key = self.translated_key
        if not key:
            return None
        context, config = self.context, self.config
        block_size = config.block_size
        look_behind = config.max_look_behind // block_size
        timestamp = context['end'] if timestamp is None else timestamp
        end_block, offset = divmod(timestamp, block_size)
        offset = offset // config.resolution
        get_block = self.get_block
        result = block = None
        try:
            while end_block >= 0:
                block = get_block(key, end_block)
                if block is not None:
                    if offset is not None:
                        tmp = block.slice(end=offset+1)
                        block.free()
                        block = tmp
                    if block.byte_length():
                        result = block.last()
                        block.free()
                        block = None
                        break
                    block.free()
                    block = None
                if not look_behind:
                    break
                offset = None
                look_behind -= 1
                end_block -= 1
        finally:
            if block is not None:
                block.free()
        return result

    def aggregate(self, start=None, end=None):
        key = self.translated_key
        if not key:
            return None
        context = self.context
        aggregate = context['aggregate']
        start = context['start'] if start is None else start
        end = context['end'] if end is None else end
        result = block = None
        aggregate = context['aggregate']
        if aggregate not in Aggregate.ALL:
            raise ValueError('Unknown aggregate: %s' % aggregate)
        try:
            # TODO: Certain aggregates could call aggregate_series()
            # to utilise the cache and then aggregate the results
            if aggregate == Aggregate.SUM:
                for block in self.block_iterator(start, end, key):
                    if result is None:
                        result = 0
                    result += block.sum()
                    block.free()
                    block = None
            elif aggregate == Aggregate.COUNT:
                result = 0
                for block in self.block_iterator(start, end, key):
                    result += block.count()
                    block.free()
                    block = None
            elif aggregate == Aggregate.MIN:
                for block in self.block_iterator(start, end, key):
                    comparison = block.min()
                    if result is None or result > comparison:
                        result = comparison
                    block.free()
                    block = None
            elif aggregate == Aggregate.MAX:
                for block in self.block_iterator(start, end, key):
                    comparison = block.max()
                    if result is None or result < comparison:
                        result = comparison
                    block.free()
                    block = None
            elif aggregate == Aggregate.FIRST:
                for block in self.block_iterator(start, end, key):
                    first = block.first()
                    block.free()
                    block = None
                    if first is not None and first == first: # => !NaN
                        result = first
                        break
            elif aggregate == Aggregate.MEAN:
                count = 0
                for block in self.block_iterator(start, end, key):
                    block_count = block.count()
                    if block_count:
                        count += block_count
                        if result is None:
                            result = 0
                        result += block.sum()
                    block.free()
                    block = None
                if result is not None:
                    result = result / count if count else 0
            else: # stddev, percentile, median
                block = self.query(key, start, end)
                result = None
                if block is not None:
                    if aggregate == Aggregate.PERCENTILE:
                        result = block.percentile(context['percentile'])
                    else:
                        result = getattr(block, aggregate)()
        finally:
            if block is not None:
                block.free()
        return result if result == result else None

    def value_series(self):
        key = self.translated_key
        if not key or self.no_data:
            return TimeSeries([])
        context = self.context
        start = context['start']
        end = context['end']
        namespace = self.namespace
        interval = self.interval
        cached_values = {}
        cache = self.cache
        if cache:
            cache_key = self.cache_key
            driver = self.driver
            cached = driver.get_cache(namespace, cache_key, interval, start, end)
            for timestamp, value in cached:
                cached_values[timestamp] = value
        values = []
        value_fn = self.value
        while start < end:
            group_end = min(end, start + interval)
            if start in cached_values:
                value = cached_values[start]
            else:
                value = value_fn(start)
            values.append(( start, group_end, value ))
            start += interval
        if cache:
            to_cache = []
            cache_until_timestamp = self.cache_until * self.config.block_size
            for start, end, value in values:
                if cache_until_timestamp >= end and start not in cached_values:
                    to_cache.append(( start, value ))
            if len(to_cache):
                driver.add_cache(namespace, cache_key, interval, to_cache)
        return TimeSeries(( (start, value) for start, _, value in values if value ))

    def aggregate_series(self):
        key = self.translated_key
        if not key or self.no_data:
            return TimeSeries([])
        context = self.context
        start = context['start']
        end = context['end']
        namespace = self.namespace
        interval = self.interval
        cached_values = {}
        cache = self.cache
        if cache:
            cache_key = self.cache_key
            driver = self.driver
            cached = driver.get_cache(namespace, cache_key, interval, start, end)
            for timestamp, value in cached:
                cached_values[timestamp] = value
        values = []
        aggregate_fn = self.aggregate
        while start < end:
            group_end = min(end, start + interval)
            if start in cached_values:
                result = cached_values[start]
            else:
                result = aggregate_fn(start, group_end)
            values.append(( start, group_end, result ))
            start += interval
        if cache:
            to_cache = []
            cache_until_timestamp = self.cache_until * self.config.block_size
            for start, end, result in values:
                if cache_until_timestamp >= end and start not in cached_values:
                    to_cache.append(( start, result ))
            if len(to_cache):
                driver.add_cache(namespace, cache_key, interval, to_cache)
        return TimeSeries(( (start, value) for start, _, value in values ))

    def block_iterator(self, start, end, key, yield_if_empty=False):
        config = self.config
        block_size, resolution = config.block_size, config.resolution
        start_block, start_array = divmod(start, block_size)
        end_block, end_array = divmod(end, block_size)
        start_array, end_array = start_array // resolution, end_array // resolution
        if not end_array:
            end_block -= 1
        get_block = self.get_block
        block = None
        try:
            while start_block <= end_block:
                block = get_block(key, start_block)
                if block is not None:
                    if start_block != end_block:
                        if start_array:
                            sliced = block.slice(start=start_array)
                            block.free()
                            block = sliced
                    elif start_array or end_array:
                        sliced = block.slice(start=start_array, end=end_array)
                        block.free()
                        block = sliced
                    yield block
                    block = None
                elif yield_if_empty:
                    yield None
                start_array = 0
                start_block += 1
        finally:
            if block is not None:
                block.free()

    def query(self, key, start, end):
        context = self.context
        start = context['start'] if start is None else start
        end = context['end'] if end is None else end
        blocks = self.block_iterator(start, end, key, yield_if_empty=True)
        block_arrays = self.config.block_arrays
        offset = 0
        result = SparseMap()
        block = None
        try:
            for block in blocks:
                if block is not None:
                    result.concat(block, offset=offset)
                    block.free()
                    block = None
                offset += block_arrays
        except: # pragma: no cover
            result.free()
            raise
        finally:
            if block is not None:
                block.free()
        return result

    def get_block(self, key, block):
        # Note: the second item is a flags column for future extensions, e.g.
        # to signal that the block needs decompressing
        buf, _ = self.driver.get_block(self.namespace, block, key)
        return SparseMap(buf, len(buf)) if buf is not None else None

    def check_timestamps(self):
        context = self.context
        start, end = context['start'], context['end']
        if start is None:
            start = 0
        elif isinstance(start, date):
            start = long(timegm(start.timetuple()) * 1000)
        block_size = self.config.block_size
        if end is None:
            end = context['max_block'] * block_size + block_size
        elif isinstance(end, date):
            end = long(timegm(end.timetuple()) * 1000)
        start = long(start)
        end = long(end)
        if start < 0 or end < 0:
            now = long(time() * 1000)
            if start < 0:
                start += now
            if end < 0:
                end += now
            if start < 0 or end < 0:
                raise GaugedDateRangeError('Invalid date range')
        start = max(context['min_block'] * block_size, start)
        end = min(context['max_block'] * block_size + block_size, end)
        if start > end:
            # Don't error if exactly one timestamp was specified. We might
            # have truncated the other without them knowing..
            has_start_timestamp = context.get('start') is not None
            has_end_timestamp = context.get('end') is not None
            if has_start_timestamp ^ has_end_timestamp:
                start = end
        if start > end:
            raise GaugedDateRangeError('Invalid date range')
        context['start'] = start
        context['end'] = end

    @property
    def translated_key(self):
        namespace_key = (self.namespace, to_bytes(self.context['key']))
        ids = self.driver.lookup_ids([ namespace_key ])
        return ids.get(namespace_key)

    @property
    def cache(self):
        if not self.context['cache']:
            return False
        if self.context['interval'] < self.config.min_cache_interval:
            return False
        return True

    @property
    def cache_until(self):
        return self.context['max_block'] if self.cache else 0

    @property
    def interval(self):
        context = self.context
        interval = long(context['interval'])
        if interval <= 0:
            raise GaugedIntervalSizeError
        interval_steps = (context['end'] - context['start']) // interval
        if interval_steps > self.config.max_interval_steps:
            raise GaugedIntervalSizeError
        return interval

    @property
    def cache_key(self):
        context = self.context
        return sha1(str({
            'key': context['key'],
            'aggregate': context['aggregate'],
            'look_behind': self.config.max_look_behind
        })).digest()
