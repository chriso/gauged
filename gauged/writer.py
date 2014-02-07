'''
Gauged
https://github.com/chriso/gauged (MIT Licensed)
Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
'''

from time import time
from threading import Timer
from types import StringType, UnicodeType, DictType
from collections import defaultdict
from pprint import pprint
from ctypes import c_uint32, byref
from .structures import SparseMap
from .lru import LRU
from ctypes import c_float
from .errors import (GaugedAppendOnlyError, GaugedKeyOverflowError, GaugedNaNError,
    GaugedUseAfterFreeError)
from .bridge import Gauged
from .results import Statistics
from .utilities import to_bytes

class Writer(object):
    '''Handle queueing and writes to the specified data store'''

    ERROR = 0
    IGNORE = 1
    REWRITE = 2

    KEY_OVERFLOW = -1

    ALLOCATIONS = 0

    def __init__(self, driver, config):
        self.driver = driver
        self.config = config
        self.key_cache = LRU(config.key_cache_size)
        self.current_array = 0
        self.current_block = 0
        self.whitelist = None
        self.flush_now = False
        self.statistics = defaultdict(Statistics)
        if config.flush_seconds:
            self.start_flush_timer()
        self.flush_daemon = None
        self.writer = None
        self.allocate_writer()

    def add(self, data, value=None, timestamp=None, namespace=None, debug=False):
        '''Queue a gauge or gauges to be written'''
        if value is not None:
            return self.add(((data, value),), timestamp=timestamp,
                namespace=namespace, debug=debug)
        writer = self.writer
        if writer is None:
            raise GaugedUseAfterFreeError
        if timestamp is None:
            timestamp = long(time() * 1000)
        config = self.config
        block_size = config.block_size
        this_block = timestamp // block_size
        this_array = (timestamp % block_size) // config.resolution
        if namespace is None:
            namespace = config.namespace
        if this_block < self.current_block or \
                (this_block == self.current_block \
                    and this_array < self.current_array):
            if config.append_only_violation == Writer.ERROR:
                msg = 'Gauged is append-only; timestamps must be increasing'
                raise GaugedAppendOnlyError(msg)
            elif config.append_only_violation == Writer.REWRITE:
                this_block = self.current_block
                this_array = self.current_array
            else:
                return
        if type(data) == UnicodeType:
            data = data.encode('utf8')
        if debug:
            return self.debug(timestamp, namespace, data)
        if this_block > self.current_block:
            self.flush_blocks()
            self.current_block = this_block
            self.current_array = this_array
        elif this_array > self.current_array:
            if not Gauged.writer_flush_arrays(writer, self.current_array):
                raise MemoryError
            self.current_array = this_array
        data_points = 0
        namespace_statistics = self.statistics[namespace]
        whitelist = config.key_whitelist
        skip_long_keys = config.key_overflow == Writer.IGNORE
        skip_gauge_nan = config.gauge_nan == Writer.IGNORE
        if type(data) == StringType and skip_gauge_nan \
                and skip_long_keys and whitelist is None: # fast path
            data_points = c_uint32(0)
            if not Gauged.writer_emit_pairs(writer, namespace, data, \
                    byref(data_points)):
                raise MemoryError
            data_points = data_points.value
        else:
            if type(data) == DictType:
                data = data.iteritems()
            elif type(data) == StringType:
                data = self.parse_query(data)
            emit = Gauged.writer_emit
            for key, value in data:
                key = to_bytes(key)
                if whitelist is not None and key not in whitelist:
                    continue
                try:
                    value = float(value)
                except ValueError:
                    value = float('nan')
                if value != value: # NaN?
                    if skip_gauge_nan:
                        continue
                    raise GaugedNaNError
                success = emit(writer, namespace, key, c_float(value))
                if success != 1:
                    if not success:
                        raise MemoryError
                    elif success == Writer.KEY_OVERFLOW and not skip_long_keys:
                        msg = 'Key is larger than the driver allows '
                        msg += '(%s)' % key
                        raise GaugedKeyOverflowError(msg)
                data_points += 1
        namespace_statistics.data_points += data_points
        if self.flush_now:
            self.flush()

    def flush(self):
        '''Flush all pending gauges'''
        writer = self.writer
        if writer is None:
            raise GaugedUseAfterFreeError
        self.flush_writer_position()
        keys = self.translate_keys()
        blocks = []
        current_block = self.current_block
        statistics = self.statistics
        driver = self.driver
        flags = 0 # for future extensions, e.g. block compression
        for namespace, key, block in self.pending_blocks():
            length = block.byte_length()
            if not length:
                continue
            key_id = keys[( namespace, key )]
            statistics[namespace].byte_count += length
            blocks.append((namespace, current_block, key_id, block.buffer(), flags))
        if self.config.overwrite_blocks:
            driver.replace_blocks(blocks)
        else:
            driver.insert_or_append_blocks(blocks)
            if not Gauged.writer_flush_maps(writer, True):
                raise MemoryError
        update_namespace = driver.add_namespace_statistics
        for namespace, stats in statistics.iteritems():
            update_namespace(namespace, self.current_block,
                stats.data_points, stats.byte_count)
        statistics.clear()
        driver.commit()
        self.flush_now = False

    def resume_from(self):
        '''Get a timestamp representing the position just after the last
        written gauge'''
        position = self.driver.get_writer_position(self.config.writer_name)
        return position + self.config.resolution if position else 0

    def clear_from(self, timestamp):
        '''Clear all data from `timestamp` onwards. Note that the timestamp
        is rounded down to the nearest block boundary'''
        block_size = self.config.block_size
        offset, remainder = timestamp // block_size, timestamp % block_size
        if remainder:
            raise ValueError('Timestamp must be on a block boundary')
        self.driver.clear_from(offset, timestamp)

    def parse_query(self, query):
        '''Parse a query string and return an iterator which yields (key, value)'''
        writer = self.writer
        if writer is None:
            raise GaugedUseAfterFreeError
        Gauged.writer_parse_query(writer, query)
        position = 0
        writer_contents = writer.contents
        size = writer_contents.buffer_size
        pointers = writer_contents.buffer
        while position < size:
            yield pointers[position], pointers[position+1]
            position += 2

    def debug(self, timestamp, namespace, data): # pragma: no cover
        print 'Timestamp: %s, Namespace: %s' % (timestamp, namespace)
        if type(data) == StringType:
            data = self.parse_query(data)
        elif type(data) == DictType:
            data = data.iteritems()
        whitelist = self.config.key_whitelist
        if whitelist is not None:
            data = { key: value for key, value in data if key in whitelist }
        else:
            data = dict(data)
        pprint(data)
        print ''

    def flush_writer_position(self):
        config = self.config
        timestamp = long(self.current_block) * config.block_size \
            + long(self.current_array) * config.resolution
        if timestamp:
            self.driver.set_writer_position(config.writer_name, timestamp)

    def translate_keys(self):
        keys = list(self.pending_keys())
        if not len(keys):
            return {}
        key_cache = self.key_cache
        to_translate = [ key for key in keys if key not in key_cache ]
        self.driver.insert_keys(to_translate)
        ids = self.driver.lookup_ids(to_translate)
        for key in keys:
            if key in key_cache:
                ids[key] = key_cache[key]
        for key, id_ in ids.iteritems():
            key_cache[key] = id_
        return ids

    def flush_blocks(self):
        writer = self.writer
        if not Gauged.writer_flush_arrays(writer, self.current_array):
            raise MemoryError
        self.flush()
        if not Gauged.writer_flush_maps(writer, False):
            raise MemoryError

    def start_flush_timer(self):
        period = self.config.flush_seconds
        self.flush_daemon = Timer(period, self.flush_timer_tick)
        self.flush_daemon.setDaemon(True)
        self.flush_daemon.start()

    def flush_timer_tick(self):
        self.flush_now = True
        self.start_flush_timer()

    def pending_keys(self):
        pending = self.writer.contents.pending.contents
        head = pending.head
        while True:
            if not head:
                break
            node = head.contents
            yield node.namespace, node.key
            head = node.next

    def pending_blocks(self):
        block = SparseMap()
        initial_ptr = block._ptr
        pending = self.writer.contents.pending.contents
        head = pending.head
        while True:
            if not head:
                break
            node = head.contents
            block._ptr = node.map
            yield node.namespace, node.key, block
            head = node.next
        block._ptr = initial_ptr
        block.free()

    def allocate_writer(self):
        self.current_array = 0
        self.current_block = 0
        if self.writer is None:
            self.writer = Gauged.writer_new(self.driver.MAX_KEY or 0)
            Writer.ALLOCATIONS += 1
            if not self.writer:
                raise MemoryError

    def cleanup(self):
        if self.flush_daemon is not None:
            self.flush_daemon.cancel()
            self.flush_daemon = None
        self.flush_blocks()
        Gauged.writer_free(self.writer)
        Writer.ALLOCATIONS -= 1
        self.writer = None
        self.key_cache.clear()

    def __enter__(self):
        self.allocate_writer()
        return self

    def __exit__(self, type_, value, traceback):
        self.cleanup()
