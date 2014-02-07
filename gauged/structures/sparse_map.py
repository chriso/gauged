'''
Gauged
https://github.com/chriso/gauged (MIT Licensed)
Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
'''

from types import DictType, BufferType
from ctypes import (create_string_buffer, c_void_p, pythonapi, py_object, byref,
    cast, c_uint32, addressof, c_char, c_size_t, c_float)
from ..bridge import Gauged, MapPtr, Uint32Ptr, FloatPtr
from ..utilities import IS_PYPY
from ..errors import GaugedUseAfterFreeError

class SparseMap(object):
    '''A structure which adds another dimension to FloatArray. The
    Map encodes a FloatArray + offset contiguously to improve cache
    locality'''

    ALLOCATIONS = 0

    __slots__ = [ '_ptr' ]

    def __init__(self, buf=None, length=0):
        '''Create a new SparseMap. The constructor accepts a buffer and
        byte_length, a python dict containing { offset: array, ... }, or a
        pointer to a C structure'''
        if type(buf) == DictType:
            items = buf
            buf = None
        else:
            items = None
        if type(buf) == MapPtr:
            self._ptr = buf
        else:
            if buf is not None:
                if IS_PYPY:
                    buf = create_string_buffer(str(buf))
                if type(buf) == BufferType:
                    address = c_void_p()
                    buf_length = c_size_t()
                    pythonapi.PyObject_AsReadBuffer(py_object(buf),
                        byref(address), byref(buf_length))
                    buf_length = buf_length.value
                    buf = address
                buf = cast(buf, Uint32Ptr)
            self._ptr = Gauged.map_import(buf, length)
        if self._ptr is None:
            raise MemoryError
        SparseMap.ALLOCATIONS += 1
        if items is not None:
            for position in sorted(items.keys()):
                self.append(position, items[position])

    @property
    def ptr(self):
        '''Get the map's C pointer'''
        if self._ptr is None:
            raise GaugedUseAfterFreeError
        return self._ptr

    def free(self):
        '''Free the map'''
        if self._ptr is None:
            return
        Gauged.map_free(self.ptr)
        SparseMap.ALLOCATIONS -= 1
        self._ptr = None

    def append(self, position, array):
        '''Append an array to the end of the map. The position
        must be greater than any positions in the map'''
        if not Gauged.map_append(self.ptr, position, array.ptr):
            raise MemoryError

    def slice(self, start=0, end=0):
        '''Slice the map from [start, end)'''
        tmp = Gauged.map_new()
        if tmp is None:
            raise MemoryError
        if not Gauged.map_concat(tmp, self.ptr, start, end, 0):
            Gauged.map_free(tmp) # pragma: no cover
            raise MemoryError
        return SparseMap(tmp)

    def concat(self, operand, start=0, end=0, offset=0):
        '''Concat a map. You can also optionally slice the operand map
        and apply an offset to each position before concatting'''
        if not Gauged.map_concat(self.ptr, operand.ptr, start, end, offset):
            raise MemoryError

    def byte_length(self):
        '''Get the byte length of the map'''
        return self.ptr.contents.length * 4

    def buffer(self, byte_offset=0):
        '''Get a copy of the map buffer'''
        contents = self.ptr.contents
        ptr = addressof(contents.buffer.contents) + byte_offset
        length = contents.length * 4 - byte_offset
        return buffer((c_char * length).from_address(ptr).raw) if length else None

    def clear(self):
        '''Clear the map'''
        self.ptr.contents.length = 0

    def first(self):
        '''Get the first float in the map'''
        return Gauged.map_first(self.ptr)

    def last(self):
        '''Get the last float in the map'''
        return Gauged.map_last(self.ptr)

    def sum(self):
        '''Get the sum of all floats in the map'''
        return Gauged.map_sum(self.ptr)

    def min(self):
        '''Get the minimum of all floats in the map'''
        return Gauged.map_min(self.ptr)

    def max(self):
        '''Get the maximum of all floats in the map'''
        return Gauged.map_max(self.ptr)

    def mean(self):
        '''Get the maximum of all floats in the map'''
        return Gauged.map_mean(self.ptr)

    def stddev(self):
        '''Get the maximum of all floats in the map'''
        return Gauged.map_stddev(self.ptr)

    def count(self):
        '''Get the maximum of all floats in the map'''
        return Gauged.map_count(self.ptr)

    def sum_of_squares(self, mean):
        '''Get the sum of squared differences compared to the specified
        mean'''
        return Gauged.map_sum_of_squares(self.ptr, c_float(mean))

    def percentile(self, percentile):
        '''Get a percentile of all floats in the map. Since the sorting is
        done in-place, the map is no longer safe to use after calling this
        or median()'''
        percentile = float(percentile)
        if percentile != percentile or percentile < 0 or percentile > 100:
            raise ValueError('Expected a 0 <= percentile <= 100')
        result = c_float()
        if not Gauged.map_percentile(self.ptr, percentile, byref(result)):
            raise MemoryError
        return result.value

    def median(self):
        '''Get the median of all floats in the map'''
        return self.percentile(50)

    def items(self):
        '''Get a dict representing map items => { offset: array, ... }'''
        return dict(self.iteritems())

    def iteritems(self):
        '''Get a generator which yields (offset, array)'''
        current_buf = self.ptr.contents.buffer
        length = self.ptr.contents.length
        seen_length = 0
        header = c_size_t()
        position = c_uint32()
        arraylength = c_size_t()
        arrayptr = FloatPtr()
        header_ = byref(header)
        position_ = byref(position)
        arraylength_ = byref(arraylength)
        arrayptr_ = byref(arrayptr)
        advance = Gauged.map_advance
        while seen_length < length:
            current_buf = advance(current_buf, header_, position_,
                arraylength_, arrayptr_)
            seen_length += header.value + arraylength.value
            address = addressof(arrayptr.contents)
            arr = (c_float * arraylength.value).from_address(address)
            yield position.value, list(arr)

    def __repr__(self):
        rows = []
        for position, values in self.iteritems():
            row = '[ %s ] = [%s]' % (position, ', '.join(( str(v) for v in values )))
            rows.append(row)
        return '\n'.join(rows)

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self.free()
