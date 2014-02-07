'''
Gauged
https://github.com/chriso/gauged (MIT Licensed)
Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
'''

from types import ListType, BufferType
from ctypes import (create_string_buffer, c_void_p, pythonapi, py_object, byref,
    cast, addressof, c_char, c_size_t)
from ..bridge import Gauged, FloatPtr
from ..errors import GaugedUseAfterFreeError
from ..utilities import IS_PYPY

class FloatArray(object):
    '''An array of C floats'''

    ALLOCATIONS = 0

    __slots__ = [ '_ptr' ]

    def __init__(self, buf=None, length=0):
        '''Create a new array. The constructor accepts a buffer + byte_length or a
        python list of floats'''
        if type(buf) == ListType:
            items = buf
            buf = None
        else:
            items = None
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
            buf = cast(buf, FloatPtr)
        self._ptr = Gauged.array_import(buf, length)
        FloatArray.ALLOCATIONS += 1
        if self._ptr is None:
            raise MemoryError
        if items is not None:
            for item in items:
                self.append(item)

    @property
    def ptr(self):
        '''Get the array's C pointer'''
        if self._ptr is None:
            raise GaugedUseAfterFreeError
        return self._ptr

    def free(self):
        '''Free the underlying C array'''
        if self._ptr is None:
            return
        Gauged.array_free(self.ptr)
        FloatArray.ALLOCATIONS -= 1
        self._ptr = None

    def values(self):
        '''Get all floats in the array as a list'''
        return list(self)

    def append(self, member):
        '''Append a float to the array'''
        if not Gauged.array_append(self.ptr, member):
            raise MemoryError

    def byte_length(self):
        '''Get the byte length of the array'''
        return self.ptr.contents.length * 4

    def buffer(self, byte_offset=0):
        '''Get a copy of the array buffer'''
        contents = self.ptr.contents
        ptr = addressof(contents.buffer.contents) + byte_offset
        length = contents.length * 4 - byte_offset
        return buffer((c_char * length).from_address(ptr).raw) if length else None

    def clear(self):
        '''Clear the array'''
        self.ptr.contents.length = 0

    def __getitem__(self, offset):
        '''Get the member at the specified offset'''
        contents = self.ptr.contents
        if offset >= contents.length:
            raise IndexError
        return contents.buffer[offset]

    def __len__(self):
        '''Get the number of floats in the array'''
        return self.ptr.contents.length

    def __repr__(self):
        return '[' + ', '.join(( str(value) for value in self )) + ']'

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self.free()
