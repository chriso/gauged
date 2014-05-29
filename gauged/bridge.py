'''
Gauged
https://github.com/chriso/gauged (MIT Licensed)
Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
'''

import os, glob, sys
from ctypes import POINTER, Structure, CDLL, cdll
from ctypes import c_int, c_size_t, c_uint32, c_char_p, c_bool, c_float

class SharedLibrary(object):
    '''A shared library wrapper'''

    def __init__(self, name, prefix):
        self.prefix = prefix
        path = os.path.dirname(os.path.realpath(os.path.join(__file__, '..')))
        version = sys.version.split(' ')[0][0:3]
        shared_lib = os.path.join(path, 'build', 'lib*-' + version, name + '*.*')
        lib = glob.glob(shared_lib)
        if not lib:
            lib = glob.glob(os.path.join(path, name + '*.*'))
        try:
            self.library = cdll.LoadLibrary(lib[0])
        except OSError as err:
            raise OSError('Failed to load the C extension: ' + str(err))

    def prototype(self, name, argtypes, restype=None):
        '''Define argument / return types for the specified C function'''
        function = self.function(name)
        function.argtypes = argtypes
        if restype:
            function.restype = restype

    def function(self, name):
        '''Get a function by name'''
        return getattr(self.library, '%s_%s' % (self.prefix, name))

    def __getattr__(self, name):
        fn = self.function(name)
        setattr(self, name, fn)
        return fn

class Array(Structure):
    '''A wrapper for the C type gauged_array_t'''
    _fields_ = [('buffer', POINTER(c_float)), ('size', c_size_t),
                ('length', c_size_t)]

class Map(Structure):
    '''A wrapper for the C type gauged_map_t'''
    _fields_ = [('buffer', POINTER(c_uint32)), ('size', c_size_t),
                ('length', c_size_t)]

class WriterHashNode(Structure):
    '''A wrapper for the C type gauged_writer_hash_node_t'''

WriterHashNode._fields_ = [('key', c_char_p), ('map', POINTER(Map)),
    ('array', POINTER(Array)), ('namespace', c_uint32),
    ('seed', c_uint32), ('next', POINTER(WriterHashNode))]

class WriterHash(Structure):
    '''A wrapper for the C type gauged_writer_hash_t'''
    _fields_ = [('nodes', POINTER(POINTER(WriterHashNode))),
                ('size', c_size_t), ('count', c_size_t),
                ('head', POINTER(WriterHashNode))]

class Writer(Structure):
    '''A wrapper for the C type gauged_writer_t'''
    _fields_ = [('pending', POINTER(WriterHash)),
                ('max_key', c_size_t), ('copy', c_char_p),
                ('buffer', POINTER(c_char_p)),
                ('buffer_size', c_size_t)]

# Define pointer types
ArrayPtr = POINTER(Array)
MapPtr = POINTER(Map)
WriterPtr = POINTER(Writer)
SizetPtr = POINTER(c_size_t)
Uint32Ptr = POINTER(c_uint32)
FloatPtr = POINTER(c_float)

# Load the shared library
Gauged = SharedLibrary('_gauged', 'gauged')

# Define argument & return types
Gauged.prototype('array_new', [], ArrayPtr)
Gauged.prototype('array_free', [ArrayPtr])
Gauged.prototype('array_length', [ArrayPtr], c_size_t)
Gauged.prototype('array_export', [ArrayPtr], FloatPtr)
Gauged.prototype('array_import', [FloatPtr, c_size_t], ArrayPtr)
Gauged.prototype('array_append', [ArrayPtr, c_float], c_int)
Gauged.prototype('map_new', [], MapPtr)
Gauged.prototype('map_free', [MapPtr])
Gauged.prototype('map_export', [MapPtr], Uint32Ptr)
Gauged.prototype('map_length', [MapPtr], c_size_t)
Gauged.prototype('map_import', [Uint32Ptr, c_size_t], MapPtr)
Gauged.prototype('map_append', [MapPtr, c_uint32, ArrayPtr], c_int)
Gauged.prototype('map_advance', [Uint32Ptr, SizetPtr, Uint32Ptr, SizetPtr,
    POINTER(FloatPtr)], Uint32Ptr)
Gauged.prototype('map_concat', [MapPtr, MapPtr, c_uint32, c_uint32,
    c_uint32], c_int)
Gauged.prototype('map_first', [MapPtr], c_float)
Gauged.prototype('map_last', [MapPtr], c_float)
Gauged.prototype('map_sum', [MapPtr], c_float)
Gauged.prototype('map_min', [MapPtr], c_float)
Gauged.prototype('map_max', [MapPtr], c_float)
Gauged.prototype('map_mean', [MapPtr], c_float)
Gauged.prototype('map_stddev', [MapPtr], c_float)
Gauged.prototype('map_sum_of_squares', [MapPtr, c_float], c_float)
Gauged.prototype('map_count', [MapPtr], c_float)
Gauged.prototype('map_percentile', [MapPtr, c_float, FloatPtr], c_int)
Gauged.prototype('writer_new', [c_size_t], WriterPtr)
Gauged.prototype('writer_free', [WriterPtr])
Gauged.prototype('writer_flush_arrays', [WriterPtr, c_uint32], c_int)
Gauged.prototype('writer_flush_maps', [WriterPtr, c_bool], c_int)
