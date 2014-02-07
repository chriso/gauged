'''
Gauged
https://github.com/chriso/gauged (MIT Licensed)
Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
'''

class GaugedError(Exception):
    '''All Gauged errors inherit from this class'''

class GaugedAppendOnlyError(GaugedError):
    '''An error that occurs when gauges aren't sent to writer.add() in
    chronological order'''

class GaugedDateRangeError(GaugedError):
    '''An error that occurs when the start & end dates in a date range are
    overlapping'''

class GaugedKeyOverflowError(GaugedError):
    '''An error that occurs when the current driver cannot handle
    a large key'''

class GaugedIntervalSizeError(GaugedError):
    '''Occurs when a time series operation contains too many interval
    steps in the specified date range'''

class GaugedNaNError(GaugedError):
    '''Occurs when you try and write a NaN value'''

class GaugedUseAfterFreeError(GaugedError):
    '''Occurs when a structure that allocates memory is used after it
    has been freed, e.g. outside of its context manager'''

class GaugedVersionMismatchError(GaugedError):
    '''Occurs when the Gauged version does not match the version stored in
    the database'''

class GaugedBlockSizeMismatch(RuntimeWarning):
    '''Occurs when the configured block_size and/or resolution doesn't
    match those stored in the database'''

class GaugedSchemaError(GaugedError):
    '''Occurs when an operation is attempted and no schema can be found'''
