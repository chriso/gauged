'''
Gauged
https://github.com/chriso/gauged (MIT Licensed)
Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
'''

from time import time
from datetime import datetime
from sys import builtin_module_names
from types import StringType, UnicodeType

IS_PYPY = '__pypy__' in builtin_module_names

def to_bytes(value):
    '''Get a byte array representing the value'''
    if type(value) == UnicodeType:
        return value.encode('utf8')
    elif type(value) != StringType:
        return str(value)
    return value

class Time(object):
    '''Common time constants in milliseconds'''

    SECOND = 1000
    MINUTE = 60 * SECOND
    HOUR = 60 * MINUTE
    DAY = 24 * HOUR
    WEEK = 7 * DAY

    def __get__(self, instance, owner):
        return long(time() * 1000)

def table_repr(columns, rows, data, padding=2):
    '''Generate a table for cli output'''
    padding = ' ' * padding
    column_lengths = [ len(column) for column in columns ]
    for row in rows:
        for i, column in enumerate(columns):
            item = str(data[row][column])
            column_lengths[i] = max(len(item), column_lengths[i])
    max_row_length = max(( len(row) for row in rows )) if len(rows) else 0
    table_row = ' ' * max_row_length
    for i, column in enumerate(columns):
        table_row += padding + column.rjust(column_lengths[i])
    table_rows = [ table_row ]
    for row in rows:
        table_row = row.rjust(max_row_length)
        for i, column in enumerate(columns):
            item = str(data[row][column])
            table_row += padding + item.rjust(column_lengths[i])
        table_rows.append(table_row)
    return '\n'.join(table_rows)

def to_datetime(milliseconds):
    '''Convert a timestamp in milliseconds to a datetime'''
    return datetime.fromtimestamp(milliseconds // 1000)
