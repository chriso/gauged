'''
Gauged
https://github.com/chriso/gauged (MIT Licensed)
Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
'''

from urlparse import urlparse, parse_qsl
from urllib import unquote
from .interface import DriverInterface
from .mysql import MySQLDriver
from .sqlite import SQLiteDriver
from .postgresql import PostgreSQLDriver
from ..utilities import IS_PYPY

def parse_dsn(dsn_string):
    '''Parse a connection string and return the associated driver'''
    dsn = urlparse(dsn_string)
    scheme = dsn.scheme.split('+')[0]
    username = password = host = port = None
    host = dsn.netloc
    if '@' in host:
        username, host = host.split('@')
        if ':' in username:
            username, password = username.split(':')
            password = unquote(password)
        username = unquote(username)
    if ':' in host:
        host, port = host.split(':')
        port = int(port)
    database = dsn.path.split('?')[0][1:]
    query = dsn.path.split('?')[1] if '?' in dsn.path else dsn.query
    kwargs = dict(parse_qsl(query, True))
    if scheme == 'sqlite':
        return SQLiteDriver, [ dsn.path ], {}
    elif scheme == 'mysql':
        kwargs['user'] = username or 'root'
        kwargs['db'] = database
        if port:
            kwargs['port'] = port
        if host:
            kwargs['host'] = host
        if password:
            kwargs['passwd'] = password
        return MySQLDriver, [], kwargs
    elif scheme == 'postgresql':
        kwargs['user'] = username or 'postgres'
        kwargs['database'] = database
        if port:
            kwargs['port'] = port
        if 'unix_socket' in kwargs:
            kwargs['host'] = kwargs.pop('unix_socket')
        elif host:
            kwargs['host'] = host
        if password:
            kwargs['password'] = password
        return PostgreSQLDriver, [], kwargs
    else:
        raise ValueError('Unknown driver %s' % dsn_string)

def get_driver(dsn_string):
    driver, args, kwargs = parse_dsn(dsn_string)
    return driver(*args, **kwargs)
