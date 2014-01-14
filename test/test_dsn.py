'''
Gauged - https://github.com/chriso/gauged
Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
'''

from gauged.drivers import parse_dsn, SQLiteDriver
from .test_case import TestCase

class TestDSN(TestCase):
    '''Test the driver functions'''

    def test_stripping_dialect_from_schema(self):
        driver = parse_dsn('sqlite+foobar://')[0]
        self.assertIs(driver, SQLiteDriver)

    def test_unknown_driver(self):
        with self.assertRaises(ValueError):
            parse_dsn('foobar://')

    def test_user_and_password(self):
        kwargs = parse_dsn('mysql://root:foo@localhost')[2]
        self.assertEqual(kwargs['user'], 'root')
        self.assertEqual(kwargs['passwd'], 'foo')
        kwargs = parse_dsn('postgresql://root:foo@localhost')[2]
        self.assertEqual(kwargs['user'], 'root')
        self.assertEqual(kwargs['password'], 'foo')

    def test_user_without_password(self):
        kwargs = parse_dsn('mysql://root@localhost')[2]
        self.assertEqual(kwargs['user'], 'root')
        self.assertNotIn('passwd', kwargs)
        kwargs = parse_dsn('postgresql://root@localhost')[2]
        self.assertEqual(kwargs['user'], 'root')
        self.assertNotIn('password', kwargs)

    def test_default_users(self):
        kwargs = parse_dsn('mysql://localhost')[2]
        self.assertEqual(kwargs['user'], 'root')
        kwargs = parse_dsn('postgresql://localhost')[2]
        self.assertEqual(kwargs['user'], 'postgres')

    def test_unix_socket(self):
        kwargs = parse_dsn('mysql:///?unix_socket=/tmp/mysql.sock')[2]
        self.assertEqual(kwargs['unix_socket'], '/tmp/mysql.sock')
        kwargs = parse_dsn('postgresql:///?unix_socket=/tmp/mysql.sock')[2]
        self.assertNotIn('unix_socket', kwargs)
        self.assertEqual(kwargs['host'], '/tmp/mysql.sock')

    def test_no_host_or_port(self):
        kwargs = parse_dsn('mysql://')[2]
        self.assertNotIn('host', kwargs)
        self.assertNotIn('port', kwargs)
        kwargs = parse_dsn('postgresql://')[2]
        self.assertNotIn('host', kwargs)
        self.assertNotIn('port', kwargs)

    def test_hort_without_port(self):
        kwargs = parse_dsn('mysql://localhost')[2]
        self.assertEqual(kwargs['host'], 'localhost')
        self.assertNotIn('port', kwargs)
        kwargs = parse_dsn('postgresql://localhost')[2]
        self.assertEqual(kwargs['host'], 'localhost')
        self.assertNotIn('port', kwargs)

    def test_hort_with_port(self):
        kwargs = parse_dsn('mysql://localhost:123')[2]
        self.assertEqual(kwargs['host'], 'localhost')
        self.assertEqual(kwargs['port'], 123)
        kwargs = parse_dsn('postgresql://localhost:123')[2]
        self.assertEqual(kwargs['host'], 'localhost')
        self.assertEqual(kwargs['port'], 123)

    def test_passing_kwargs(self):
        kwargs = parse_dsn('mysql://localhost?foo=bar')[2]
        self.assertEqual(kwargs['foo'], 'bar')
        kwargs = parse_dsn('postgresql://localhost?foo=bar')[2]
        self.assertEqual(kwargs['foo'], 'bar')
