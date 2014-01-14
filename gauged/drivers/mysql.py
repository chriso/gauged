'''
Gauged
https://github.com/chriso/gauged (GPL Licensed)
Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
'''

from warnings import filterwarnings
from .interface import DriverInterface

class MySQLDriver(DriverInterface):
    '''A mysql driver for gauged'''

    MAX_KEY = 255

    def __init__(self, bulk_insert=1000, **kwargs):
        try:
            mysql = __import__('MySQLdb')
            filterwarnings('ignore', category=mysql.Warning)
            self.to_buffer = lambda buf: buf
        except ImportError:
            try:
                mysql = __import__('pymysql')
                self.to_buffer = str
            except ImportError:
                raise ImportError('The mysql-python or pymysql library is required')
        self.db = mysql.connect(**kwargs)
        self.bulk_insert = bulk_insert
        self.cursor = self.db.cursor()

    def keys(self, namespace, prefix=None, limit=None, offset=None):
        '''Get keys from a namespace'''
        args = [ namespace ]
        query = '''SELECT `key` FROM gauged_keys
            WHERE namespace = %s'''
        if prefix is not None:
            query += ' AND `key` LIKE %s'
            args.append(prefix + '%')
        if limit is not None:
            query += ' LIMIT '
            if offset is not None:
                query += '%s, '
                args.append(offset)
            query += '%s'
            args.append(limit)
        cursor = self.cursor
        cursor.execute(query, args)
        return [ key for key, in cursor ]

    def lookup_ids(self, keys):
        '''Lookup the integer ID associated with each (namespace, key) in the
        keys list'''
        keys_len = len(keys)
        ids = { namespace_key: None for namespace_key in keys }
        start = 0
        bulk_insert = self.bulk_insert
        query = 'SELECT namespace, `key`, id FROM gauged_keys WHERE '
        check = '(namespace = %s AND `key` = %s) '
        cursor = self.cursor
        execute = cursor.execute
        while start < keys_len:
            namespace_keys = keys[start:start+bulk_insert]
            args = []
            for namespace, key in namespace_keys:
                args.extend([ namespace, key ])
            id_query = query + (check + ' OR ') * (len(namespace_keys) - 1) + check
            execute(id_query, args)
            for namespace, key, id_ in cursor:
                ids[( namespace, key )] = id_
            start += bulk_insert
        return ids

    def get_block(self, namespace, offset, key):
        '''Get the block identified by namespace, offset and key'''
        lookup = ( namespace, offset, key )
        cursor = self.cursor
        cursor.execute('''SELECT data, flags FROM gauged_data
            WHERE namespace = %s AND offset = %s AND `key` = %s''', lookup)
        row = cursor.fetchone()
        return ( None, None ) if row is None else row

    def insert_keys(self, keys):
        '''Insert keys into a table which assigns an ID'''
        start = 0
        bulk_insert = self.bulk_insert
        keys_len = len(keys)
        query = 'INSERT IGNORE INTO gauged_keys (namespace, `key`) VALUES '
        execute = self.cursor.execute
        while start < keys_len:
            namespace_keys = keys[start:start+bulk_insert]
            args = []
            for namespace, key in namespace_keys:
                args.extend([ namespace, key ])
            insert = '(%s,%s),' * (len(namespace_keys) - 1) + '(%s,%s)'
            execute(query + insert, args)
            start += bulk_insert

    def replace_blocks(self, blocks):
        '''Replace multiple blocks. blocks must be a list of tuples where
        each tuple consists of (namespace, offset, key, data)'''
        start = 0
        bulk_insert = self.bulk_insert
        blocks_len = len(blocks)
        row = '(%s,%s,%s,%s,%s)'
        query = 'REPLACE INTO gauged_data (namespace, offset, `key`, data, flags) VALUES '
        execute = self.cursor.execute
        to_buffer = self.to_buffer
        while start < blocks_len:
            values = blocks[start:start+bulk_insert]
            params = []
            for namespace, offset, key, data, flags in values:
                data = to_buffer(data)
                params.extend([ namespace, offset, key, data, flags ])
            insert = (row + ',') * (len(values) - 1) + row
            execute(query + insert, params)
            start += bulk_insert

    def insert_or_append_blocks(self, blocks):
        '''Insert multiple blocks. If a block already exists, the data is
        appended. blocks must be a list of tuples where each tuple consists
        of (namespace, offset, key, data)'''
        start = 0
        bulk_insert = self.bulk_insert
        blocks_len = len(blocks)
        row = '(%s,%s,%s,%s,%s)'
        query = 'INSERT INTO gauged_data (namespace, offset, `key`, data, flags) VALUES '
        post = ''' ON DUPLICATE KEY UPDATE data = CONCAT(data, VALUES(data)),
            flags = VALUES(flags)'''
        execute = self.cursor.execute
        to_buffer = self.to_buffer
        while start < blocks_len:
            values = blocks[start:start+bulk_insert]
            params = []
            for namespace, offset, key, data, flags in values:
                data = to_buffer(data)
                params.extend([ namespace, offset, key, data, flags ])
            insert = (row + ',') * (len(values) - 1) + row
            execute(query + insert + post, params)
            start += bulk_insert

    def block_offset_bounds(self, namespace):
        '''Get the minimum and maximum block offset for the specified namespace'''
        cursor = self.cursor
        cursor.execute('''SELECT CONVERT(MIN(offset), UNSIGNED),
            CONVERT(MAX(offset), UNSIGNED)
            FROM gauged_statistics WHERE namespace = %s''', (namespace,))
        return cursor.fetchone()

    def set_metadata(self, metadata, replace=True):
        query = 'REPLACE' if replace else 'INSERT IGNORE'
        args = []
        for pair in metadata.iteritems():
            args.extend(pair)
        query += ' INTO gauged_metadata VALUES (%s,%s)'
        query += ',(%s,%s)' * (len(metadata) - 1)
        self.cursor.execute(query, args)
        self.commit()

    def get_metadata(self, key):
        cursor = self.cursor
        cursor.execute('SELECT value FROM gauged_metadata WHERE `key` = %s', (key,))
        result = cursor.fetchone()
        return result[0] if result else None

    def all_metadata(self):
        cursor = self.cursor
        cursor.execute('SELECT * FROM gauged_metadata')
        return dict(( row for row in self.cursor ))

    def set_writer_position(self, name, timestamp):
        '''Insert a timestamp to keep track of the current writer position'''
        self.cursor.execute('''REPLACE INTO gauged_writer_history (id, timestamp)
            VALUES (%s, %s)''', (name, timestamp))

    def get_writer_position(self, name):
        '''Get the current writer position'''
        cursor = self.cursor
        cursor.execute('''SELECT timestamp FROM gauged_writer_history
            WHERE id = %s''', (name,))
        result = cursor.fetchone()
        return result[0] if result else 0

    def get_namespaces(self):
        '''Get a list of namespaces'''
        cursor = self.cursor
        cursor.execute('''SELECT DISTINCT namespace FROM gauged_statistics''')
        return [ namespace for namespace, in cursor ]

    def remove_namespace(self, namespace):
        '''Remove all data associated with the current namespace'''
        args = (namespace, )
        execute = self.cursor.execute
        execute('DELETE FROM gauged_data WHERE namespace = %s', args)
        execute('DELETE FROM gauged_statistics WHERE namespace = %s', args)
        execute('DELETE FROM gauged_keys WHERE namespace = %s', args)
        self.remove_cache(namespace)

    def clear_from(self, offset, timestamp):
        args = (offset, )
        execute = self.cursor.execute
        execute('''DELETE FROM gauged_data WHERE offset >= %s''', args)
        execute('''DELETE FROM gauged_statistics WHERE offset >= %s ''', args)
        execute('''DELETE FROM gauged_cache WHERE start + length >= %s''',
            (timestamp,))
        execute('''UPDATE gauged_writer_history SET timestamp = %s
            WHERE timestamp > %s''', (timestamp, timestamp))

    def get_cache(self, namespace, query_hash, length, start, end):
        '''Get a cached count for the specified date range and query'''
        row = (namespace, query_hash, length, start, end)
        cursor = self.cursor
        cursor.execute('''SELECT start, value FROM gauged_cache WHERE namespace = %s
            AND hash = %s AND length = %s AND start BETWEEN %s AND %s''', row)
        return cursor.fetchall()

    def add_cache(self, namespace, query_hash, length, cache):
        '''Add cached counts for the specified date range and query'''
        start = 0
        bulk_insert = self.bulk_insert
        cache_len = len(cache)
        row = '(%s,%s,%s,%s,%s)'
        query = '''INSERT IGNORE INTO gauged_cache
            (namespace, hash, length, start, value) VALUES '''
        execute = self.cursor.execute
        while start < cache_len:
            values = cache[start:start+bulk_insert]
            params = []
            for start_, count in values:
                params.extend([ namespace, query_hash, length, start_, count ])
            insert = (row + ',') * (len(values) - 1) + row
            execute(query + insert, params)
            start += bulk_insert
        self.db.commit()

    def remove_cache(self, namespace):
        '''Remove all cached counts for the specified namespace'''
        self.cursor.execute('DELETE FROM gauged_cache WHERE namespace = %s', (namespace,))

    def commit(self):
        '''Commit the current transaction'''
        self.db.commit()

    def create_schema(self):
        '''Create all necessary tables'''
        cursor = self.cursor
        execute = cursor.execute
        execute('SHOW TABLES')
        tables = set(( table for table, in cursor ))
        if 'gauged_data' not in tables:
            execute('''CREATE TABLE gauged_data (
                namespace INT(11) UNSIGNED NOT NULL,
                offset INT(11) UNSIGNED NOT NULL,
                `key` BIGINT(15) UNSIGNED NOT NULL,
                data MEDIUMBLOB NOT NULL,
                flags INT(11) UNSIGNED NOT NULL,
                PRIMARY KEY (offset, namespace, `key`))''')
        if 'gauged_keys' not in tables:
            execute('''CREATE TABLE gauged_keys (
                id BIGINT(15) UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
                namespace INT(11) UNSIGNED NOT NULL,
                `key` VARCHAR(255) BINARY NOT NULL,
                UNIQUE KEY (namespace, `key`))''')
        if 'gauged_writer_history' not in tables:
            execute('''CREATE TABLE gauged_writer_history (
                id VARCHAR(255) NOT NULL PRIMARY KEY,
                timestamp BIGINT(15) UNSIGNED NOT NULL)''')
        if 'gauged_cache' not in tables:
            execute('''CREATE TABLE gauged_cache (
                namespace INT(11) UNSIGNED NOT NULL,
                hash BINARY(20) NOT NULL,
                length BIGINT(15) UNSIGNED NOT NULL,
                start BIGINT(15) UNSIGNED NOT NULL,
                value FLOAT(11),
                PRIMARY KEY (namespace, hash, length, start))''')
        if 'gauged_statistics' not in tables:
            execute('''CREATE TABLE gauged_statistics (
                namespace INT(11) UNSIGNED NOT NULL,
                offset INT(11) UNSIGNED NOT NULL,
                data_points INT(11) UNSIGNED NOT NULL,
                byte_count INT(11) UNSIGNED NOT NULL,
                PRIMARY KEY (namespace, offset))''')
        if 'gauged_metadata' not in tables:
            execute('''CREATE TABLE gauged_metadata (
                `key` VARCHAR(255) NOT NULL PRIMARY KEY,
                value VARCHAR(255) NOT NULL)''')
        self.commit()

    def clear_schema(self):
        '''Clear all gauged data'''
        execute = self.cursor.execute
        execute('TRUNCATE TABLE gauged_data')
        execute('TRUNCATE TABLE gauged_keys')
        execute('TRUNCATE TABLE gauged_writer_history')
        execute('TRUNCATE TABLE gauged_cache')
        execute('TRUNCATE TABLE gauged_statistics')
        self.db.commit()

    def drop_schema(self):
        '''Drop all gauged tables'''
        execute = self.cursor.execute
        execute('DROP TABLE IF EXISTS gauged_data')
        execute('DROP TABLE IF EXISTS gauged_keys')
        execute('DROP TABLE IF EXISTS gauged_writer_history')
        execute('DROP TABLE IF EXISTS gauged_cache')
        execute('DROP TABLE IF EXISTS gauged_statistics')
        execute('DROP TABLE IF EXISTS gauged_metadata')
        self.db.commit()

    def add_namespace_statistics(self, namespace, offset, data_points, byte_count):
        '''Update namespace statistics for the period identified by
        offset'''
        self.cursor.execute('''INSERT INTO gauged_statistics VALUES (%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE data_points = data_points + VALUES(data_points),
            byte_count = byte_count + VALUES(byte_count)''',
            ( namespace, offset, data_points, byte_count ))

    def get_namespace_statistics(self, namespace, start_offset, end_offset):
        '''Get namespace statistics for the period between start_offset and
        end_offset (inclusive)'''
        cursor = self.cursor
        cursor.execute('''SELECT SUM(data_points), SUM(byte_count)
            FROM gauged_statistics WHERE namespace = %s AND offset
            BETWEEN %s AND %s''', (namespace, start_offset, end_offset))
        return [ long(count or 0) for count in cursor.fetchone() ]
