'''
Gauged
https://github.com/chriso/gauged (MIT Licensed)
Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
'''

from .interface import DriverInterface

class SQLiteDriver(DriverInterface):


    MAX_KEY = 255

    MEMORY = 'sqlite://'

    def __init__(self, database, bulk_insert=125):
        try:
            sqlite = __import__('sqlite3')
        except ImportError:
            raise ImportError('The sqlite3 library is required')
        self.db = sqlite.connect(database, check_same_thread=False)
        self.db.text_factory = str
        self.bulk_insert = bulk_insert
        self.cursor = self.db.cursor()

    def keys(self, namespace, prefix=None, limit=None, offset=None):
        '''Get keys from a namespace'''
        params = [ namespace ]
        query = '''SELECT `key` FROM gauged_keys
            WHERE namespace = ?'''
        if prefix is not None:
            query += ' AND `key` LIKE ?'
            params.append(prefix + '%')
        if limit is not None:
            query += ' LIMIT '
            if offset is not None:
                query += '?, '
                params.append(offset)
            query += '?'
            params.append(limit)
        cursor = self.cursor
        cursor.execute(query, params)
        return [ key for key, in cursor ]

    def lookup_ids(self, keys):
        '''Lookup the integer ID associated with each (namespace, key) in the
        keys list'''
        keys_len = len(keys)
        ids = { namespace_key: None for namespace_key in keys }
        start = 0
        bulk_insert = self.bulk_insert
        query = 'SELECT namespace, `key`, id FROM gauged_keys WHERE '
        check = '(namespace = ? AND `key` = ?) '
        cursor = self.cursor
        execute = cursor.execute
        while start < keys_len:
            rows = keys[start:start+bulk_insert]
            params = [ param for params in rows for param in params ]
            id_query = query + (check + ' OR ') * (len(rows) - 1) + check
            execute(id_query, params)
            for namespace, key, id_ in cursor:
                ids[( namespace, key )] = id_
            start += bulk_insert
        return ids

    def get_block(self, namespace, offset, key):
        '''Get the block identified by namespace, offset and key'''
        cursor = self.cursor
        cursor.execute('''SELECT data, flags FROM gauged_data
            WHERE namespace = ? AND offset = ? AND `key` = ?''',
            ( namespace, offset, key ))
        row = cursor.fetchone()
        return ( None, None ) if row is None else row

    def insert_keys(self, keys):
        '''Insert keys into a table which assigns an ID'''
        start = 0
        bulk_insert = self.bulk_insert
        keys_len = len(keys)
        select = 'SELECT ?,?'
        query = 'INSERT OR IGNORE INTO gauged_keys (namespace, `key`) '
        execute = self.cursor.execute
        while start < keys_len:
            rows = keys[start:start+bulk_insert]
            params = [ param for params in rows for param in params ]
            insert = (select + ' UNION ') * (len(rows) - 1) + select
            execute(query + insert, params)
            start += bulk_insert

    def replace_blocks(self, blocks):
        '''Replace multiple blocks. blocks must be a list of tuples where
        each tuple consists of (namespace, offset, key, data, flags)'''
        start = 0
        bulk_insert = self.bulk_insert
        blocks_len = len(blocks)
        select = 'SELECT ?,?,?,?,?'
        query = 'REPLACE INTO gauged_data (namespace, offset, `key`, data, flags) '
        execute = self.cursor.execute
        while start < blocks_len:
            rows = blocks[start:start+bulk_insert]
            params = [ param for params in rows for param in params ]
            insert = (select + ' UNION ') * (len(rows) - 1) + select
            execute(query + insert, params)
            start += bulk_insert

    def insert_or_append_blocks(self, blocks):
        '''Insert multiple blocks. If a block already exists, the data is
        appended. blocks must be a list of tuples where each tuple consists
        of (namespace, offset, key, data)'''
        start = 0
        bulk_insert = self.bulk_insert
        blocks_len = len(blocks)
        select = 'SELECT ?,?,?,"",0'
        query = 'INSERT OR IGNORE INTO gauged_data (namespace, offset, `key`, data, flags) '
        execute = self.cursor.execute
        while start < blocks_len:
            rows = blocks[start:start+bulk_insert]
            params = []
            for namespace, offset, key, _, _ in rows:
                params.extend(( namespace, offset, key ))
            insert = (select + ' UNION ') * (len(rows) - 1) + select
            execute(query + insert, params)
            start += bulk_insert
        for namespace, offset, key, data, flags in blocks:
            execute('''UPDATE gauged_data SET data = CAST(data || ? AS BLOB)
                , flags = ? WHERE namespace = ? AND offset = ? AND
                `key` = ?''', ( data, flags, namespace, offset, key ))

    def block_offset_bounds(self, namespace):
        '''Get the minimum and maximum block offset for the specified namespace'''
        cursor = self.cursor
        cursor.execute('''SELECT MIN(offset), MAX(offset) FROM gauged_statistics
            WHERE namespace = ?''', (namespace,))
        return cursor.fetchone()

    def set_metadata(self, metadata, replace=True):
        params = [ param for params in metadata.iteritems() for param in params ]
        query = 'REPLACE' if replace else 'INSERT OR IGNORE'
        query += ' INTO gauged_metadata SELECT ?,?'
        query += ' UNION SELECT ?,?' * (len(metadata) - 1)
        self.cursor.execute(query, params)
        self.db.commit()

    def get_metadata(self, key):
        cursor = self.cursor
        cursor.execute('SELECT value FROM gauged_metadata WHERE `key` = ?', (key,))
        result = cursor.fetchone()
        return result[0] if result else None

    def all_metadata(self):
        cursor = self.cursor
        cursor.execute('SELECT * FROM gauged_metadata')
        return dict(( row for row in cursor ))

    def set_writer_position(self, name, timestamp):
        '''Insert a timestamp to keep track of the current writer position'''
        self.cursor.execute('''REPLACE INTO gauged_writer_history (id, timestamp)
            VALUES (?, ?)''', (name, timestamp))

    def get_writer_position(self, name):
        '''Get the current writer position'''
        cursor = self.cursor
        cursor.execute('''SELECT timestamp FROM gauged_writer_history
            WHERE id = ?''', (name,))
        result = cursor.fetchone()
        return result[0] if result else 0

    def get_namespaces(self):
        '''Get a list of namespaces'''
        cursor = self.cursor
        cursor.execute('''SELECT DISTINCT namespace FROM gauged_statistics''')
        return [ namespace for namespace, in cursor ]

    def remove_namespace(self, namespace):
        '''Remove all data associated with the current namespace'''
        params = (namespace, )
        execute = self.cursor.execute
        execute('DELETE FROM gauged_data WHERE namespace = ?', params)
        execute('DELETE FROM gauged_statistics WHERE namespace = ?', params)
        execute('DELETE FROM gauged_keys WHERE namespace = ?', params)
        self.remove_cache(namespace)

    def clear_from(self, offset, timestamp):
        params = (offset, )
        execute = self.cursor.execute
        execute('''DELETE FROM gauged_data WHERE offset >= ?''', params)
        execute('''DELETE FROM gauged_statistics WHERE offset >= ? ''', params)
        execute('''DELETE FROM gauged_cache WHERE start + length >= ?''', (timestamp,))
        execute('''UPDATE gauged_writer_history SET timestamp = ?
            WHERE timestamp > ?''', (timestamp, timestamp))

    def get_cache(self, namespace, query_hash, length, start, end):
        '''Get a cached value for the specified date range and query'''
        query = '''SELECT start, value FROM gauged_cache WHERE namespace = ?
            AND hash = ? AND length = ? AND start BETWEEN ? AND ?'''
        cursor = self.cursor
        cursor.execute(query, (namespace, query_hash, length, start, end))
        return tuple(cursor.fetchall())

    def add_cache(self, namespace, query_hash, length, cache):
        '''Add cached values for the specified date range and query'''
        start = 0
        bulk_insert = self.bulk_insert
        cache_len = len(cache)
        select = 'SELECT ?, ?, ?, ?, ?'
        query = '''INSERT OR IGNORE INTO gauged_cache
            (namespace, hash, length, start, value) '''
        execute = self.cursor.execute
        while start < cache_len:
            rows = cache[start:start+bulk_insert]
            params = []
            for timestamp, value in rows:
                params.extend(( namespace, query_hash, length, timestamp, value ))
            insert = (select + ' UNION ') * (len(rows) - 1) + select
            execute(query + insert, params)
            start += bulk_insert
        self.db.commit()

    def remove_cache(self, namespace):
        '''Remove all cached values for the specified namespace'''
        self.cursor.execute('DELETE FROM gauged_cache WHERE namespace = ?', (namespace,))

    def commit(self):
        '''Commit the current transaction'''
        self.db.commit()

    def create_schema(self):
        '''Create all necessary tables'''
        self.cursor.executescript('''
            CREATE TABLE IF NOT EXISTS gauged_data (
                namespace UNSIGNED INT NOT NULL,
                offset UNSIGNED INT NOT NULL,
                `key` INTEGER NOT NULL,
                data BLOB,
                flags UNSIGNED INT NOT NULL,
                PRIMARY KEY (offset, namespace, `key`));
            CREATE TABLE IF NOT EXISTS gauged_keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                namespace UNSIGNED INT NOT NULL,
                `key` VARCHAR NOT NULL);
            CREATE UNIQUE INDEX IF NOT EXISTS gauged_namespace_key ON gauged_keys
                (namespace, `key`);
            CREATE TABLE IF NOT EXISTS gauged_writer_history (
                id VARCHAR NOT NULL PRIMARY KEY,
                timestamp UNSIGNED BIGINT NOT NULL);
            CREATE TABLE IF NOT EXISTS gauged_cache (
                namespace UNSIGNED INT NOT NULL,
                hash CHAR(20) NOT NULL,
                length UNSIGNED BIGINT NOT NULL,
                start UNSIGNED BIGINT NOT NULL,
                value FLOAT,
                PRIMARY KEY (namespace, hash, length, start));
            CREATE TABLE IF NOT EXISTS gauged_statistics (
                namespace UNSIGNED INT NOT NULL,
                offset UNSIGNED INT NOT NULL,
                data_points UNSIGNED INT NOT NULL,
                byte_count INUNSIGNED INT NOT NULL,
                PRIMARY KEY (namespace, offset));
            CREATE TABLE IF NOT EXISTS gauged_metadata (
                `key` VARCHAR NOT NULL PRIMARY KEY,
                value VARCHAR NOT NULL)''')
        self.db.commit()

    def clear_schema(self):
        '''Clear all gauged data'''
        self.cursor.executescript('''
            DELETE FROM gauged_data;
            DELETE FROM gauged_keys;
            DELETE FROM gauged_writer_history;
            DELETE FROM gauged_cache;
            DELETE FROM gauged_statistics;
            DELETE FROM sqlite_sequence WHERE name = "gauged_keys"''')
        self.db.commit()

    def drop_schema(self):
        '''Drop all gauged tables'''
        self.cursor.executescript('''
            DROP TABLE IF EXISTS gauged_data;
            DROP TABLE IF EXISTS gauged_keys;
            DROP TABLE IF EXISTS gauged_writer_history;
            DROP TABLE IF EXISTS gauged_cache;
            DROP TABLE IF EXISTS gauged_statistics;
            DROP TABLE IF EXISTS gauged_metadata''')
        self.db.commit()

    def add_namespace_statistics(self, namespace, offset, data_points, byte_count):
        '''Update namespace statistics for the period identified by
        offset'''
        execute = self.cursor.execute
        execute('''INSERT OR IGNORE INTO gauged_statistics
            VALUES (?, ?, 0, 0)''', ( namespace, offset ))
        execute('''UPDATE gauged_statistics SET data_points = data_points + ?,
            byte_count = byte_count + ? WHERE namespace = ? AND offset = ?''',
            ( data_points, byte_count, namespace, offset ))

    def get_namespace_statistics(self, namespace, start_offset, end_offset):
        '''Get namespace statistics for the period between start_offset and
        end_offset (inclusive)'''
        cursor = self.cursor
        cursor.execute('''SELECT SUM(data_points), SUM(byte_count)
            FROM gauged_statistics WHERE namespace = ? AND offset
            BETWEEN ? AND ?''', (namespace, start_offset, end_offset))
        return [ long(count or 0) for count in cursor.fetchone() ]
