'''
Gauged
https://github.com/chriso/gauged (MIT Licensed)
Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
'''

from .interface import DriverInterface

class PostgreSQLDriver(DriverInterface):
    '''A PostgreSQL driver for gauged'''

    MAX_KEY = 255

    def __init__(self, **kwargs):
        try:
            self.psycopg2 = __import__('psycopg2')
        except ImportError:
            raise ImportError('The psycopg2 library is required')
        self.db = self.psycopg2.connect(**kwargs)
        self.cursor = self.db.cursor()
        self.bulk_insert = 1000

    def keys(self, namespace, prefix=None, limit=None, offset=None):
        '''Get keys from a namespace'''
        params = [ namespace ]
        query = '''SELECT key FROM gauged_keys
            WHERE namespace = %s'''
        if prefix is not None:
            query += ' AND key LIKE %s'
            params.append(prefix + '%')
        if limit is not None:
            query += ' LIMIT %s'
            params.append(limit)
        if offset is not None:
            query += ' OFFSET %s'
            params.append(offset)
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
        query = 'SELECT namespace, key, id FROM gauged_keys WHERE '
        check = '(namespace = %s AND key = %s) '
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
        '''Get the block identified by namespace, offset, key and
        value'''
        cursor = self.cursor
        cursor.execute('''SELECT data, flags FROM gauged_data
            WHERE namespace = %s AND "offset" = %s AND key = %s''',
            ( namespace, offset, key ))
        row = cursor.fetchone()
        return ( None, None ) if row is None else row

    def insert_keys(self, keys):
        '''Insert keys into a table which assigns an ID'''
        start = 0
        bulk_insert = self.bulk_insert
        keys_len = len(keys)
        query = 'INSERT INTO gauged_keys (namespace, key) VALUES '
        execute = self.cursor.execute
        while start < keys_len:
            rows = keys[start:start+bulk_insert]
            params = [ param for params in rows for param in params ]
            insert = '(%s,%s),' * (len(rows) - 1) + '(%s,%s)'
            execute(query + insert, params)
            start += bulk_insert

    def replace_blocks(self, blocks):
        '''Replace multiple blocks. blocks must be a list of tuples where
        each tuple consists of (namespace, offset, key, data)'''
        start = 0
        execute = self.cursor.execute
        query = '''DELETE FROM gauged_data WHERE namespace = %s AND
            "offset" = %s AND key = %s'''
        for namespace, offset, key, _, _ in blocks:
            execute(query, (namespace, offset, key))
        bulk_insert = self.bulk_insert
        blocks_len = len(blocks)
        row = '(%s,%s,%s,%s,%s)'
        query = '''INSERT INTO gauged_data
            (namespace, "offset", key, data, flags) VALUES '''
        binary = self.psycopg2.Binary
        while start < blocks_len:
            rows = blocks[start:start+bulk_insert]
            params = []
            for namespace, offset, key, data, flags in rows:
                params.extend(( namespace, offset, key, binary(data), flags ))
            insert = (row + ',') * (len(rows) - 1) + row
            execute(query + insert, params)
            start += bulk_insert

    def insert_or_append_blocks(self, blocks):
        '''Insert multiple blocks. If a block already exists, the data is
        appended. blocks must be a list of tuples where each tuple consists
        of (namespace, offset, key, data)'''
        binary = self.psycopg2.Binary
        execute = self.cursor.execute
        query = '''UPDATE gauged_data SET data = data || %s, flags = %s
            WHERE namespace = %s AND "offset" = %s AND key = %s;
            INSERT INTO gauged_data (data, flags, namespace, "offset", key)
            SELECT %s, %s, %s, %s, %s WHERE NOT EXISTS (
            SELECT 1 FROM gauged_data WHERE namespace = %s AND "offset" = %s
            AND key = %s)'''
        for namespace, offset, key, data, flags in blocks:
            data = binary(data)
            execute(query, (data, flags, namespace, offset, key, data, flags,
                namespace, offset, key, namespace, offset, key))

    def block_offset_bounds(self, namespace):
        '''Get the minimum and maximum block offset for the specified namespace'''
        cursor = self.cursor
        cursor.execute('''SELECT MIN("offset"), MAX("offset")
            FROM gauged_statistics WHERE namespace = %s''', (namespace,))
        return cursor.fetchone()

    def set_metadata(self, metadata, replace=True):
        execute = self.cursor.execute
        if replace:
            query = 'DELETE FROM gauged_metadata WHERE key IN (%s'
            query += ',%s' * (len(metadata) - 1) + ')'
            execute(query, metadata.keys())
        params = [ param for params in metadata.iteritems() for param in params ]
        query = 'INSERT INTO gauged_metadata VALUES (%s,%s)'
        query += ',(%s,%s)' * (len(metadata) - 1)
        execute(query, params)
        self.db.commit()

    def get_metadata(self, key):
        cursor = self.cursor
        cursor.execute('SELECT value FROM gauged_metadata WHERE key = %s', (key,))
        result = cursor.fetchone()
        return result[0] if result else None

    def all_metadata(self):
        cursor = self.cursor
        cursor.execute('SELECT * FROM gauged_metadata')
        return dict(( row for row in cursor ))

    def set_writer_position(self, name, timestamp):
        '''Insert a timestamp to keep track of the current writer position'''
        execute = self.cursor.execute
        execute('DELETE FROM gauged_writer_history WHERE id = %s', (name,))
        execute('''INSERT INTO gauged_writer_history (id, timestamp)
            VALUES (%s, %s)''', (name, timestamp,))

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
        params = (namespace, )
        execute = self.cursor.execute
        execute('DELETE FROM gauged_data WHERE namespace = %s', params)
        execute('DELETE FROM gauged_statistics WHERE namespace = %s', params)
        execute('DELETE FROM gauged_keys WHERE namespace = %s', params)
        self.remove_cache(namespace)

    def clear_from(self, offset, timestamp):
        params = (offset, )
        execute = self.cursor.execute
        execute('DELETE FROM gauged_data WHERE "offset" >= %s', params)
        execute('DELETE FROM gauged_statistics WHERE "offset" >= %s', params)
        execute('DELETE FROM gauged_cache WHERE start + length >= %s',
            (timestamp,))
        execute('''UPDATE gauged_writer_history SET timestamp = %s
            WHERE timestamp > %s''', (timestamp, timestamp))

    def get_cache(self, namespace, query_hash, length, start, end):
        '''Get a cached value for the specified date range and query'''
        query_hash = self.psycopg2.Binary(query_hash)
        cursor = self.cursor
        cursor.execute('''SELECT start, value FROM gauged_cache WHERE namespace = %s
            AND "hash" = %s AND length = %s AND start BETWEEN %s AND %s''',
            (namespace, query_hash, length, start, end))
        return cursor.fetchall()

    def add_cache(self, namespace, query_hash, length, cache):
        '''Add cached values for the specified date range and query'''
        start = 0
        bulk_insert = self.bulk_insert
        cache_len = len(cache)
        row = '(%s,%s,%s,%s,%s)'
        query = '''INSERT INTO gauged_cache
            (namespace, "hash", length, start, value) VALUES '''
        execute = self.cursor.execute
        query_hash = self.psycopg2.Binary(query_hash)
        while start < cache_len:
            rows = cache[start:start+bulk_insert]
            params = []
            for timestamp, value in rows:
                params.extend(( namespace, query_hash, length, timestamp, value ))
            insert = (row + ',') * (len(rows) - 1) + row
            execute(query + insert, params)
            start += bulk_insert
        self.db.commit()

    def remove_cache(self, namespace):
        '''Remove all cached values for the specified namespace'''
        self.cursor.execute('DELETE FROM gauged_cache WHERE namespace = %s', (namespace,))

    def commit(self):
        '''Commit the current transaction'''
        self.db.commit()

    def create_schema(self):
        '''Create all necessary tables'''
        execute = self.cursor.execute
        try:
            return execute('SELECT 1 FROM gauged_statistics')
        except self.psycopg2.ProgrammingError:
            pass
        self.db.rollback()
        execute('''CREATE TABLE IF NOT EXISTS gauged_data (
                namespace integer NOT NULL,
                "offset" integer NOT NULL,
                key bigint NOT NULL,
                data bytea NOT NULL,
                flags integer NOT NULL,
                PRIMARY KEY ("offset", namespace, key));
            CREATE TABLE IF NOT EXISTS gauged_keys (
                id serial PRIMARY KEY,
                namespace integer NOT NULL,
                key varchar NOT NULL);
            CREATE UNIQUE INDEX ON gauged_keys (
                namespace, key);
            CREATE OR REPLACE RULE gauged_ignore_duplicate_keys
                AS ON INSERT TO gauged_keys WHERE EXISTS (
                SELECT 1 FROM gauged_keys WHERE key = NEW.key
                    AND namespace = NEW.namespace)
                DO INSTEAD NOTHING;
            CREATE TABLE IF NOT EXISTS gauged_writer_history (
                id varchar PRIMARY KEY,
                timestamp bigint NOT NULL);
            CREATE TABLE IF NOT EXISTS gauged_cache (
                namespace integer NOT NULL,
                "hash" bytea NOT NULL,
                length bigint NOT NULL,
                start bigint NOT NULL,
                value real,
                PRIMARY KEY(namespace, hash, length, start));
            CREATE OR REPLACE RULE gauged_ignore_duplicate_cache
                AS ON INSERT TO gauged_cache WHERE EXISTS (
                SELECT 1 FROM gauged_cache WHERE namespace = NEW.namespace AND
                "hash" = NEW.hash AND length = NEW.length AND start = NEW.start)
                DO INSTEAD NOTHING;
            CREATE TABLE IF NOT EXISTS gauged_statistics (
                namespace integer NOT NULL,
                "offset" integer NOT NULL,
                data_points integer NOT NULL,
                byte_count integer NOT NULL,
                PRIMARY KEY (namespace, "offset"));
            CREATE TABLE IF NOT EXISTS gauged_metadata (
                key varchar PRIMARY KEY,
                value varchar NOT NULL);
            CREATE OR REPLACE RULE gauged_ignore_duplicate_metadata
                AS ON INSERT TO gauged_metadata WHERE EXISTS (
                SELECT 1 FROM gauged_metadata WHERE key = NEW.key)
                DO INSTEAD NOTHING''')
        self.db.commit()

    def clear_schema(self):
        '''Clear all gauged data'''
        execute = self.cursor.execute
        execute('''TRUNCATE gauged_data;
            TRUNCATE gauged_keys RESTART IDENTITY;
            TRUNCATE gauged_writer_history;
            TRUNCATE gauged_cache;
            TRUNCATE gauged_statistics''')
        self.db.commit()

    def drop_schema(self):
        '''Drop all gauged tables'''
        try:
            self.cursor.execute('''
                DROP TABLE IF EXISTS gauged_data;
                DROP TABLE IF EXISTS gauged_keys;
                DROP TABLE IF EXISTS gauged_writer_history;
                DROP TABLE IF EXISTS gauged_cache;
                DROP TABLE IF EXISTS gauged_statistics;
                DROP TABLE IF EXISTS gauged_metadata''')
            self.db.commit()
        except self.psycopg2.InternalError: # pragma: no cover
            self.db.rollback()

    def add_namespace_statistics(self, namespace, offset, data_points, byte_count):
        '''Update namespace statistics for the period identified by
        offset'''
        query = '''UPDATE gauged_statistics SET data_points = data_points + %s,
            byte_count = byte_count + %s WHERE namespace = %s AND "offset" = %s;
            INSERT INTO gauged_statistics SELECT %s, %s, %s, %s WHERE NOT EXISTS (
            SELECT 1 FROM gauged_statistics WHERE namespace = %s AND "offset" = %s)'''
        self.cursor.execute(query, (data_points, byte_count, namespace,
            offset, namespace, offset, data_points, byte_count, namespace, offset))

    def get_namespace_statistics(self, namespace, start_offset, end_offset):
        '''Get namespace statistics for the period between start_offset and
        end_offset (inclusive)'''
        cursor = self.cursor
        cursor.execute('''SELECT SUM(data_points), SUM(byte_count)
            FROM gauged_statistics WHERE namespace = %s AND "offset"
            BETWEEN %s AND %s''', (namespace, start_offset, end_offset))
        return [ long(count or 0) for count in cursor.fetchone() ]
