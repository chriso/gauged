'''
Gauged
https://github.com/chriso/gauged (MIT Licensed)
Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
'''

class DriverInterface(object):

    MAX_KEY = 1024

    def create_schema(self):
        raise NotImplementedError

    def clear_schema(self):
        raise NotImplementedError

    def drop_schema(self):
        raise NotImplementedError

    def keys(self, namespace, prefix=None, limit=None, offset=None):
        raise NotImplementedError

    def lookup_ids(self, keys):
        raise NotImplementedError

    def get_block(self, namespace, offset, key):
        raise NotImplementedError

    def insert_keys(self, keys):
        raise NotImplementedError

    def replace_blocks(self, blocks):
        raise NotImplementedError

    def insert_or_append_blocks(self, blocks):
        raise NotImplementedError

    def commit(self):
        raise NotImplementedError

    def block_offset_bounds(self, namespace):
        raise NotImplementedError

    def set_metadata(self, metadata, replace=True):
        raise NotImplementedError

    def get_metadata(self, key):
        raise NotImplementedError

    def set_writer_position(self, name, timestamp):
        raise NotImplementedError

    def get_writer_position(self, name):
        raise NotImplementedError

    def get_namespaces(self):
        raise NotImplementedError

    def remove_namespace(self, namespace):
        raise NotImplementedError

    def clear_from(self, offset, timestamp):
        raise NotImplementedError

    def get_cache(self, namespace, query_hash, length, start, end):
        pass

    def add_cache(self, namespace, query_hash, length, cache):
        pass

    def remove_cache(self, namespace):
        pass

    def add_namespace_statistics(self, namespace, offset,
            data_points, byte_count):
        raise NotImplementedError

    def get_namespace_statistics(self, namespace, start_offset, end_offset):
        raise NotImplementedError
