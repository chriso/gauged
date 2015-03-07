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

    def prepare_migrations(self):
        raise NotImplementedError

    def migrate(self, goto_version, debug=False):
        migrations = self.prepare_migrations()
        if not migrations:
            return "migrations not implemented"
        if goto_version is None:
            return "version to migrate to not specified"
        if goto_version not in migrations:
            return "%s not found on allowed migrations" % goto_version
        current_meta = self.all_metadata()
        current_version = current_meta['initial_version']
        if current_version > goto_version:
            return "You have a newer schema than the current version"
        migrate = False
        for version, upgrade_script in migrations.iteritems():
            if debug:
                print '-'*20
                print 'from/to version', "%s/%s" % (current_version, version)
                print 'upgrade', upgrade_script
            # skip until current version
            if version <= current_version:
                if debug:
                    print 'skipping because already done'
                continue
            elif version > goto_version:
                if debug:
                    print 'skipping because not needed'
                break
            # if migration is found but empty, skip to next
            if not upgrade_script:
                if debug:
                    print 'skipping because migration empty'
                current_version = version
                continue
            # if migration is found, needs to be executed
            success = False
            try:
                if debug:
                    print 'executing %s' % upgrade_script
                self.cursor.executescript(upgrade_script)
                self.db.commit()
            except:
                self.db.rollback()
                if debug:
                    print 'failed to execute %s' % upgrade_script
                return 'failure migrating to %s' % version
            if debug:
                print 'successfully migrated to %s' % version
            self.set_metadata({'initial_version': version, 'current_version': version})
            current_version = version


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

    def clear_key_after(self, key, namespace, timestamp):
        raise NotImplementedError

    def clear_key_before(self, key, namespace, timestamp):
        raise NotImplementedError

    def get_cache(self, namespace, query_hash, length, start, end):
        pass

    def add_cache(self, namespace, key, query_hash, length, cache):
        pass

    def remove_cache(self, namespace, key):
        pass

    def add_namespace_statistics(self, namespace, offset,
            data_points, byte_count):
        raise NotImplementedError

    def get_namespace_statistics(self, namespace, start_offset, end_offset):
        raise NotImplementedError
