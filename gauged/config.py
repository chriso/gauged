'''
Gauged
https://github.com/chriso/gauged (MIT Licensed)
Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
'''

from .writer import Writer
from .utilities import to_bytes, Time

DEFAULTS = {
    'namespace': 0,
    'block_size': Time.DAY,
    'resolution': Time.SECOND,
    'writer_name': 'default',
    'overwrite_blocks': False,
    'key_overflow': Writer.ERROR,
    'key_whitelist': None,
    'flush_seconds': 0,
    'append_only_violation': Writer.ERROR,
    'gauge_nan': Writer.ERROR,
    'key_cache_size': 64 * 1024,
    'max_interval_steps': 31 * 24,
    'min_cache_interval': Time.HOUR,
    'max_look_behind': Time.WEEK,
    'defaults': {
        'namespace': None,
        'limit': 10,
        'offset': None,
        'prefix': None,
        'start': None,
        'end': None,
        'interval': Time.DAY,
        'cache': True,
        'key': None,
        'aggregate': None,
        'percentile': 50
    }
}

class Config(object):

    def __init__(self, **kwargs):
        self.block_arrays = None
        self.defaults = None
        self.key_whitelist = None
        self.block_size = None
        self.resolution = None
        self.update(**kwargs)

    def update(self, **kwargs):
        for key in kwargs.iterkeys():
            if key not in DEFAULTS:
                raise ValueError('Unknown configuration key: ' + key)
        for key, default in DEFAULTS.iteritems():
            if key == 'defaults':
                defaults = DEFAULTS['defaults'].copy()
                if 'defaults' in kwargs:
                    for key, value in kwargs['defaults'].iteritems():
                        if key not in defaults:
                            raise ValueError('Unknown default key: ' + key)
                        defaults[key] = value
                self.defaults = defaults
            else:
                setattr(self, key, kwargs.get(key, default))
        if self.block_size % self.resolution != 0:
            raise ValueError('`block_size` must be a multiple of `resolution`')
        self.block_arrays = self.block_size // self.resolution
        if self.key_whitelist is not None:
            self.key_whitelist = set(( to_bytes(key) for key in self.key_whitelist ))
