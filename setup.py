'''
Gauged - https://github.com/chriso/gauged
Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
'''

import re
from distutils.core import setup, Extension

cflags = [ '-O3', '-std=c99', '-pedantic', '-Wall', '-Wextra', '-pthread' ]

gauged = Extension('_gauged',
    sources=['lib/%s.c' % src for src in ('array', 'hash', 'sort', 'map', 'writer')],
    include_dirs=['include'],
    extra_compile_args=cflags)

with open('gauged/version.py', 'r') as handle:
    version = re.search(r'__version__ = \'([^\']+)\'', handle.read(), re.M).group(1)

setup(
    name = 'gauged',
    version = version,
    author = 'Chris O\'Hara',
    author_email = 'cohara87@gmail.com',
    description = 'A fast, append-only storage layer for numeric data that changes over time',
    license = 'MIT',
    url = 'https://github.com/chriso/gauged',
    ext_modules = [gauged],
    packages = ['gauged', 'gauged.errors', 'gauged.results',
        'gauged.drivers','gauged.structures']
)
