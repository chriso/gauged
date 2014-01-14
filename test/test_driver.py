'''
Gauged - https://github.com/chriso/gauged
Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
'''

from hashlib import sha1
from .test_case import TestCase

class TestDriver(TestCase):
    '''Test each available driver'''

    # Override this with an instantiated driver
    driver = None

    def setUp(self):
        self.driver.clear_schema()

    def test_insert_keys(self):
        self.driver.insert_keys([ (1, 'foo') ])
        self.driver.insert_keys([ (1, 'bar') ])
        self.driver.insert_keys([ (2, 'foobar') ])
        self.driver.insert_keys([ (3, 'Foo') ])
        ids = self.driver.lookup_ids([ (1, 'foo'), (1, 'bar'), (1, 'foobar') ])
        self.assertEqual(ids[(1, 'foo')], 1)
        self.assertEqual(ids[(1, 'bar')], 2)
        self.assertEqual(ids[(1, 'foobar')], None)
        self.assertEqual(len(ids), 3)
        ids = self.driver.lookup_ids([ (2, 'foo'), (2, 'foobar') ])
        self.assertEqual(ids[(2, 'foo')], None)
        self.assertEqual(ids[(2, 'foobar')], 3)
        self.assertEqual(len(ids), 2)
        ids = self.driver.lookup_ids([ (3, 'Foo') ])
        self.assertEqual(ids[(3, 'Foo')], 4)
        self.assertEqual(len(ids), 1)

    def test_insert_blocks(self):
        blocks = [( 0,  1, 2, 'foo', 0x10 ),
                  ( 1,  2, 3, 'bar', 0x10 )]
        self.driver.replace_blocks(blocks)
        buf, flags = self.driver.get_block(0, 1, 2)
        self.assertEqual(str(buf), 'foo')
        self.assertEqual(flags, 0x10)
        self.assertEqual(str(self.driver.get_block(1, 2, 3)[0]), 'bar')
        self.driver.replace_blocks([( 0,  1, 2, '\xe4\x00\x12', 0x10 )])
        self.assertEqual(str(self.driver.get_block(0, 1, 2)[0]), '\xe4\x00\x12')
        self.driver.insert_or_append_blocks([( 0,  1, 2, '\xe4\x00\x12', 0x10 )])
        self.assertEqual(str(self.driver.get_block(0, 1, 2)[0]), '\xe4\x00\x12\xe4\x00\x12')

    def test_keys(self):
        self.driver.insert_keys([ (1, 'bar') ])
        self.driver.insert_keys([ (1, 'foobar') ])
        self.driver.insert_keys([ (1, 'fooqux') ])
        self.driver.insert_keys([ (2, 'foo') ])
        keys = self.driver.keys(1)
        self.assertListEqual(keys, [ 'bar', 'foobar', 'fooqux' ])
        keys = self.driver.keys(1, prefix='foo')
        self.assertListEqual(keys, [ 'foobar', 'fooqux' ])
        keys = self.driver.keys(1, prefix='foo', limit=1)
        self.assertListEqual(keys, [ 'foobar' ])
        keys = self.driver.keys(1, prefix='foo', limit=1, offset=1)
        self.assertListEqual(keys, [ 'fooqux' ])
        keys = self.driver.keys(2, prefix='foo')
        self.assertListEqual(keys, [ 'foo' ])
        keys = self.driver.keys(2, prefix='bar')
        self.assertListEqual(keys, [ ])

    def test_insert_or_append(self):
        blocks = [( 0,  1, 1, 'foo', 0x10 )]
        self.driver.replace_blocks(blocks)
        blocks = [( 0,  1, 1, 'bar', 0x10 )]
        self.driver.insert_or_append_blocks(blocks)
        buf, flags = self.driver.get_block(0, 1, 1)
        self.assertEqual(str(buf), 'foobar')
        self.assertEqual(flags, 0x10)

    def test_history(self):
        self.assertEqual(self.driver.get_writer_position('foo'), 0)
        self.driver.set_writer_position('foo', 100)
        self.assertEqual(self.driver.get_writer_position('bar'), 0)
        self.assertEqual(self.driver.get_writer_position('foo'), 100)
        self.driver.set_writer_position('foo', 10)
        self.assertEqual(self.driver.get_writer_position('foo'), 10)
        self.driver.set_writer_position('bar', 1000)
        self.assertEqual(self.driver.get_writer_position('bar'), 1000)

    def test_remove_namespace(self):
        self.driver.remove_namespace(1)
        blocks = [( 0,  1, 2, 'foo', 0x10 ),
                  ( 1,  2, 3, 'bar', 0x10 )]
        self.driver.replace_blocks(blocks)
        self.assertEqual(str(self.driver.get_block(0, 1, 2)[0]), 'foo')
        self.assertEqual(str(self.driver.get_block(1, 2, 3)[0]), 'bar')
        self.driver.remove_namespace(1)
        self.assertEqual(str(self.driver.get_block(0, 1, 2)[0]), 'foo')
        self.assertEqual(self.driver.get_block(1, 2, 3)[0], None)

    def test_cache(self):
        id_ = sha1('foobar').digest()
        self.assertSequenceEqual(self.driver.get_cache(0, id_, 2, 3, 4), ())
        self.driver.add_cache(0, id_, 2, [(3, 4), (4, 6)])
        self.assertSequenceEqual(self.driver.get_cache(0, id_, 2, 3, 4),
            ((3, 4), (4, 6)))
        self.driver.add_cache(1, id_, 2, [(3, 4)])
        self.assertSequenceEqual(self.driver.get_cache(1, id_, 2, 3, 4), ((3, 4),))
        self.driver.remove_cache(0)
        self.assertSequenceEqual(self.driver.get_cache(0, id_, 2, 3, 4), ())
        self.assertSequenceEqual(self.driver.get_cache(1, id_, 2, 3, 4), ((3, 4),))

    def test_namespace_statistics(self):
        min_block, max_block = self.driver.block_offset_bounds(0)
        self.assertEqual(min_block, None)
        self.assertEqual(max_block, None)
        statistics = self.driver.get_namespace_statistics(0, 0, 0)
        self.assertListEqual(statistics, [ 0, 0 ])
        self.driver.add_namespace_statistics(0, 0, 1, 2)
        self.driver.add_namespace_statistics(0, 0, 6, 7)
        self.driver.add_namespace_statistics(0, 1, 101, 102)
        statistics = self.driver.get_namespace_statistics(0, 0, 0)
        self.assertListEqual(statistics, [ 7, 9 ])
        statistics = self.driver.get_namespace_statistics(0, 0, 1)
        self.assertListEqual(statistics, [ 108, 111 ])
        min_block, max_block = self.driver.block_offset_bounds(0)
        self.assertEqual(min_block, 0)
        self.assertEqual(max_block, 1)
        self.driver.add_namespace_statistics(1, 2, 101, 102)
        min_block, max_block = self.driver.block_offset_bounds(1)
        self.assertEqual(min_block, 2)
        self.assertEqual(max_block, 2)
        min_block, max_block = self.driver.block_offset_bounds(0)
        self.assertEqual(min_block, 0)
        self.assertEqual(max_block, 1)
        self.assertItemsEqual(self.driver.get_namespaces(), [0, 1])

    def test_clear_from(self):
        self.driver.add_namespace_statistics(0, 0, 1, 2)
        self.driver.add_namespace_statistics(1, 1, 4, 5)
        self.driver.add_namespace_statistics(1, 2, 101, 102)
        blocks = [( 0,  1, 2, 'foo', 0x10 ),
                  ( 1,  2, 3, 'bar', 0x10 )]
        self.driver.replace_blocks(blocks)
        id_ = sha1('foobar').digest()
        self.driver.add_cache(0, id_, 5, [(13, 4), (15, 4), (18, 4), (23, 6)])
        self.driver.set_writer_position('foo', 18)
        self.driver.set_writer_position('bar', 22)
        self.driver.clear_from(2, 20)
        buf, flags = self.driver.get_block(0, 1, 2)
        self.assertEqual(str(buf), 'foo')
        self.assertEqual(flags, 0x10)
        self.assertEqual(self.driver.get_block(1, 2, 3)[0], None)
        self.assertEqual(self.driver.get_writer_position('foo'), 18)
        self.assertEqual(self.driver.get_writer_position('bar'), 20)
        self.assertSequenceEqual(self.driver.get_cache(0, id_, 5, 0, 100), [(13, 4)])
        self.assertListEqual(self.driver.get_namespace_statistics(1, 0, 3), [4, 5])

    def test_metadata(self):
        self.assertEqual(self.driver.get_metadata('foobaz'), None)
        self.driver.set_metadata({ 'foo': 'bar', 'bar': 'baz' })
        self.assertEqual(self.driver.get_metadata('foo'), 'bar')
        self.assertEqual(self.driver.get_metadata('bar'), 'baz')
        self.driver.set_metadata({ 'foo': 'qux' })
        self.assertEqual(self.driver.get_metadata('foo'), 'qux')
        self.driver.set_metadata({ 'foo': 'bar', 'qux': 'foobar' }, replace=False)
        self.assertEqual(self.driver.get_metadata('foo'), 'qux')
        self.assertEqual(self.driver.get_metadata('qux'), 'foobar')
