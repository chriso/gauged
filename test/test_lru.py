'''
Gauged - https://github.com/chriso/gauged
Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
'''

from gauged import LRU
from .test_case import TestCase

class TestLRU(TestCase):
    '''Test the least recently used cache in lru.py'''

    def test_lru_eviction(self):
        lru = LRU(3)
        lru[1] = 'foo'
        lru[2] = 'bar'
        lru[3] = 'foobar'
        self.assertTrue(3 in lru)
        self.assertTrue(2 in lru)
        self.assertTrue(1 in lru)
        self.assertEqual(lru[3], 'foobar')
        self.assertEqual(lru[2], 'bar')
        self.assertEqual(lru[1], 'foo')
        lru[4] = 'bla'
        self.assertTrue(4 in lru)
        self.assertTrue(1 in lru)
        self.assertTrue(2 in lru)
        self.assertFalse(3 in lru)

    def test_len_1(self):
        lru = LRU(1)
        lru[1] = 'foo'
        self.assertEqual(lru[1], 'foo')
        self.assertTrue(1 in lru)
        lru[2] = 'bar'
        self.assertEqual(lru[2], 'bar')
        self.assertFalse(1 in lru)
