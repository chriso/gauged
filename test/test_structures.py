'''
Gauged - https://github.com/chriso/gauged
Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
'''

from gauged.structures import FloatArray, SparseMap
from gauged.errors import GaugedUseAfterFreeError
from .test_case import TestCase

class TestStructures(TestCase):
    '''Test the FloatArray structure'''

    def setUp(self):
        SparseMap.ALLOCATIONS = 0
        FloatArray.ALLOCATIONS = 0

    def tearDown(self):
        self.assertEqual(SparseMap.ALLOCATIONS, 0)
        self.assertEqual(FloatArray.ALLOCATIONS, 0)

    def test_array_empty_array(self):
        s = FloatArray()
        self.assertEqual(s.byte_length(), 0)
        self.assertEqual(len(s), 0)
        self.assertListEqual(s.values(), [])
        s.free()

    def test_array_instantiation_from_list(self):
        s = FloatArray([1, 2, 3])
        self.assertEqual(s.byte_length(), 12)
        self.assertEqual(len(s), 3)
        self.assertListEqual(s.values(), [1, 2, 3])
        s.free()

    def test_array_append(self):
        s = FloatArray([1, 2, 3])
        s.append(4)
        s.append(10)
        self.assertEqual(s.byte_length(), 20)
        self.assertListEqual(s.values(), [1, 2, 3, 4, 10])
        s.free()

    def test_array_clear(self):
        s = FloatArray([1, 2, 3])
        self.assertEqual(s.byte_length(), 12)
        s.clear()
        self.assertEqual(s.byte_length(), 0)
        s.free()

    def test_array_getitem(self):
        s = FloatArray([1, 2, 3])
        self.assertEqual(s[0], 1)
        self.assertEqual(s[1], 2)
        self.assertEqual(s[2], 3)
        with self.assertRaises(IndexError):
            self.assertEqual(s[3], 0)
        s.free()

    def test_array_import_export(self):
        s = FloatArray([1, 2, 3])
        buf = str(s.buffer())
        s.free()
        self.assertEqual(len(buf), 12)
        copy = FloatArray(buf, 12)
        self.assertListEqual(copy.values(), [1, 2, 3])
        copy.free()
        s = FloatArray()
        self.assertEqual(s.buffer(), None)
        s.free()

    def test_array_import_export_using_buffer(self):
        s = FloatArray([1, 2, 3])
        buf = buffer(s.buffer())
        s.free()
        self.assertEqual(len(buf), 12)
        copy = FloatArray(buf, 12)
        self.assertListEqual(copy.values(), [1, 2, 3])
        copy.free()
        s = FloatArray()
        self.assertEqual(s.buffer(), None)
        s.free()

    def test_array_buffer_offset(self):
        s = FloatArray([1, 2, 3])
        buf = s.buffer(byte_offset=4)
        s.free()
        self.assertEqual(len(buf), 8)
        copy = FloatArray(buf, 8)
        self.assertListEqual(copy.values(), [2, 3])
        copy.free()

    def test_map_empty_map(self):
        v = SparseMap()
        self.assertDictEqual(dict(v.items()), { })
        self.assertEqual(v.byte_length(), 0)
        v.free()

    def test_map_map_from_dict(self):
        a = FloatArray([ 1, 2, 3, 4 ])
        b = FloatArray([ 2, 4, 6, 8 ])
        v = SparseMap({ 1: a, 3: b })
        self.assertDictEqual(dict(v.items()), { 1: [1, 2, 3, 4], 3: [2, 4, 6, 8] })
        v.free()
        a.free()
        b.free()

    def test_map_append(self):
        v = SparseMap()
        s = FloatArray([1, 2, 3])
        v.append(1, s)
        s.free()
        self.assertDictEqual(dict(v.items()), { 1: [1, 2, 3] })
        v.free()

    def test_map_import_export(self):
        a = FloatArray([ 1, 2, 3, 4 ])
        b = FloatArray([ 2, 4, 6, 8 ])
        v = SparseMap({ 1: a, 3: b })
        buf = v.buffer()
        a.free()
        b.free()
        v.free()
        self.assertEqual(len(buf), 40)
        copy = SparseMap(buf, len(buf))
        self.assertDictEqual(dict(copy.items()), { 1: [1, 2, 3, 4], 3: [2, 4, 6, 8] })
        copy.free()
        v = SparseMap()
        self.assertEqual(v.buffer(), None)
        v.free()

    def test_map_clear(self):
        a = FloatArray([ 1, 2, 3, 4 ])
        b = FloatArray([ 2, 4, 6, 8 ])
        v = SparseMap({ 1: a, 3: b })
        a.free()
        b.free()
        v.clear()
        self.assertEqual(v.byte_length(), 0)
        v.free()

    def test_map_buffer_offset(self):
        a = FloatArray([ 1 ])
        b = FloatArray([ 1, 2, 3 ])
        v = SparseMap({ 1: a, 3: b })
        buf = v.buffer(byte_offset=8)
        a.free()
        b.free()
        v.free()
        copy = SparseMap(buf, len(buf))
        self.assertDictEqual(dict(copy.items()), { 3: [1, 2, 3] })
        copy.free()

    def test_context_manager_free(self):
        outside = None
        with FloatArray() as structure:
            outside = structure
            self.assertIsNot(structure.ptr, None)
        self.assertIs(outside._ptr, None)
        with SparseMap() as structure:
            outside = structure
            self.assertIsNot(structure.ptr, None)
        self.assertIs(outside._ptr, None)

    def test_use_array_after_free_error(self):
        s = FloatArray()
        s.free()
        with self.assertRaises(GaugedUseAfterFreeError):
            len(s)
        with self.assertRaises(GaugedUseAfterFreeError):
            s.values()
        with self.assertRaises(GaugedUseAfterFreeError):
            s.values()
        with self.assertRaises(GaugedUseAfterFreeError):
            s.append(1)
        with self.assertRaises(GaugedUseAfterFreeError):
            s.byte_length()
        with self.assertRaises(GaugedUseAfterFreeError):
            s.buffer()
        with self.assertRaises(GaugedUseAfterFreeError):
            s.clear()

    def test_use_map_after_free_error(self):
        s = SparseMap()
        s.free()
        freed_array = FloatArray()
        freed_array.free()
        tmp_array = FloatArray()
        tmp = SparseMap()
        with self.assertRaises(GaugedUseAfterFreeError):
            s.append(1, tmp_array)
        with self.assertRaises(GaugedUseAfterFreeError):
            tmp.append(1, freed_array)
        with self.assertRaises(GaugedUseAfterFreeError):
            s.slice()
        with self.assertRaises(GaugedUseAfterFreeError):
            s.concat(tmp)
        with self.assertRaises(GaugedUseAfterFreeError):
            tmp.concat(s)
        with self.assertRaises(GaugedUseAfterFreeError):
            s.byte_length()
        with self.assertRaises(GaugedUseAfterFreeError):
            s.buffer()
        with self.assertRaises(GaugedUseAfterFreeError):
            s.clear()
        with self.assertRaises(GaugedUseAfterFreeError):
            s.first()
        with self.assertRaises(GaugedUseAfterFreeError):
            s.last()
        with self.assertRaises(GaugedUseAfterFreeError):
            s.sum()
        with self.assertRaises(GaugedUseAfterFreeError):
            s.min()
        with self.assertRaises(GaugedUseAfterFreeError):
            s.max()
        with self.assertRaises(GaugedUseAfterFreeError):
            s.mean()
        with self.assertRaises(GaugedUseAfterFreeError):
            s.stddev()
        with self.assertRaises(GaugedUseAfterFreeError):
            s.count()
        with self.assertRaises(GaugedUseAfterFreeError):
            s.percentile(50)
        with self.assertRaises(GaugedUseAfterFreeError):
            list(s.iteritems())
        tmp_array.free()
        tmp.free()

    def test_map_repr(self):
        map = SparseMap()
        self.assertEqual(str(map), '')
        a = FloatArray([1, 2, 3])
        map.append(1, a)
        b = FloatArray([4])
        map.append(2, b)
        self.assertEqual(str(map), '[ 1 ] = [1.0, 2.0, 3.0]\n[ 2 ] = [4.0]')
        a.free()
        b.free()
        map.free()

    def test_array_repr(self):
        s = FloatArray()
        self.assertEqual(str(s), '[]')
        s.free()
        s = FloatArray([1])
        self.assertEqual(str(s), '[1.0]')
        s.free()
        s = FloatArray([1, 2, 3])
        self.assertEqual(str(s), '[1.0, 2.0, 3.0]')
        s.free()

    def test_double_free(self):
        s = FloatArray()
        s.free()
        s.free()
        v = SparseMap()
        v.free()
        v.free()
