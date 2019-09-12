import sys
import os

sys.path.insert(0,
                os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from bson_dataframe import *
from bson_dataframe.arrow import *

import bson.raw_bson
import numpy
import pyarrow
import unittest


class TestArrayGenerator(SchemaVisitor):
    def __init__(self, n, typ, Typ, nullable):
        self.nullable = nullable

        self.data = ArrayData()
        self.data.length = n
        self.data.type = typ

        if pyarrow.types.is_null(typ):
            self.data.null_count = n
            self.data.buffers.append(None)
        elif self.nullable:
            m = n // 8 + 1
            mask = numpy.ndarray(m, numpy.uint8, numpy.random.bytes(m))
            vals = numpy.unpackbits(mask)[:n]
            self.data.null_count = n - numpy.sum(vals)
            buf = numpy.packbits(vals).tobytes()
            self.data.buffers.append(pyarrow.py_buffer(buf))
        else:
            self.data.null_count = 0
            self.data.buffers.append(None)

        Typ.accept(self)

        self.array = self.data.make_array()

    def visit_null(self, typ):
        pass

    def visit_bool(self, typ):
        n = self.data.length // 8 + 1
        bits = numpy.ndarray(n, numpy.uint8, numpy.random.bytes(n))
        vals = numpy.unpackbits(bits)[:self.data.length]
        buf = numpy.packbits(vals).tobytes()
        self.data.buffers.append(pyarrow.py_buffer(buf))

    def _visit_flat(self, typ):
        buf = numpy.random.bytes(self.data.length * typ.byte_width)
        self.data.buffers.append(pyarrow.py_buffer(buf))

    def visit_int8(self, typ):
        self._visit_flat(typ)

    def visit_int16(self, typ):
        self._visit_flat(typ)

    def visit_int32(self, typ):
        self._visit_flat(typ)

    def visit_int64(self, typ):
        self._visit_flat(typ)

    def visit_uint8(self, typ):
        self._visit_flat(typ)

    def visit_uint16(self, typ):
        self._visit_flat(typ)

    def visit_uint32(self, typ):
        self._visit_flat(typ)

    def visit_uint64(self, typ):
        self._visit_flat(typ)

    def visit_float16(self, typ):
        self._visit_flat(typ)

    def visit_float32(self, typ):
        self._visit_flat(typ)

    def visit_float64(self, typ):
        self._visit_flat(typ)

    def visit_date(self, typ):
        self._visit_flat(typ)

    def visit_timestamp(self, typ):
        self._visit_flat(typ)

    def visit_time(self, typ):
        self._visit_flat(typ)

    def visit_opaque(self, typ):
        self._visit_flat(typ)

    def _generate_offsets(self):
        counts = numpy.random.randint(0, 10, self.data.length + 1, numpy.int32)
        counts[0] = 0

        if self.nullable:
            buf = self.data.buffers[0]
            bits = numpy.ndarray(len(buf), numpy.uint8, buf)
            mask = numpy.unpackbits(bits, bitorder='little')[:self.data.length]
            for i, v in enumerate(mask):
                if not v:
                    counts[i + 1] = 0

        offsets = numpy.cumsum(counts, dtype=numpy.int32)
        buf = offsets.tobytes()
        self.data.buffers.append(pyarrow.py_buffer(buf))

        return offsets[-1]

    def visit_bytes(self, typ):
        n = self._generate_offsets()
        buf = numpy.random.bytes(n)
        self.data.buffers.append(pyarrow.py_buffer(buf))

    def visit_utf8(self, typ):
        self.visit_bytes(typ)

    def visit_list(self, typ):
        n = self._generate_offsets()
        self.data.children.append(
            TestArrayGenerator(n, typ.value_type, self.nullable).array)

    def visit_struct(self, typ):
        for field in typ:
            self.data.children.append(
                TestArrayGenerator(self.data.length, field.type,
                                   self.nullable).array)

    def visit_dictionary(self, typ):
        index = numpy.random.randint(0, 10, self.data.length,
                                     typ.index_type.name)
        buf = index.tobytes()
        self.data.buffers.append(pyarrow.py_buffer(buf))
        self.data.dictionary = TestArrayGenerator(10, typ.value_type,
                                                  False).array


TEST_TYPES = [
    (pyarrow.null(), Null()),
    (pyarrow.bool_(), Bool()),
    (pyarrow.int8(), Int8()),
    (pyarrow.int16(), Int16()),
    (pyarrow.int32(), Int32()),
    (pyarrow.int64(), Int64()),
    (pyarrow.uint8(), UInt8()),
    (pyarrow.uint16(), UInt16()),
    (pyarrow.uint32(), UInt32()),
    (pyarrow.uint64(), UInt64()),
    (pyarrow.float16(), Float16()),
    (pyarrow.float32(), Float32()),
    (pyarrow.float64(), Float64()),
    (pyarrow.date32(), Date('d')),
    (pyarrow.date64(), Date('ms')),
    (pyarrow.timestamp('s'), Timestamp('s')),
    (pyarrow.timestamp('ms'), Timestamp('ms')),
    (pyarrow.timestamp('us'), Timestamp('us')),
    (pyarrow.timestamp('ns'), Timestamp('ns')),
    (pyarrow.time32('s'), Time('s')),
    (pyarrow.time32('ms'), Time('ms')),
    (pyarrow.time64('us'), Time('us')),
    (pyarrow.time64('ns'), Time('ns')),
    (pyarrow.string(), Utf8()),
    (pyarrow.binary(), Bytes()),
    (pyarrow.binary(4), Opaque(4)),
    (
        pyarrow.dictionary(pyarrow.int32(), pyarrow.string(), True),
        Dictionary(Int32(), Utf8(), True),
    ),
    (
        pyarrow.dictionary(pyarrow.int64(), pyarrow.int64(), True),
        Dictionary(Int64(), Int64(), True),
    ),
    (
        pyarrow.dictionary(pyarrow.int32(), pyarrow.string(), False),
        Dictionary(Int32(), Utf8(), False),
    ),
    (
        pyarrow.dictionary(pyarrow.int64(), pyarrow.int64(), False),
        Dictionary(Int64(), Int64(), False),
    ),
    (
        pyarrow.list_(pyarrow.int32()),
        List(Int32()),
    ),
    (
        pyarrow.struct([pyarrow.field('int32', pyarrow.int32())]),
        Struct([('int32', Int32())]),
    ),
    (
        pyarrow.list_(pyarrow.struct([pyarrow.field('int32',
                                                    pyarrow.int32())])),
        List(Struct([('int32', Int32())])),
    ),
    (
        pyarrow.struct(
            [pyarrow.field('int32', pyarrow.list_(pyarrow.int32()))]),
        Struct([('int32', List(Int32()))]),
    ),
]


def test_table(n, offset=None, length=None, nullable=True):
    data = list()

    for t, T in TEST_TYPES:
        name = str(t)
        array = TestArrayGenerator(n, t, T, nullable).array
        if offset is not None:
            array = array.slice(offset, length)
        data.append(pyarrow.column(name, array))

    return pyarrow.Table.from_arrays(data)


class TestArrowBSON(unittest.TestCase):
    def test_encode(self):
        table = test_table(1000, nullable=True)
        buf = encode_table(table)
        doc = bson.raw_bson.RawBSONDocument(buf)
        for i, col in enumerate(table.columns):
            TEST_TYPES[i][1].validate(doc[col.name], validate_data=True)

        table = test_table(1000, nullable=False)
        buf = encode_table(table)
        doc = bson.raw_bson.RawBSONDocument(buf)
        for i, col in enumerate(table.columns):
            TEST_TYPES[i][1].validate(doc[col.name], validate_data=True)

    def test_decode(self):
        table = test_table(1000, nullable=True)
        buf = encode_table(table)
        ret = decode_table(buf)
        self.assertTrue(ret.equals(table))

        table = test_table(1000, nullable=False)
        buf = encode_table(table)
        ret = decode_table(buf)
        self.assertTrue(ret.equals(table))

    def test_slice(self):
        table = test_table(1000, offset=100, length=500, nullable=True)
        buf = encode_table(table)
        ret = decode_table(buf)
        self.assertTrue(ret.equals(table))

        table = test_table(1000, offset=100, length=500, nullable=False)
        buf = encode_table(table)
        ret = decode_table(buf)
        self.assertTrue(ret.equals(table))

    def test_slice_head(self):
        table = test_table(1000, offset=0, length=500, nullable=True)
        buf = encode_table(table)
        ret = decode_table(buf)
        self.assertTrue(ret.equals(table))

        table = test_table(1000, offset=0, length=500, nullable=False)
        buf = encode_table(table)
        ret = decode_table(buf)
        self.assertTrue(ret.equals(table))

    def test_slice_tail(self):
        table = test_table(1000, offset=500, length=500, nullable=True)
        buf = encode_table(table)
        ret = decode_table(buf)
        self.assertTrue(ret.equals(table))

        table = test_table(1000, offset=500, length=500, nullable=False)
        buf = encode_table(table)
        ret = decode_table(buf)
        self.assertTrue(ret.equals(table))


if __name__ == '__main__':
    unittest.main()
