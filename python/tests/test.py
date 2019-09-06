import sys
import os

sys.path.insert(0,
                os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import bson_dataframe
import numpy
import pyarrow
import unittest


class TestArrayGenerator(bson_dataframe.TypeVisitor):
    def __init__(self, n, typ, nullable):
        self.nullable = nullable

        self.data = bson_dataframe.ArrayData()
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

        self.accept(typ)

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
        buf = numpy.random.bytes(self.data.length * typ.bit_width // 8)
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

    def visit_time32(self, typ):
        self._visit_flat(typ)

    def visit_time64(self, typ):
        self._visit_flat(typ)

    def visit_date32(self, typ):
        self._visit_flat(typ)

    def visit_date64(self, typ):
        self._visit_flat(typ)

    def visit_timestamp(self, typ):
        self._visit_flat(typ)

    def visit_fixed_size_binary(self, typ):
        self._visit_flat(typ)

    def visit_float32(self, typ):
        buf = numpy.random.rand(self.data.length).astype(
            numpy.float32).tobytes()
        self.data.buffers.append(pyarrow.py_buffer(buf))

    def visit_float64(self, typ):
        buf = numpy.random.rand(self.data.length).astype(
            numpy.float64).tobytes()
        self.data.buffers.append(pyarrow.py_buffer(buf))

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

    def visit_binary(self, typ):
        n = self._generate_offsets()
        buf = numpy.random.bytes(n)
        self.data.buffers.append(pyarrow.py_buffer(buf))

    def visit_string(self, typ):
        self.visit_binary(typ)

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
                                     typ.index_type.to_pandas_dtype())
        buf = index.tobytes()
        self.data.buffers.append(pyarrow.py_buffer(buf))
        self.data.dictionary = TestArrayGenerator(10, typ.value_type,
                                                  False).array


def test_table(n, types=None, offset=None, length=None, nullable=True):
    if types is None:
        types = [
            pyarrow.null(),
            pyarrow.bool_(),
            pyarrow.int8(),
            pyarrow.int16(),
            pyarrow.int32(),
            pyarrow.int64(),
            pyarrow.uint8(),
            pyarrow.uint16(),
            pyarrow.uint32(),
            pyarrow.uint64(),
            pyarrow.float16(),
            pyarrow.float32(),
            pyarrow.float64(),
            pyarrow.date32(),
            pyarrow.date64(),
            pyarrow.timestamp('s'),
            pyarrow.timestamp('ms'),
            pyarrow.timestamp('us'),
            pyarrow.timestamp('ns'),
            pyarrow.time32('s'),
            pyarrow.time32('ms'),
            pyarrow.time64('us'),
            pyarrow.time64('ns'),
            pyarrow.string(),
            pyarrow.binary(),
            pyarrow.binary(4),
            pyarrow.dictionary(pyarrow.int32(), pyarrow.string(), True),
            pyarrow.dictionary(pyarrow.int64(), pyarrow.int64(), True),
            pyarrow.dictionary(pyarrow.int32(), pyarrow.string(), False),
            pyarrow.dictionary(pyarrow.int64(), pyarrow.int64(), False),
            pyarrow.list_(pyarrow.int32()),
            pyarrow.struct([pyarrow.field('int32', pyarrow.int32())]),
            pyarrow.list_(
                pyarrow.struct([pyarrow.field('int32', pyarrow.int32())])),
            pyarrow.struct(
                [pyarrow.field('int32', pyarrow.list_(pyarrow.int32()))]),
        ]

    data = list()

    for t in types:
        name = str(t)
        array = TestArrayGenerator(n, t, False).array
        if offset is not None:
            array = array.slice(offset, length)
        data.append(pyarrow.column(name, array))

        if nullable:
            name = str(t) + ' (null)'
            array = TestArrayGenerator(n, t, True).array
            if offset is not None:
                array = array.slice(offset, length)
            data.append(pyarrow.column(name, array))

    return pyarrow.Table.from_arrays(data)


class TestArrowBSON(unittest.TestCase):
    def test_offsets_codec(self):
        val = numpy.array([0, 1, 3, 3, 4, 8, 9, 13], numpy.int32)
        buf = val.tobytes()
        enc = bson_dataframe.encode_offsets(buf)[0]
        dec = bson_dataframe.decode_offsets(enc)[0].to_pybytes()
        self.assertEqual(buf, dec)

        off = (val + numpy.int32(10)).tobytes()
        enc = bson_dataframe.encode_offsets(off)[0]
        dec = bson_dataframe.decode_offsets(enc)[0].to_pybytes()
        self.assertEqual(buf, dec)

    def test_datetime_codec(self):
        val = numpy.array([1, 1, 3, 3, 4, 8, 9, 13], numpy.int32)
        buf = val.tobytes()
        enc = bson_dataframe.encode_datetime(buf, numpy.int32)
        dec = bson_dataframe.decode_datetime(enc, numpy.int32)[0].to_pybytes()
        self.assertEqual(buf, dec)

    def test_write(self):
        table = test_table(1000)
        buf = bson_dataframe.write_table(table)
        bson_dataframe.validate(bson_dataframe.write_table(table))

    def test_read(self):
        table = test_table(1000)
        buf = bson_dataframe.write_table(table)
        ret = bson_dataframe.read_table(buf)
        self.assertTrue(ret.equals(table))

    def test_slice(self):
        table = test_table(1000, offset=100, length=500)
        buf = bson_dataframe.write_table(table)
        ret = bson_dataframe.read_table(buf)
        self.assertTrue(ret.equals(table))

    def test_slice_head(self):
        table = test_table(1000, offset=0, length=500)
        buf = bson_dataframe.write_table(table)
        ret = bson_dataframe.read_table(buf)
        self.assertTrue(ret.equals(table))

    def test_slice_tail(self):
        table = test_table(1000, offset=500, length=500)
        buf = bson_dataframe.write_table(table)
        ret = bson_dataframe.read_table(buf)
        self.assertTrue(ret.equals(table))


if __name__ == '__main__':
    unittest.main()
