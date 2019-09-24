import sys
import os

sys.path.insert(0,
                os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from bson_dataframe import *

import numpy
import unittest


class TestArray(Visitor):
    def __init__(self, length, nullable):
        self.length = length
        self.nullable = nullable

        self.counts = numpy.random.randint(0, 10, self.length + 1, numpy.int32)
        self.counts[0] = 0

        if self.nullable:
            n = length
            m = n // 8 + 1
            mask = numpy.ndarray(m, numpy.uint8, numpy.random.bytes(m))
            mask = numpy.unpackbits(mask)[:n].astype(bool, copy=False)
            if n > 1:
                mask[0] = True
                mask[1] = False
            for i, v in enumerate(mask):
                if not v:
                    self.counts[i + 1] = 0
            self.mask = numpy.packbits(mask).tobytes()
        else:
            self.mask = None

        self.total = numpy.sum(self.counts)

    def visit_null(self, schema):
        return NullArray(self.length)

    def visit_numeric(self, schema):
        data = numpy.random.bytes(self.length * schema.byte_width)
        return array_type(schema)(data, self.mask)

    def visit_date(self, schema):
        data = numpy.random.bytes(self.length * schema.byte_width)
        return DateArray(data, self.mask, schema=schema)

    def visit_timestamp(self, schema):
        data = numpy.random.bytes(self.length * schema.byte_width)
        return TimestampArray(data, self.mask, schema=schema)

    def visit_time(self, schema):
        data = numpy.random.bytes(self.length * schema.byte_width)
        return TimeArray(data, self.mask, schema=schema)

    def visit_opaque(self, schema):
        data = numpy.random.bytes(self.length * schema.byte_width)
        return OpaqueArray(data, self.mask, schema=schema)

    def visit_binary(self, schema):
        data = numpy.random.randint(65, 97, self.total, numpy.int8).tobytes()
        return array_type(schema)(data, self.mask, counts=self.counts)

    def visit_dictionary(self, schema):
        index = numpy.random.randint(0, 10, self.length, schema.index.name)
        index = array_type(schema.index)(index, self.mask)
        value = schema.value.accept(TestArray(10, False))
        return array_type(schema)(index, value)

    def visit_list(self, schema):
        data = schema.value.accept(TestArray(self.total, self.nullable))
        return ListArray(data, self.mask, counts=self.counts)

    def visit_struct(self, schema):
        data = [(k, v.accept(TestArray(self.length, self.nullable)))
                for k, v in schema.fields]
        return StructArray(data, self.mask, schema=schema)


def test_array(schema, length, nullable=False):
    return schema.accept(TestArray(length, nullable))


TEST_SCHEMAS = (
    # Null(),
    # Bool(),
    # Int8(),
    # Int16(),
    # Int32(),
    # Int64(),
    # UInt8(),
    # UInt16(),
    # UInt32(),
    # UInt64(),
    # Float16(),
    # Float32(),
    # Float64(),
    # Date('d'),
    # Date('ms'),
    # Timestamp('s'),
    # Timestamp('ms'),
    # Timestamp('us'),
    # Timestamp('ns'),
    # Timestamp('s', 'Asia/Singapore'),
    # Timestamp('ms', 'Asia/Singapore'),
    # Timestamp('us', 'Asia/Singapore'),
    # Timestamp('ns', 'Asia/Singapore'),
    # Time('s'),
    # Time('ms'),
    # Time('us'),
    # Time('ns'),
    # Opaque(3),
    # Opaque(7),
    # Bytes(),
    # Utf8(),
    # Ordered(Int32(), Utf8()),
    # Ordered(Int64(), UInt64()),
    # Factor(Int32(), Utf8()),
    # Factor(Int64(), UInt64()),
    List(Int32()),
    # Struct([('x', Int32()), ('y', Int64())]),
    # List(Struct([('x', Int32()), ('y', Int64())])),
    # Struct([('x', List(Int32())), ('y', List(Int64()))]),
)
