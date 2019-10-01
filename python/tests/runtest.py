# ============================================================================
# Copyright 2019 Fairtide Pte. Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ============================================================================

import sys
import os

sys.path.insert(0,
                os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from bson_dataframe import *
import bson_dataframe.arrow

import numpy
import unittest

NUMERIC = [
    Bool(),
    Int8(),
    Int16(),
    Int32(),
    Int64(),
    UInt8(),
    UInt16(),
    UInt32(),
    UInt64(),
    Float16(),
    Float32(),
    Float64(),
]

DATE = [
    Date('d'),
]

DATE_MS = [
    Date('ms'),
]

TIMESTAMP = [
    Timestamp('s'),
    Timestamp('ms'),
    Timestamp('us'),
    Timestamp('ns'),
]

TIME = [
    Time('s'),
    Time('ms'),
    Time('us'),
    Time('ns'),
]

OPAQUE = [
    Opaque(1),
    Opaque(3),
    Opaque(5),
    Opaque(7),
]

BINARY = [
    Bytes(),
    Utf8(),
]

DICTIONARY = [
    Ordered(Int32(), Utf8()),
    Factor(Int32(), Utf8()),
]

LIST = [
    List(Int32()),
]

STRUCT = [
    Struct([('x', Int32()), ('y', Int64())]),
]

NESTED = [
    List(Utf8()),
    Struct([('x', Int32()), ('y', Utf8())]),
    List(Struct([('x', Int32()), ('y', Utf8())])),
    Struct([('x', List(Int32())), ('y', List(Utf8()))]),
]

ARRAY_SCHEMAS = sum([
    NUMERIC,
    DATE,
    TIMESTAMP,
    TIME,
    OPAQUE,
    BINARY,
    DICTIONARY,
    LIST,
    STRUCT,
    NESTED,
], [])

NUMPY_SCHEMAS = sum([
    NUMERIC,
    DATE,
    TIMESTAMP,
    TIME,
    OPAQUE,
    STRUCT,
], [])

NUMPY_ARRAYS = sum([
    NUMPY_SCHEMAS,
    DATE_MS,
    BINARY,
    LIST,
    NESTED,
], [])

PANDAS_SCHEMAS = sum([
    NUMERIC,
    DATE,
    [Timestamp('ns')],
    [Time('ns')],
    OPAQUE,
], [])

PANDAS_ARRAYS = sum([
    PANDAS_SCHEMAS,
    DATE_MS,
    TIMESTAMP,
    TIME,
    BINARY,
    DICTIONARY,
    LIST,
], [])

ARRAY_LENGTH = 10


class TestArray(Visitor):
    def __init__(self, length, masked):
        self.length = length
        self.masked = masked

        self.counts = numpy.random.randint(10, 20, self.length + 1,
                                           numpy.int32)
        self.counts[0] = 0

        if self.masked:
            n = length
            m = n // 8 + 1
            mask = numpy.ndarray(m, numpy.uint8, numpy.random.bytes(m))
            mask = numpy.unpackbits(mask)[:n].astype(bool, copy=False)
            if len(mask) > 1:
                mask[0] = True
                mask[1] = False
            for i, v in enumerate(mask):
                if not v:
                    self.counts[i + 1] = 0
            self.numpy_mask = ~mask
            self.mask = numpy.packbits(mask).tobytes()
        else:
            self.numpy_mask = numpy.ma.nomask
            self.mask = None

        self.total = numpy.sum(self.counts)

    def visit_numeric(self, schema):
        data = numpy.random.randint(-1000, 1000, self.length)
        data = data.astype(schema.name)
        data = numpy.ma.masked_array(data, self.numpy_mask)
        return array_type(schema)(data, self.mask)

    def visit_date(self, schema):
        data = numpy.random.randint(-1000, 1000, self.length)
        data = data.astype(f'i{schema.byte_width}')
        data = numpy.ma.masked_array(data, self.numpy_mask)
        return DateArray(data, self.mask, schema=schema)

    def visit_timestamp(self, schema):
        data = numpy.random.randint(-1000, 1000, self.length)
        data = data.astype(numpy.int64)
        data = numpy.ma.masked_array(data, self.numpy_mask)
        return TimestampArray(data, self.mask, schema=schema)

    def visit_time(self, schema):
        data = numpy.random.randint(-1000, 1000, self.length)
        data = data.astype(f'i{schema.byte_width}')
        data = numpy.ma.masked_array(data, self.numpy_mask)
        return TimeArray(data, self.mask, schema=schema)

    def visit_opaque(self, schema):
        data = numpy.random.bytes(self.length * schema.byte_width)
        data = numpy.frombuffer(data, f'S{schema.byte_width}')
        data = numpy.ma.masked_array(data, self.numpy_mask)
        return OpaqueArray(data, self.mask, schema=schema)

    def visit_binary(self, schema):
        data = numpy.random.randint(65, 97, self.total, numpy.int8).tobytes()
        return array_type(schema)(data,
                                  self.mask,
                                  length=self.length,
                                  counts=self.counts)

    def visit_dictionary(self, schema):
        index = numpy.random.randint(0, 10, self.length, schema.index.name)
        index = array_type(schema.index)(index, self.mask)
        value = schema.value.accept(TestArray(10, False))
        return array_type(schema)(index, value)

    def visit_list(self, schema):
        data = schema.value.accept(TestArray(self.total, self.masked))
        return ListArray(data,
                         self.mask,
                         length=self.length,
                         counts=self.counts)

    def visit_struct(self, schema):
        data = [(k, v.accept(TestArray(self.length, self.masked)))
                for k, v in schema.fields]
        return StructArray(data, schema=schema)


def array(schema, length, masked=False):
    return schema.accept(TestArray(length, masked))


class TestBSONDataFrame(unittest.TestCase):
    def assertMask(self, ary, encmask):
        arymask = numpy.frombuffer(ary.mask, numpy.uint8)
        arymask = numpy.unpackbits(arymask)[:len(ary)]
        arymask = arymask.astype(bool)
        arymask = ~arymask

        if encmask is None:
            self.assertFalse(arymask.any())
            return

        if encmask is numpy.ma.nomask:
            self.assertFalse(arymask.any())
            return

        if encmask.dtype.kind == 'V':
            for k, v in ary.fields:
                with self.subTest(k):
                    self.assertMask(v, encmask[k])
            return

        arymask = numpy.packbits(arymask).tobytes()
        encmask = numpy.packbits(encmask).tobytes()
        self.assertEqual(arymask, encmask)

    def assertArrayEqual(self, ary, dec):
        self.assertEqual(ary.schema, dec.schema)
        self.assertEqual(len(ary), len(dec))
        self.assertEqual(ary.mask, dec.mask)

        if isinstance(ary, DictionaryArray):
            with self.subTest('index'):
                self.assertArrayEqual(ary.index, dec.index)
            with self.subTest('value'):
                self.assertArrayEqual(ary.value, dec.value)
        elif isinstance(ary, ListArray):
            with self.subTest('value'):
                self.assertArrayEqual(ary.value, dec.value)
        elif isinstance(ary, StructArray):
            for aryfield, decfield in zip(ary.fields, dec.fields):
                with self.subTest(aryfield[0]):
                    self.assertEqual(aryfield[0], decfield[0])
                    self.assertArrayEqual(aryfield[1], decfield[1])
        else:
            self.assertEqual(ary.data, dec.data)

    def test_schema(self):
        for sch in ARRAY_SCHEMAS:
            with self.subTest(schema=sch):
                enc = sch.encode()
                dec = sch.decode(enc)
                self.assertEqual(sch, dec)

    def test_array(self):
        for sch in ARRAY_SCHEMAS:
            for masked in [False, True]:
                with self.subTest(schema=sch, masked=masked):
                    ary = array(sch, ARRAY_LENGTH, masked)
                    enc = ary.encode()
                    sch.validate(enc)
                    dec = Array.decode(enc)
                    self.assertArrayEqual(ary, dec)

    def test_numpy_schema(self):
        for sch in NUMPY_SCHEMAS:
            with self.subTest(schema=sch):
                enc = sch.to_numpy()
                dec = Schema.from_numpy(enc)
                self.assertEqual(sch, dec)

    def test_numpy_array(self):
        for sch in NUMPY_ARRAYS:
            for masked in [False, True]:
                with self.subTest(schema=sch, masked=masked):
                    ary = array(sch, ARRAY_LENGTH, masked)
                    enc = ary.to_numpy(usemask=masked)
                    dec = Array.from_numpy(enc, schema=sch)

                    ismasked = isinstance(enc, numpy.ma.masked_array)
                    self.assertEqual(masked, ismasked)
                    self.assertMask(ary, getattr(enc, 'mask', None))
                    self.assertArrayEqual(ary, dec)

    def test_pandas_schema(self):
        for sch in PANDAS_SCHEMAS:
            with self.subTest(schema=sch):
                enc = sch.to_pandas()
                dec = Schema.from_pandas(enc)
                self.assertEqual(sch, dec)

    def test_pandas_series(self):
        for sch in PANDAS_ARRAYS:
            with self.subTest(schema=sch):
                ary = array(sch, ARRAY_LENGTH, False)
                enc = ary.to_pandas()
                dec = Array.from_pandas(enc, schema=sch)
                self.assertArrayEqual(ary, dec)

    def test_arrow_schema(self):
        for sch in ARRAY_SCHEMAS:
            with self.subTest(schema=sch):
                enc = sch.to_arrow()
                dec = Schema.from_arrow(enc)
                self.assertEqual(sch, dec)

    def test_arrow_array(self):
        for sch in ARRAY_SCHEMAS:
            for masked in [False, True]:
                with self.subTest(schema=sch, masked=masked):
                    ary = array(sch, ARRAY_LENGTH, masked)
                    enc = ary.to_arrow()
                    dec = Array.from_arrow(enc)
                    self.assertArrayEqual(ary, dec)


if __name__ == '__main__':
    unittest.main()
