from .test import *

import bson.raw_bson
import numpy
import pyarrow
import unittest



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
