import sys
import os

root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

sys.path.insert(0, os.path.join(root, 'python'))

import arrow_bson
import gzip
import numpy
import pyarrow
import unittest

with gzip.open(os.path.join(root, 'test.data.gz'), 'rb') as f:
    TABLE = pyarrow.RecordBatchFileReader(f).read_all()


class TestArrowBSON(unittest.TestCase):
    def test_offsets_codec(self):
        val = numpy.array([0, 1, 3, 3, 4, 8, 9, 13], numpy.int32)
        buf = val.tobytes()
        enc = arrow_bson.encode_offsets(buf)[0]
        dec = arrow_bson.decode_offsets(enc)[0].to_pybytes()
        self.assertEqual(buf, dec)

        off = (val + numpy.int32(10)).tobytes()
        enc = arrow_bson.encode_offsets(off)[0]
        dec = arrow_bson.decode_offsets(enc)[0].to_pybytes()
        self.assertEqual(buf, dec)

    def test_datetime_codec(self):
        val = numpy.array([1, 1, 3, 3, 4, 8, 9, 13], numpy.int32)
        buf = val.tobytes()
        enc = arrow_bson.encode_datetime(buf, numpy.int32)
        dec = arrow_bson.decode_datetime(enc, numpy.int32)[0].to_pybytes()
        self.assertEqual(buf, dec)

    def test_write(self):
        buf = arrow_bson.write_table(TABLE)
        arrow_bson.validate(arrow_bson.write_table(TABLE))

    def test_read(self):
        buf = arrow_bson.write_table(TABLE)
        ret = arrow_bson.read_table(buf)
        self.assertTrue(ret.equals(TABLE))


if __name__ == '__main__':
    unittest.main()
