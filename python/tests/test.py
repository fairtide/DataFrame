import sys
import os

root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

sys.path.insert(0, os.path.join(root, 'python'))

import arrow_bson
import bson.json_util
import gzip
import json
import jsonschema
import numpy
import pyarrow
import unittest

with gzip.open(os.path.join(root, 'test.data.gz'), 'rb') as f:
    TABLE = pyarrow.RecordBatchFileReader(f).read_all()

with open(os.path.join(root, 'schema.json')) as f:
    SCHEMA = json.loads(f.read())


class TestArrowBSON(unittest.TestCase):
    def test_offsets(self):
        offsets = numpy.array([0, 1, 3, 3, 4, 8, 9, 13], numpy.int32)
        buf = offsets.tobytes()
        enc = arrow_bson.encode_offsets(buf)[0]
        dec = arrow_bson.decode_offsets(enc)[0].to_pybytes()
        self.assertEqual(buf, dec)

    def test_datetime(self):
        t = numpy.int32()
        datetime = numpy.array([0, 1, 3, 3, 4, 8, 9, 13], t)
        buf = datetime.tobytes()
        enc = arrow_bson.encode_datetime(buf, t)
        dec = arrow_bson.decode_datetime(enc, t)[0].to_pybytes()
        self.assertEqual(buf, dec)

    def test_write(self):
        buf = arrow_bson.write_table(TABLE)
        options = bson.json_util.JSONOptions(
            json_mode=bson.json_util.JSONMode.CANONICAL)
        instance = bson.json_util.dumps(bson.raw_bson.RawBSONDocument(buf),
                                        json_options=options)
        jsonschema.validate(instance=json.loads(instance), schema=SCHEMA)

    def test_read(self):
        buf = arrow_bson.write_table(TABLE)
        ret = arrow_bson.read_table(buf)
        for c1, c2 in zip(TABLE.columns, ret.columns):
            if not c1.equals(c2):
                print(c1.name)
        # self.assertTrue(ret.equals(TABLE))


if __name__ == '__main__':
    unittest.main()
