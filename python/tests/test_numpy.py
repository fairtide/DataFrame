from test_data import *

import numpy
from bson_dataframe.numpy import *


class TestNumpy(unittest.TestCase):
    def test_array(self):
        for sch in TEST_SCHEMAS:
            if isinstance(sch, Dictionary):
                continue
            with self.subTest(schema=str(sch)):
                ary = test_array(sch, 1000, False)
                enc = to_numpy(ary)
                dec = from_numpy(enc, schema=sch)
                self.assertEqual(ary, dec)

    def test_nullable_array(self):
        for sch in TEST_SCHEMAS:
            if isinstance(sch, Dictionary):
                continue
            with self.subTest(schema=str(sch)):
                ary = test_array(sch, 1000, True)
                enc = to_numpy(ary)
                dec = from_numpy(enc, schema=sch)
                self.assertEqual(ary, dec)


if __name__ == '__main__':
    unittest.main()
