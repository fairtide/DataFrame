from test_data import *


class TestSchema(unittest.TestCase):
    def test_type(self):
        for sch in TEST_SCHEMAS:
            with self.subTest(schema=str(sch)):
                enc = sch.encode()
                dec = sch.decode(enc)
                self.assertEqual(sch, dec)

    def test_array(self):
        for sch in TEST_SCHEMAS:
            with self.subTest(schema=str(sch)):
                ary = test_array(sch, 1000, False)
                enc = ary.encode()
                sch.validate(enc)
                dec = Array.decode(enc)
                self.assertEqual(ary, dec)

    def test_nullable_array(self):
        for sch in TEST_SCHEMAS:
            with self.subTest(schema=str(sch)):
                ary = test_array(sch, 1000, True)
                enc = ary.encode()
                sch.validate(enc)
                dec = Array.decode(enc)
                self.assertEqual(ary, dec)


if __name__ == '__main__':
    unittest.main()
