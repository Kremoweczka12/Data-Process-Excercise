import unittest
import time
import main


class TestDataReader(unittest.TestCase):
    def test_time(self):
        start = time.time()
        self.metadata = main.gather_Data('data.raw', True)
        self.assertLess(round(time.time() - start, 4), 1)

    def test_size(self):
        self.metadata = main.gather_Data('data.raw', False)
        self.assertEqual(25000, self.metadata['size'])
        self.assertEqual(self.metadata['size'], sum([x for x in self.metadata['sizebygroup'].values()]))

    def test_errors(self):
        self.metadata = main.gather_Data('data.raw', False)
        self.assertEqual(8, len(self.metadata['errors']))

    def test_age(self):
        self.metadata = main.gather_Data('data.raw', False)
        self.assertEqual(self.metadata['newest'][2], 'WHEJNKGW99')
        self.assertEqual(self.metadata['oldest'][2], 'HBWRKAXG18')


if __name__ == '__main__':
    unittest.main()
