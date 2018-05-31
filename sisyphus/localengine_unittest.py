import unittest

from sisyphus import localengine


class LETest(unittest.TestCase):

    def test_f(self):
        le = localengine.LocalEngine()
        # TODO actuall testing...
        le.stop()

if __name__ == '__main__':
    unittest.main()
