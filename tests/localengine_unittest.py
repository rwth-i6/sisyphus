import unittest

from sisyphus import localengine


class LETest(unittest.TestCase):

    def test_f(self):
        le = localengine.LocalEngine()
        le.start_engine()
        # TODO actuall testing...
        le.stop_engine()


if __name__ == '__main__':
    unittest.main()
