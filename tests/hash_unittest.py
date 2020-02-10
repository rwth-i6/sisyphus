import unittest

from sisyphus.hash import *


def b():
    pass


class HashTest(unittest.TestCase):

    def test_get_object_state(self):

        c = lambda x: x  # noqa: E731

        def d():
            pass

        self.assertEqual(sis_hash_helper(b),
                         b"(function, (tuple, (str, '" + __name__.encode() + b"'), (str, 'b')))")
        self.assertRaises(AssertionError, sis_hash_helper, c)


if __name__ == '__main__':
    unittest.main()
