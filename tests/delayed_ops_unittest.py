import unittest
from sisyphus.delayed_ops import *
from sisyphus.hash import short_hash


class DelayedOpsTest(unittest.TestCase):

    def check_only_get_eq(self, a, b):
        """ Check that a and b are normally not equal, but are equal after calling get """
        self.assertNotEqual(a, b)
        self.assertEqual(a.get(), b)

    def test_value(self):
        a = Delayed(3)
        self.assertNotEqual(a, 3)
        self.assertEqual(a.get(), 3)

        self.assertEqual(short_hash(a), short_hash(3))

    def test_int(self):
        a = Delayed(3)
        self.check_only_get_eq(a + 4, 7)
        self.check_only_get_eq(4 + a, 7)

        self.check_only_get_eq(4 * a, 12)
        self.check_only_get_eq(a * 4, 12)

        self.check_only_get_eq(a - 4, -1)
        self.check_only_get_eq(4 - a, 1)

        self.check_only_get_eq(a ** 2, 9)
        self.check_only_get_eq(2 ** a, 8)
        self.check_only_get_eq(a ** 2 % 2, 1)
        self.check_only_get_eq(2 ** a % 2, 0)

    def test_string(self):
        a = Delayed('foo')

        self.check_only_get_eq(a + 'bar', 'foobar')
        self.check_only_get_eq('bar' + a, 'barfoo')

        self.check_only_get_eq(a * 3, 'foofoofoo')

        a = Delayed('foo%s foo')
        self.check_only_get_eq(a % 'bar', 'foobar foo')

        a = Delayed('foo{b} foo')
        self.check_only_get_eq(a.format(b='bar'), 'foobar foo')

        a = Delayed('foobbb foo')
        b = a.replace('bbb', 'bar')
        self.check_only_get_eq(b, 'foobar foo')
        self.assertEqual(str(b), 'foobar foo')
        self.assertEqual(repr(b), 'foobar foo')

        a = Delayed('foo')
        self.check_only_get_eq(a.function(add, 'bar'), 'foobar')

        a = Delayed('foobar')
        self.check_only_get_eq(a[1], 'o')
        self.check_only_get_eq(a[:-1], 'fooba')
        self.check_only_get_eq(a[1:-1], 'ooba')

    def test_assertions(self):
        a = Delayed('foo')
        self.assertRaises(AssertionError, lambda: a.function(lambda a, b: a + b, 'bar'))

        a = DelayedBase(1, 2)
        self.assertRaises(AssertionError, lambda: a.get())


def add(a, b):
    return a + b


if __name__ == '__main__':
    unittest.main()
