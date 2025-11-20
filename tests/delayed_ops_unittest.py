import unittest
from sisyphus.delayed_ops import *
from sisyphus.hash import short_hash
from sisyphus.job_path import Variable
import sisyphus.toolkit as tk


class DelayedOpsTest(unittest.TestCase):
    def check_only_get_eq(self, a, b):
        """Check that a and b are normally not equal, but are equal after calling get"""
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

        self.check_only_get_eq(a**2, 9)
        self.check_only_get_eq(2**a, 8)
        self.check_only_get_eq(a**2 % 2, 1)
        self.check_only_get_eq(2**a % 2, 0)

        self.check_only_get_eq(a.rformat("foo{:04d} foo"), "foo0003 foo")

    def test_string(self):
        a = Delayed("foo")

        self.check_only_get_eq(a + "bar", "foobar")
        self.check_only_get_eq("bar" + a, "barfoo")

        self.check_only_get_eq(a * 3, "foofoofoo")

        a = Delayed("foo%s foo")
        self.check_only_get_eq(a % "bar", "foobar foo")

        a = Delayed("foo{b} foo")
        self.check_only_get_eq(a.format(b="bar"), "foobar foo")

        a = Delayed("bar")
        self.check_only_get_eq(a.rformat("foo{} foo"), "foobar foo")

        a = Delayed("foobbb foo")
        b = a.replace("bbb", "bar")
        self.check_only_get_eq(b, "foobar foo")
        self.assertEqual(str(b), "foobar foo")
        self.assertEqual(repr(b), "foobar foo")

        a = Delayed("foo")
        self.check_only_get_eq(a.function(add, "bar"), "foobar")

        a = Delayed("foobar")
        self.check_only_get_eq(a[1], "o")
        self.check_only_get_eq(a[:-1], "fooba")
        self.check_only_get_eq(a[1:-1], "ooba")

    def test_join(self):
        delayed_join = DelayedJoin([tk.Path("/random/path"), "/foo/bar"], ";")
        self.check_only_get_eq(delayed_join, "/random/path;/foo/bar")

    def test_assertions(self):
        a = Delayed("foo")
        self.assertRaises(AssertionError, lambda: a.function(lambda a, b: a + b, "bar"))

        a = DelayedBase(1, 2)
        self.assertRaises(AssertionError, lambda: a.get())

    def test_ist_set(self):
        with tk.mktemp() as t1, tk.mktemp() as t2:
            var1 = Variable(t1)
            var2 = Variable(t2)
            c1 = var1 + 5
            c2 = c1 * 8
            c3 = c2 - (var2 % 5)
            c4 = c3.rformat("Hello {}")
            for i in [var1, var2, c1, c2, c3, c4]:
                self.assertEqual(i.is_set(), False)
            var1.set(3)
            for i in [var1, c1, c2]:
                self.assertEqual(i.is_set(), True)
            for i in [var2, c3, c4]:
                self.assertEqual(i.is_set(), False)
            var2.set(9)
            for i in [var1, var2, c1, c2, c3, c4]:
                self.assertEqual(i.is_set(), True)

            self.check_only_get_eq(var1, 3)
            self.check_only_get_eq(var2, 9)
            self.check_only_get_eq(c1, 8)
            self.check_only_get_eq(c2, 64)
            self.check_only_get_eq(c3, 60)
            self.check_only_get_eq(c4, "Hello 60")

    def test_fallback(self):
        with tk.mktemp() as t:
            var = Variable(t)
            self.assertEqual(var.is_set(), False)
            fallback = var.fallback(0)
            self.check_only_get_eq(fallback, 0)
            self.check_only_get_eq(fallback + 5, 5)
            var.set(3)
            self.assertEqual(var.is_set(), True)
            self.check_only_get_eq(fallback + 5, 8)

        with tk.mktemp() as t:
            var = Variable(t)
            var_chain = ((var + 4) % 2) * 42
            fallback = var_chain.rformat("{:05.1f}").fallback(0)
            self.assertEqual(var.is_set(), False)
            self.check_only_get_eq(fallback, 0)
            var.set(3)
            self.assertEqual(var.is_set(), True)
            self.check_only_get_eq(fallback, "042.0")

    def test_call(self):
        func = Delayed(add)
        self.assertEqual(func(2, 3).get(), 5)
        self.assertEqual(func(Delayed(2), Delayed(3)).get(), 5)


def add(a, b):
    return a + b


if __name__ == "__main__":
    unittest.main()
