import unittest

from sisyphus.hash import *


def b():
    pass


class MyEnum(enum.Enum):
    Entry0 = 0
    Entry1 = 1


class HashTest(unittest.TestCase):
    def test_get_object_state(self):

        c = lambda x: x  # noqa: E731

        def d():
            pass

        self.assertEqual(sis_hash_helper(b), b"(function, (tuple, (str, '" + __name__.encode() + b"'), (str, 'b')))")
        self.assertRaises(AssertionError, sis_hash_helper, c)

    def test_enum(self):
        self.assertEqual(
            sis_hash_helper(MyEnum.Entry1),
            b"(%s, (dict, (tuple, (str, '__objclass__')," % MyEnum.__name__.encode()
            + b" (EnumMeta, (tuple, (str, '%s'), (str, '%s')))),"
            % (MyEnum.__module__.encode(), MyEnum.__name__.encode())
            + b" (tuple, (str, '_name_'), (str, 'Entry1')), (tuple, (str, '_value_'), (int, 1))))",
        )

    def test_functools_partial(self):
        from functools import partial

        obj = partial(int, 42)
        self.assertEqual(
            sis_hash_helper(obj),
            (
                b"(partial, (dict,"
                b" (tuple, (str, 'args'), (tuple, (int, 42))),"
                b" (tuple, (str, 'func'), (type,"
                b" (tuple, (str, 'builtins'), (str, 'int')))),"
                b" (tuple, (str, 'keywords'), (dict))))"
            ),
        )

    def test_pathlib_Path(self):
        from pathlib import Path

        obj = Path("/etc/passwd")
        self.assertEqual(sis_hash_helper(obj), b"(PosixPath, (str, '/etc/passwd'))")


if __name__ == "__main__":
    unittest.main()
