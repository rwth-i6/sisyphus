import unittest

from sisyphus.hash import *


def b():
    pass


class MyEnum(enum.Enum):
    Entry0 = 0
    Entry1 = 1


class MyFoo:
    def __init__(self, some_data: str):
        self.some_data = some_data

    def get_data(self):
        return self.some_data


class HashTest(unittest.TestCase):
    def test_get_object_state(self):
        c = lambda x: x  # noqa: E731

        def d():
            pass

        self.assertEqual(sis_hash_helper(b), b"(function, (tuple, (str, '" + __name__.encode() + b"'), (str, 'b')))")
        self.assertRaises(AssertionError, sis_hash_helper, c)

    def test_get_object_state_cls(self):
        # Note: the hash of a class currently does not depend on get_object_state,
        # but there is special logic in sis_hash_helper for classes,
        # thus it doesn't really matter for the hash what is being returned here.
        # However, this is used by extract_paths, so we test it here.
        s = get_object_state(str)
        self.assertEqual(s, ("builtins", "str"))

    def test_get_object_state_function(self):
        # Note: the hash of a function currently does not depend on get_object_state,
        # but there is special logic in sis_hash_helper for functions,
        # thus it doesn't really matter for the hash what is being returned here.
        # However, this is used by extract_paths, so we test it here.
        s = get_object_state(b)
        self.assertEqual(s, (b.__module__, b.__name__))

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

    def test_bound_method(self):
        first_obj = MyFoo("First")
        second_obj = MyFoo("Second")
        func_hash = sis_hash_helper(MyFoo.get_data)
        bound_to_first_obj_hash = sis_hash_helper(first_obj.get_data)
        bound_to_second_obj_hash = sis_hash_helper(second_obj.get_data)

        self.assertEqual(func_hash, b"(function, (tuple, (str, 'tests.hash_unittest'), (str, 'MyFoo.get_data')))")
        self.assertEqual(
            bound_to_first_obj_hash,
            (
                b"(method, (dict, "
                b"(tuple, (str, '__func__'), " + func_hash + b"), "
                b"(tuple, (str, '__self__'), (MyFoo, (dict, (tuple, (str, 'some_data'), (str, 'First')))))"
                b"))"
            ),
        )
        self.assertNotEqual(bound_to_first_obj_hash, bound_to_second_obj_hash)

    def test_builtin_method_and_func(self):
        func_hash = sis_hash_helper(len)
        unbound_hash = sis_hash_helper(str.lower)
        bound1_hash = sis_hash_helper("a".__add__)
        bound2_hash = sis_hash_helper("b".__add__)
        bound3_hash = sis_hash_helper("a".__mul__)
        bound4_hash = sis_hash_helper("a".lower)

        self.assertEqual(
            func_hash,
            b"(builtin_function_or_method,"
            b" (dict, (tuple, (str, '__name__'), (str, 'len')),"
            b" (tuple, (str, '__self__'), (module, (str, 'builtins')))))",
        )
        self.assertEqual(unbound_hash, b"(method_descriptor, (str, 'str.lower'))")
        self.assertEqual(
            bound1_hash,
            b"(method-wrapper, (dict, (tuple, (str, '__name__'), (str, '__add__')),"
            b" (tuple, (str, '__self__'), (str, 'a'))))",
        )
        self.assertEqual(
            bound2_hash,
            b"(method-wrapper, (dict, (tuple, (str, '__name__'), (str, '__add__')),"
            b" (tuple, (str, '__self__'), (str, 'b'))))",
        )
        self.assertEqual(
            bound3_hash,
            b"(method-wrapper, (dict, (tuple, (str, '__name__'), (str, '__mul__')),"
            b" (tuple, (str, '__self__'), (str, 'a'))))",
        )
        self.assertEqual(
            bound4_hash,
            b"(builtin_function_or_method, (dict, (tuple, (str, '__name__'), (str, 'lower')),"
            b" (tuple, (str, '__self__'), (str, 'a'))))",
        )
        self.assertNotEqual(bound1_hash, bound2_hash)
        self.assertNotEqual(bound1_hash, bound3_hash)
        self.assertNotEqual(bound1_hash, bound4_hash)

    def test_pathlib_Path(self):
        from pathlib import Path

        obj = Path("/etc/passwd")
        self.assertEqual(sis_hash_helper(obj), b"(PosixPath, (str, '/etc/passwd'))")


if __name__ == "__main__":
    unittest.main()
