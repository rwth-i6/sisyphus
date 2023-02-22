import hashlib
import os
import sys
import shutil
import time
import unittest
import collections

from sisyphus import job_path
from sisyphus.tools import execute_in_dir, cache_result, sis_hash, hardlink_or_copy
from sisyphus.hash import sis_hash_helper
import sisyphus.global_settings as gs

sys.path.append(gs.TEST_DIR)
gs.SIS_HASH = lambda x: hashlib.md5(sis_hash_helper(x)).hexdigest()


class ExecuteInDir(unittest.TestCase):

    def test_f(self):
        cwd = os.getcwd()
        recipe_test_dir = os.path.join(gs.TEST_DIR, 'recipe/task')
        with execute_in_dir(recipe_test_dir):
            self.assertEqual(os.path.join(cwd, recipe_test_dir), os.getcwd())
        self.assertEqual(cwd, os.getcwd())

        try:
            with execute_in_dir(recipe_test_dir):
                self.assertEqual(os.path.join(cwd, recipe_test_dir), os.getcwd())
                assert(False)
        except AssertionError:
            self.assertEqual(cwd, os.getcwd())
        self.assertEqual(cwd, os.getcwd())


class FunctionCache(unittest.TestCase):

    @cache_result(1, 'force')
    def f(self, arg=1):
        return time.time()

    def test_f(self):
        a = self.f()
        self.assertEqual(a, self.f())
        time.sleep(0.1)
        self.assertEqual(a, self.f())

        time.sleep(1)
        b = self.f()
        self.assertNotEqual(a, b)
        self.assertNotEqual(b, self.f(2))
        c = self.f(force=True)
        self.assertNotEqual(b, c)
        self.assertEqual(c, self.f())


class MockClass(object):

    def __init__(self, a, b):
        self.a = a
        self.b = b


class SisHash(unittest.TestCase):

    def test_f(self):
        from recipe.task import test
        Point = collections.namedtuple('Point', ['x', 'y'])
        for obj, ref, hash_ref in [
                (0, b'(int, 0)', '32c41e3ec33997dc8f7aa39d8c00317b'),
                ('0', b"(str, '0')", None),
                (b'0', b"(bytes, 0)", None),
                (b'\x00', b'(bytes, \x00)', None),
                ((8 + 6j), b"(complex, (8+6j))", None),
                (None, b"(NoneType)", None),
                ([1, 2, -1], b"(list, (int, 1), (int, 2), (int, -1))", None),
                ((1, 2, -1), b"(tuple, (int, 1), (int, 2), (int, -1))", None),
                ({1, 2, -1}, b"(set, (int, -1), (int, 1), (int, 2))", None),
                (frozenset({1, 2, -1}), b"(frozenset, (int, -1), (int, 1), (int, 2))", None),
                ({'foo': 1, 'bar': -1},
                 b"(dict, (tuple, (str, 'bar'), (int, -1)), (tuple, (str, 'foo'), (int, 1)))", None),

                (MockClass(1, 2),
                 b"(MockClass, (dict, (tuple, (str, 'a'), (int, 1)), (tuple, (str, 'b'), (int, 2))))", None),
                (Point(3, 5),
                 b'(Point, (tuple, (tuple, (int, 3), (int, 5)), (dict)))', None),

                (test.Test('foo'), b"task/test/Test.7be358a10ed713206e44d0ab965e8612", None),
                (job_path.Path('foo/bar'), b"(Path, (tuple, (NoneType), (str, 'foo/bar')))", None),
                (b'0' * 4087, b"(bytes, " + b'0' * 4087 + b")", None),
                (b'0' * 4088,
                 b't\xe0\xf8\xbb\xfd\xe6\xfaN\xa6\xac`\x7f\xd3\xfeZ\xa3c6z\xe8\xc7\x869^\xa1\x011\x8e\xfcx\xa1V', None),
                ({
                    MockClass(1, 2): 999,
                    test.Test('foo'): 777,
                    'foo': test.Test('bar'),
                    'bar': job_path.Path('foo/bar'),
                    job_path.Path('foo/bar'): 'bar'
                },
                    b"(dict, "
                    b"(tuple, (MockClass, "
                    b"(dict, (tuple, (str, 'a'), (int, 1)), (tuple, (str, 'b'), (int, 2)))), (int, 999)), "
                    b"(tuple, (Path, (tuple, (NoneType), (str, 'foo/bar'))), (str, 'bar')), "
                    b"(tuple, (str, 'bar'), (Path, (tuple, (NoneType), (str, 'foo/bar')))), "
                    b"(tuple, (str, 'foo'), task/test/Test.84bbb5730368c68c8151b56c3ede6c5e), "
                    b"(tuple, task/test/Test.7be358a10ed713206e44d0ab965e8612, (int, 777)))",
                    None),
        ]:
            res = sis_hash_helper(obj)
            self.assertEqual(res, ref)
            if hash_ref is None:
                hash_ref = hashlib.md5(ref).hexdigest()
            hash_res = sis_hash(obj)
            self.assertEqual(hash_res, hash_ref)


class HardCopy(unittest.TestCase):

    def test_copy(self):
        src = '%s/recipe' % gs.TEST_DIR
        dst = '%s/recipe_copy_test' % gs.TEST_DIR
        hardlink_or_copy(src, dst)
        for i, j in zip(os.walk(src), os.walk(dst)):
            self.assertEqual(i[1:], j[1:])
            self.assertEqual(i[0][len(src):], j[0][len(dst):])
        shutil.rmtree(dst)


if __name__ == '__main__':
    unittest.main()
