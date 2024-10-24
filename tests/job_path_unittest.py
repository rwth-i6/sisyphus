import unittest
import os
import pickle

from sisyphus import gs
import sisyphus.toolkit as tk
from sisyphus.job_path import Path, Variable
from sisyphus.tools import finished_results_cache


class MockJob(object):
    def __init__(self, path):
        self.path = path

    def _sis_path(self, postfix):
        return os.path.join("work", self.path, postfix)

    def __eq__(self, other):
        return self.path == other.path

    def _sis_finished(self):
        return False

    def path_available(self, path):
        return False


def path_available_false(path):
    return False


def path_available_true(path):
    return True


class PathTest(unittest.TestCase):
    def test_f(self):
        path = Path("out")
        self.assertEqual(str(path), os.path.abspath("out"))

        mjob = MockJob("test/me.1234")
        path = Path("lm.gz", mjob)
        self.assertEqual(str(path), os.path.abspath("work/test/me.1234/output/lm.gz"))

    def test_hash(self):
        path = Path("out")
        self.assertEqual(path._sis_hash(), b"(Path, (tuple, (NoneType), (str, 'out')))")

        mjob = MockJob("test/me.1234")
        path = Path("lm.gz", mjob)
        self.assertEqual(
            path._sis_hash(),
            b"(Path, (tuple, (MockJob, (dict, (tuple, "
            b"(str, 'path'), (str, 'test/me.1234')))), "
            b"(str, 'lm.gz')))",
        )

    def test_overwrite_hash(self):
        path = Path("out", hash_overwrite="foo")
        self.assertEqual(path._sis_hash(), b"(Path, (tuple, (NoneType), (str, 'foo')))")

        mjob = MockJob("test/me.1234")
        path = Path("lm.gz", mjob, hash_overwrite=("foo", "bar"))
        self.assertEqual(path._sis_hash(), b"(Path, (tuple, (str, 'foo'), (str, 'bar')))")

    def test_hash_overwrite_modify(self):
        path = Path("out", hash_overwrite="foo")
        path_join = path.join_right("bar")
        path_append = path.copy_append("bar")
        self.assertEqual(path._sis_hash(), b"(Path, (tuple, (NoneType), (str, 'foo')))")
        self.assertEqual(path_join._sis_hash(), b"(Path, (tuple, (NoneType), (str, 'foo/bar')))")
        self.assertEqual(path_append._sis_hash(), b"(Path, (tuple, (NoneType), (str, 'foobar')))")

        mjob = MockJob("test/me.1234")
        path = Path("lm.gz", mjob, hash_overwrite=("foo", "bar"))
        path_join = path.join_right("baz")
        path_append = path.copy_append("baz")

        self.assertEqual(path._sis_hash(), b"(Path, (tuple, (str, 'foo'), (str, 'bar')))")
        self.assertEqual(path_join._sis_hash(), b"(Path, (tuple, (str, 'foo'), (str, 'bar/baz')))")
        self.assertEqual(path_append._sis_hash(), b"(Path, (tuple, (str, 'foo'), (str, 'barbaz')))")

    def test_pickle(self):
        def pickle_and_check(path):
            with tk.mktemp() as pickle_path:
                with open(pickle_path, "wb") as f:
                    pickle.dump(path, f)
                with open(pickle_path, "rb") as f:
                    path_unpickled = pickle.load(f)

            self.assertEqual(path.get_path(), path_unpickled.get_path())

            if gs.INCLUDE_CREATOR_STATE:
                self.assertEqual(path.creator, path_unpickled.creator)
            else:
                self.assertIsNone(path_unpickled.creator)

            excluded_keys = {'creator', 'path'}
            original_attrs = {k: v for k, v in path.__dict__.items() if k not in excluded_keys}
            unpickled_attrs = {k: v for k, v in path_unpickled.__dict__.items() if k not in excluded_keys}
            self.assertEqual(original_attrs, unpickled_attrs)



        pickle_and_check(Path("out"))

        path = Path("lm.gz", MockJob("test/me.1234"))
        pickle_and_check(path)

        path = Path("lm.gz", MockJob("test/me.1234"), available=path_available_false)
        pickle_and_check(path)

        path = Variable("lm.gz", MockJob("test/me.1234"))
        pickle_and_check(path)

    def test_path_available(self):
        mjob = MockJob("test/me.1234")
        path = Path("lm.gz", mjob)
        self.assertEqual(path.available(), False)

        mjob._sis_finished = lambda: True
        mjob.path_available = lambda path: True
        self.assertEqual(path.available(), True)

        with tk.mktemp() as test_path:
            path = Path(test_path)
            self.assertEqual(path.available(), False)
            with open(test_path, "wb") as _:
                pass
            self.assertEqual(path.available(), True)

        finished_results_cache.reset()
        path = Path("lm.gz", mjob, available=path_available_false)
        self.assertEqual(path.available(), False)
        path = Path("lm.gz", mjob, available=path_available_true)
        self.assertEqual(path.available(), True)

    def check_only_get_eq(self, a, b):
        """Check that a and b are normally not equal, but are equal after calling get"""
        self.assertNotEqual(a, b)
        self.assertEqual(a.get(), b)

    def test_path_delay(self):
        mjob = MockJob("test/me.1234")
        path = Path("lm.gz", mjob)

        self.check_only_get_eq(path, str(path))
        self.check_only_get_eq(path + ".foo", str(path) + ".foo")
        self.check_only_get_eq(path[:-3], str(path)[:-3])
        self.check_only_get_eq(path[-2], str(path)[-2])
        self.check_only_get_eq(path[:-3] + ".foo", str(path)[:-3] + ".foo")

        with tk.mktemp() as t:
            var = Variable(t)
            var.set(3)
            self.check_only_get_eq(var + 4, 7)
            self.check_only_get_eq(4 + var, 7)
            self.check_only_get_eq(var * 4, 12)
            self.check_only_get_eq(4 * var, 12)


if __name__ == "__main__":
    unittest.main()
