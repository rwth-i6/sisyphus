import unittest
import os
import pickle

import sisyphus.toolkit as tk
from sisyphus.job_path import Path, Variable


class MockJob(object):

    def __init__(self, path):
        self.path = path

    def _sis_path(self, postfix):
        return os.path.join('work', self.path, postfix)

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
        path = Path('out')
        self.assertEqual(str(path), os.path.abspath('out'))

        mjob = MockJob('test/me.1234')
        path = Path('lm.gz', mjob)
        self.assertEqual(str(path), os.path.abspath('work/test/me.1234/output/lm.gz'))

    def test_hash(self):
        path = Path('out')
        self.assertEqual(path._sis_hash(),
                         b"(Path, (tuple, (NoneType), (str, 'out')))")

        mjob = MockJob('test/me.1234')
        path = Path('lm.gz', mjob)
        self.assertEqual(path._sis_hash(),
                         b"(Path, (tuple, (MockJob, (dict, (tuple, "
                         b"(str, 'path'), (str, 'test/me.1234')))), "
                         b"(str, 'lm.gz')))")

    def test_overwrite_hash(self):
        path = Path('out', hash_overwrite='foo')
        self.assertEqual(path._sis_hash(),
                         b"(Path, (tuple, (NoneType), (str, 'foo')))")

        mjob = MockJob('test/me.1234')
        path = Path('lm.gz', mjob, hash_overwrite=('foo', 'bar'))
        self.assertEqual(path._sis_hash(),
                         b"(Path, (tuple, (str, 'foo'), (str, 'bar')))")

    def test_pickle(self):
        def pickle_and_check(path):
            with tk.mktemp() as pickle_path:
                with open(pickle_path, 'wb') as f:
                    pickle.dump(path, f)
                with open(pickle_path, 'rb') as f:
                    path_unpickled = pickle.load(f)
            self.assertEqual(path.__dict__, path_unpickled.__dict__)

        pickle_and_check(Path('out'))

        path = Path('lm.gz', MockJob('test/me.1234'))
        pickle_and_check(path)

        path = Path('lm.gz', MockJob('test/me.1234'), available=path_available_false)
        pickle_and_check(path)

        path = Variable('lm.gz', MockJob('test/me.1234'))
        pickle_and_check(path)

    def test_path_available(self):
        mjob = MockJob('test/me.1234')
        path = Path('lm.gz', mjob)
        self.assertEqual(path.available(), False)

        mjob._sis_finished = lambda: True
        mjob.path_available = lambda path: True
        self.assertEqual(path.available(), True)

        with tk.mktemp() as test_path:
            path = Path(test_path)
            self.assertEqual(path.available(), False)
            with open(test_path, 'wb') as f:
                pass
            self.assertEqual(path.available(), True)

        path = Path('lm.gz', mjob, available=path_available_false)
        self.assertEqual(path.available(), False)
        path = Path('lm.gz', mjob, available=path_available_true)
        self.assertEqual(path.available(), True)

if __name__ == '__main__':
    unittest.main()
