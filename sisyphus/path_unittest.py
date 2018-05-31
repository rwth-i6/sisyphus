import unittest
import os

from sisyphus.job_path import Path


class MockJob(object):

    def __init__(self, path):
        self.path = path

    def _sis_path(self, postfix):
        return os.path.join('work', self.path, postfix)


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

if __name__ == '__main__':
    unittest.main()
