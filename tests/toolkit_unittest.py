import unittest
import os
import glob
from sisyphus.toolkit import mktemp


class MkTemp(unittest.TestCase):

    def test_file(self):
        # create and automatically delete temp file
        with mktemp() as temp:
            assert len(glob.glob(temp)) == 0
            with open(temp, 'w'):
                pass
            assert len(glob.glob(temp)) == 1
        assert len(glob.glob(temp)) == 0

    def test_dir(self):
        # create and automatically delete temp dir
        with mktemp() as temp:
            assert len(glob.glob(temp)) == 0
            os.mkdir(temp)
            assert len(glob.glob(temp)) == 1
        assert len(glob.glob(temp)) == 0


if __name__ == '__main__':
    unittest.main()
