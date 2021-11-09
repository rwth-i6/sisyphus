import unittest
import os
import glob
from sisyphus.toolkit import mktemp, remove_paths, Path


class MkTemp(unittest.TestCase):

    def test_file(self):
        # create and automatically delete temp file
        with mktemp() as temp:
            assert(len(glob.glob(temp)) == 0)
            with open(temp, 'w'):
                pass
            assert(len(glob.glob(temp)) == 1)
        assert(len(glob.glob(temp)) == 0)

    def test_dir(self):
        # create and automatically delete temp dir
        with mktemp() as temp:
            assert(len(glob.glob(temp)) == 0)
            os.mkdir(temp)
            assert(len(glob.glob(temp)) == 1)
        assert(len(glob.glob(temp)) == 0)


class RemovePath(unittest.TestCase):
    def test_remove_paths(self):
        # create and automatically delete temp dir
        a = (123, 32, ('foobar', 12), frozenset(list(range(9)) + [4, 5, (1, 2, 3)]))
        b = remove_paths(a)
        assert a == b

        a = (123, 32, ('foobar', 12), frozenset(list(range(9)) + [4, 5, (1, 2, 3)]), '/bla')
        b = remove_paths((123, 32, ('foobar', 12), frozenset(list(range(9)) + [4, 5, (1, 2, 3)]), Path('/bla')))
        assert a == b

        a = [123, 32, ('foobar', 12), set(list(range(9)) + [4, 5, (1, 2, 3)]), '/bla']
        b = remove_paths([123, 32, ('foobar', 12), set(list(range(9)) + [4, 5, (1, 2, 3)]), Path('/bla')])
        assert a == b

        class A:
            pass

        a = A()
        a.foo = 1
        a.bar = Path('/bla') + '123'
        b = remove_paths(a)
        a.bar = '/bla123'
        assert a.__dict__ == b.__dict__

        class B:
            __slots__ = ['foo', 'bar', 'empty']

        a = B()
        a.foo = 1
        a.bar = Path('/bla') + '123'
        b = remove_paths(a)
        a.bar = '/bla123'
        assert a.foo == b.foo
        assert a.bar == b.bar
        assert not hasattr(a, 'empyt')
        assert not hasattr(b, 'empyt')


if __name__ == '__main__':
    unittest.main()
