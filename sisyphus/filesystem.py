#!/usr/bin/env python

import os
import time
import collections
import types
from sisyphus.tools import cache_result

from fuse import FUSE, Operations
from sisyphus import toolkit
from sisyphus.block import all_root_blocks

Symlink = collections.namedtuple('Symlink', ["target"])


class FilesystemObject(object):
    pass


class Symlink(FilesystemObject):

    def __init__(self, target):
        self.target = target

    def getattr(self):
        return {'st_size': 0,
                'st_mode': 41471,  # int('120777', 8)
                }


class File(FilesystemObject):

    def __init__(self, obj):
        self.obj = obj

    def getattr(self):
        return {'st_size': len(repr(self.obj)) + 1,
                'st_mode': 33060,  # int('0100444', 8)
                }

    def __str__(self):
        return repr(self.obj) + '\n'


class Directory(FilesystemObject):

    def __init__(self, obj):
        self.obj = obj

    def getattr(self):
        return {'st_size': 0,
                'st_mode': 16749,  # int('40555' , 8)
                }

    def __iter__(self):
        """
        List all items of the directory

        :return:
        """
        obj = self.obj
        if hasattr(obj, '__fs_like__'):
            obj = obj.__fs_like__()

        if hasattr(obj, '__fs_directory__'):
            yield from obj.__fs_directory__()
        elif isinstance(obj, dict):
            yield '_file'
            for r in obj:
                yield r.replace('/', '_')
        elif isinstance(obj, (tuple, list, set, frozenset)):
            yield '_file'
            for r in range(len(obj)):
                yield str(r)
        else:
            yield '_file'
            for r in dir(obj):
                if r.startswith('_') or isinstance(r, (types.FunctionType, types.MethodType)):
                    pass
                else:
                    yield r

    def get(self, key, history, full_path, mountpoint):
        """ Get entry of directory by name

        :param key:
        :return:
        """
        step = key

        # Select next object
        obj = self.obj
        if hasattr(obj, '__fs_like__'):
            obj = obj.__fs_like__()

        if hasattr(obj, '__fs_get__'):
            obj_type, obj = obj.__fs_get__(step)
            if obj_type == 'symlink':
                obj = Symlink(obj)
        else:
            if step == '_file':
                return File(obj)
            elif isinstance(obj, dict):
                obj = obj.get(step, None)
                if obj is None and '_' in step:
                    for k, v in self.obj.items():
                        if k.replace('/', '_') == step:
                            obj = v
                            break
            elif isinstance(obj, (tuple, list, set, frozenset)):
                if isinstance(obj, (set, frozenset)):
                    obj_list = sorted(obj)
                else:
                    obj_list = obj
                try:
                    obj = obj_list[int(step)]
                except ValueError:
                    raise FileNotFoundError("[Errno 2] No such file or directory: '%s'" % full_path)
            else:
                obj = getattr(obj, step, None)

        # Change object type if needed
        if obj is None or isinstance(obj, (float, int, str)):
            return File(obj)
        elif isinstance(obj, Symlink):
            return obj
        elif hasattr(obj, '__fs_symlink__') and \
                obj.__fs_symlink__(mountpoint, full_path, history):
            return Symlink(obj.__fs_symlink__(mountpoint, full_path, history))
        # replace obj already in path with link to avoid loops
        elif obj in history:
            index = history.index(obj)
            return Symlink(os.path.sep.join(['..'] * (len(history) - index - 1)))
        else:
            history.append(obj)
            return Directory(obj)


class SISFilesystem(Operations):

    def __init__(self, work_dir, sis_graph, mountpoint):
        self.work_dir = work_dir
        self.sis_graph = sis_graph
        self.uid = os.getuid()
        self.gid = os.getgid()
        self.cache = ("", None, None)
        self.mountpoint = mountpoint

    # Helpers
    # =======

    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.work_dir, partial)
        return path

    def _verbose__call__(self, op, path, *args):
        """ rename to __call__ to get a very verbose filesystem call
        used for debugging

        :param op:
        :param path:
        :param args:
        :return:
        """
        print('->', op, path, args[0] if args else '')
        ret = '[Unhandled Exception]'
        try:
            ret = getattr(self, op)(path, *args)
            print(ret)
            return ret
        except OSError as e:
            ret = str(e)
            print('OSError', ret, e, type(e))
            raise
        except IOError as e:
            ret = str(e)
            print('IOError', ret, e, type(e))
            raise OSError(*e.args)
        except Exception as e:
            ret = str(e)
            print('Exception', ret, e, type(e))
            raise e
        finally:
            print('<-', op)
            pass

    @cache_result()
    def get_obj(self, path):
        dirname = os.path.dirname(path)
        if self.cache[0] == path:
            return self.cache[1]
        elif self.cache[0] == dirname:
            obj = self.cache[1]
            history = self.cache[2][:]
            steps = [os.path.basename(path)]
        else:
            root = {'output': self.sis_graph.targets_dict,
                    'jobs': self.sis_graph.job_directory_structure(),
                    'blocks': {block.name: block for block in all_root_blocks}}
            history = [root]
            obj = Directory(root)
            steps = path.split(os.path.sep)

        try:
            for step in steps:
                if step == '':
                    continue
                elif isinstance(obj, Directory):
                    obj = obj.get(step, history, path, self.mountpoint)
                else:
                    raise KeyError(os.path.join(path, step))
        except KeyError:
            raise FileNotFoundError("[Errno 2] No such file or directory: '%s'" % path)
        else:
            if isinstance(obj, Directory):
                self.cache = (path, obj, history)
            return obj

    # Filesystem methods
    # ==================

    def getattr(self, path, fh=None):
        obj = self.get_obj(path)
        base = {'st_ctime': time.time(),
                'st_atime': time.time(),
                'st_uid': self.uid,
                'st_nlink': 1,
                'st_mtime': time.time(),
                'st_gid': self.gid}
        base.update(obj.getattr())
        return base

    def readdir(self, path, fh):
        obj = self.get_obj(path)
        if isinstance(obj, Directory):
            yield from obj
        else:
            raise OSError("OSError: [Errno 22] Invalid argument: '%s'" % path)

    def readlink(self, path):
        obj = self.get_obj(path)
        if isinstance(obj, Symlink):
            return obj.target
        else:
            raise OSError("OSError: [Errno 22] Invalid argument: '%s'" % path)

    def read(self, path, length, offset, fh):
        obj = self.get_obj(path)
        if isinstance(obj, File):
            return str(obj)[offset:offset + length].encode('utf8')
        else:
            raise OSError("OSError: [Errno 22] Invalid argument: '%s'" % path)


def start(work_dir, sis_graph, mountpoint):
    FUSE(SISFilesystem(work_dir, sis_graph, mountpoint),
         mountpoint,
         foreground=True)
