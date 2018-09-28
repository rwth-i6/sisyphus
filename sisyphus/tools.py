#!/usr/bin/env python3

# Author: Jan-Thorsten Peter <peter@cs.rwth-aachen.de>
import collections
from sisyphus.hash import *
import inspect
import os
import platform
import sys
import shutil
import time
import logging
import subprocess

import sisyphus.global_settings as gs
from sisyphus.block import Block


def get_system_informations(file=sys.stdout):
    print("Uname:", platform.uname(), file=file)
    print("Load:", os.getloadavg(), file=file)


def str_to_GB(m):
    """ Takes a string with size units and converts it into float as GB.
    If only a number is given it assumes it's gigabytes

    :param m:
    :return:
    """
    try:
        m = float(m)
    except ValueError:
        if m[-1] == 'T':
            m = float(m[:-1]) * 1024
        if m[-1] == 'G':
            m = float(m[:-1])
        elif m[-1] == 'M':
            m = float(m[:-1]) / 1024.
        elif m[-1] == 'K':
            m = float(m[:-1]) / 1024. / 1024.
        else:
            assert(False)
    return m


def str_to_hours(t):
    """ Takes a string and converts it into seconds
    If only a number is given it assumes it's hours

    :param m:
    :return:
    """
    try:
        t = int(t)
    except ValueError:
        t = t.split(':')
        assert(len(t) == 3)
        t = int(t[0]) * 3600 + int(t[1]) * 60 + int(t[2])
        t /= 3600
    return t


def extract_paths(args):
    """ Extract all path objects from the given arguments """
    out = set()
    if isinstance(args, Block):
        return out
    if hasattr(args, '_sis_path') and args._sis_path is True:
        out = {args}
    elif isinstance(args, (list, tuple, set)):
        for a in args:
            out = out.union(extract_paths(a))
    elif isinstance(args, dict):
        for k, v in args.items():
            if not type(k) == str or not k.startswith('_sis_'):
                out = out.union(extract_paths(v))
    elif hasattr(args, '__sis_state__') and not inspect.isclass(args):
        out = out.union(extract_paths(args.__sis_state__()))
    elif hasattr(args, '__getstate__') and not inspect.isclass(args):
        out = out.union(extract_paths(args.__getstate__()))
    elif hasattr(args, '__dict__'):
        for k, v in args.__dict__.items():
            if not type(k) == str or not k.startswith('_sis_'):
                out = out.union(extract_paths(v))
    elif hasattr(args, '__slots__'):
        for k in args.__slots__:
            if hasattr(args, k) and not k.startswith('_sis_'):
                a = getattr(args, k)
                out = out.union(extract_paths(a))
    return out


def sis_hash(obj):
    """
    Takes most object and tries to convert the current state into a hash.

    :param object obj:
    :rtype: str
    """
    return gs.SIS_HASH(obj)


class execute_in_dir(object):

    """ Object to be used by the with statement.
    All code after the with will be executed in the given directory,
    working directory will be changed back after with statement.
    e.g.:

    cwd = os.getcwd()
    with execute_in_dir('foo'):
        assert(os.path.join(cwd, 'foo') == os.getcwd())
    assert(cwd) == os.getcwd())
    """

    def __init__(self, workdir):
        self.workdir = workdir

    def __enter__(self):
        self.base_dir = os.getcwd()
        os.chdir(self.workdir)

    def __exit__(self, type, value, traceback):
        os.chdir(self.base_dir)


class cache_result(object):

    """ decorated to cache the result of a function for x_seconds """

    def __init__(self, cache_time=30, force_update=None, clear_cache=None):
        self.cache = {}
        self.time = collections.defaultdict(int)
        self.cache_time = cache_time
        self.force_update = force_update
        self.clear_cache = clear_cache

    def __call__(self, f):
        def cache_f(*args, **kwargs):
            # if clear_cache is given as input parameter clear cache and return
            if self.clear_cache and self.clear_cache in kwargs:
                self.cache = {}
                return

            update = False
            if self.force_update and kwargs.get(self.force_update, False):
                del kwargs[self.force_update]
                update = True

            key = (f, args, kwargs)
            # to make it usable as a hash value
            # if a possible hit is missed we just lose the caching effect
            # which shouldn't happen that often
            key = str(key)

            if not update and time.time() - self.time[key] > self.cache_time or key not in self.cache:
                update = True

            if update:
                ret = f(*args, **kwargs)
                self.cache[key] = ret
                self.time[key] = time.time()
            else:
                ret = self.cache[key]
            return ret
        return cache_f


def sh(command,
       capture_output=False,
       pipefail=True,
       executable=None,
       except_return_codes=(0,),
       sis_quiet=False,
       sis_replace={},
       **kwargs):
    """ Calls a external shell and
    replaces {args} with job inputs, outputs, args
    and executes the command """

    replace = {}
    replace.update(sis_replace)
    replace.update(kwargs)

    command = command.format(**replace)
    if capture_output:
        msg = "Run in Shell (capture output): %s"
    else:
        msg = "Run in Shell: %s"
    msg = msg % command
    if not sis_quiet:
        logging.info(msg)
    sys.stdout.flush()
    sys.stderr.flush()

    if executable is None:
        executable = '/bin/bash'
        if pipefail:
            # this ensures that the job will fail if any part inside of a pipe fails
            command = 'set -ueo pipefail && ' + command

    try:
        if capture_output:
            return subprocess.check_output(command, shell=True, executable=executable).decode()
        else:
            subprocess.check_call(command, shell=True, executable=executable)
    except subprocess.CalledProcessError as e:
        if e.returncode not in except_return_codes:
            raise
        elif capture_output:
            return e.output


def hardlink_or_copy(src, dst, use_symlink_instead_of_copy=False):
    """ Emulate coping of directories by using hardlinks, if hardlink fails copy file.
    Recursively creates new directories and creates hardlinks of all source files into these directories
    if linking files copy file.

    :param src:
    :param dst:
    :return:
    """

    for dirpath, dirnames, filenames in os.walk(src):
        # get relative path to given to source directory
        relpath = dirpath[len(src)+1:]

        # create directory if it doesn't exist
        try:
            os.mkdir(os.path.join(dst, relpath))
        except FileExistsError:
            assert os.path.isdir(os.path.join(dst, relpath))

        # create subdirectories
        for dirname in dirnames:
            try:
                os.mkdir(os.path.join(dst, relpath, dirname))
            except FileExistsError:
                assert os.path.isdir(os.path.join(dst, relpath))

        # link or copy files
        for filename in filenames:
            src_file = os.path.join(dirpath, filename)
            dst_file = os.path.join(dst, relpath, filename)
            try:
                os.link(src_file, dst_file)
            except FileExistsError:
                assert os.path.isfile(dst_file)
            except OSError as e:
                if e.errno != 18:
                    if use_symlink_instead_of_copy:
                        logging.warning('Could not hardlink %s to %s, use symlink' % (src, dst))
                        shutil.copy2(src_file, dst_file)
                    else:
                        logging.warning('Could not hardlink %s to %s, use copy' % (src, dst))
                        os.symlink(os.path.abspath(src), dst)
                else:
                    raise e


def default_handle_exception_interrupt_main_thread(func):
    """
    :param func: any function. usually run in another thread.
      If some exception occurs, it will interrupt the main thread (send KeyboardInterrupt to the main thread).
      If this is run in the main thread itself, it will raise SystemExit(1).
    :return: function func wrapped
    """
    import sys
    import _thread
    import threading

    def wrapped_func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            logging.error("Exception in thread %r:" % threading.current_thread())
            sys.excepthook(*sys.exc_info())
            if threading.current_thread() is not threading.main_thread():
                _thread.interrupt_main()
            raise SystemExit(1)

    return wrapped_func
