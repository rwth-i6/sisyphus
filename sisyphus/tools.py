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
import linecache
import tracemalloc

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
        t = float(t)
    except ValueError:
        t = t.split(':')
        assert(len(t) == 3)
        t = int(t[0]) * 3600 + int(t[1]) * 60 + int(t[2])
        t /= 3600.0
    return t 


def extract_paths(args):
    """
    Extract all path objects from the given arguments.

    :rtype: set
    """
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
       include_stderr=False,
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
            return subprocess.check_output(command, shell=True, executable=executable,
                                           stderr=subprocess.STDOUT if include_stderr else None).decode()
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


def dump_all_thread_tracebacks(exclude_thread_ids=None, exclude_self=False, file=sys.stderr):
    """
    :param set[int]|None exclude_thread_ids: set|list of thread.ident to exclude
    :param bool exclude_self:
    """
    from traceback import print_stack, walk_stack
    from multiprocessing.pool import worker as mp_worker
    from multiprocessing.pool import Pool
    from queue import Queue
    import threading

    if not hasattr(sys, "_current_frames"):
        print("Does not have sys._current_frames, cannot get thread tracebacks.", file=file)
        return
    if exclude_thread_ids is None:
        exclude_thread_ids = set()
    exclude_thread_ids = set(exclude_thread_ids)
    if exclude_self:
        exclude_thread_ids.add(threading.current_thread().ident)

    print("", file=file)
    threads = {t.ident: t for t in threading.enumerate()}
    for tid, stack in sorted(sys._current_frames().items()):
        # This is a bug in earlier Python versions.
        # http://bugs.python.org/issue17094
        # Note that this leaves out all threads not created via the threading module.
        if tid not in threads:
            continue
        tags = []
        thread = threads.get(tid)
        if thread:
            assert isinstance(thread, threading.Thread)
            if thread is threading.current_thread():
                tags += ["current"]
            if thread is threading.main_thread():
                tags += ["main"]
            tags += [str(thread)]
        else:
            tags += ["unknown with id %i" % tid]
        print("Thread %s:" % ", ".join(tags), file=file)
        if tid in exclude_thread_ids:
            print("(Excluded thread.)\n", file=file)
            continue
        stack_frames = [f[0] for f in walk_stack(stack)]
        stack_func_code = [f.f_code for f in stack_frames]
        if mp_worker.__code__ in stack_func_code:
            i = stack_func_code.index(mp_worker.__code__)
            if i > 0 and stack_func_code[i - 1] is Queue.get.__code__:
                print("(Exclude multiprocessing idling worker.)\n", file=file)
                continue
        if Pool._handle_tasks.__code__ in stack_func_code:
            i = stack_func_code.index(Pool._handle_tasks.__code__)
            if i > 0 and stack_func_code[i - 1] is Queue.get.__code__:
                print("(Exclude multiprocessing idling task handler.)\n", file=file)
                continue
        if Pool._handle_workers.__code__ in stack_func_code:
            i = stack_func_code.index(Pool._handle_workers.__code__)
            if i == 0:  # time.sleep is native, thus not on the stack
                print("(Exclude multiprocessing idling worker handler.)\n", file=file)
                continue
        if Pool._handle_results.__code__ in stack_func_code:
            i = stack_func_code.index(Pool._handle_results.__code__)
            if i > 0 and stack_func_code[i - 1] is Queue.get.__code__:
                print("(Exclude multiprocessing idling result handler.)\n", file=file)
                continue
        print_stack(stack, file=file)
        print("", file=file)
    print("That were all threads.", file=file)


def format_signum(signum):
    """
    :param int signum:
    :return: string "signum (signame)"
    :rtype: str
    """
    import signal
    signum_to_signame = {
        k: v for v, k in reversed(sorted(signal.__dict__.items()))
        if v.startswith('SIG') and not v.startswith('SIG_')}
    return "%s (%s)" % (signum, signum_to_signame.get(signum, "unknown"))


def signal_handler(signum, frame):
    """
    Prints a message on stdout and dump all thread stacks.

    :param int signum: e.g. signal.SIGUSR1
    :param frame: ignored, will dump all threads
    """
    print("Signal handler: got signal %s" % format_signum(signum), file=sys.stderr)
    dump_all_thread_tracebacks(file=sys.stderr)


def install_signal_handler_if_default(signum, exceptions_are_fatal=False):
    """
    :param int signum: e.g. signal.SIGUSR1
    :param bool exceptions_are_fatal: if True, will reraise any exceptions. if False, will just print a message
    :return: True iff no exception, False otherwise. not necessarily that we registered our own handler
    :rtype: bool
    """
    try:
        import signal
        if signal.getsignal(signum) == signal.SIG_DFL:
            signal.signal(signum, signal_handler)
        return True
    except Exception as exc:
        if exceptions_are_fatal:
            raise
        print("Cannot install signal handler for signal %s, exception %s" % (format_signum(signum), exc))
    return False


def maybe_install_signal_handers():
    import signal
    install_signal_handler_if_default(signal.SIGUSR1)
    install_signal_handler_if_default(signal.SIGUSR2)


class MemoryProfiler:
    def __init__(self, log_stream, line_limit=10, min_change=512000):
        self.log_stream = log_stream
        self.limit = line_limit
        tracemalloc.start()
        self.min_change = min_change
        self.last_total = 0

    def snapshot(self):
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('lineno')
        total = sum(stat.size for stat in top_stats)
        if abs(self.last_total - total) < self.min_change:
            return

        self.last_total = total

        self.log_stream.write("Top %s lines at %s\n" % (self.limit, time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())))
        for index, stat in enumerate(top_stats[:self.limit], 1):
            frame = stat.traceback[0]
            # replace "/path/to/module/file.py" with "module/file.py"
            filename = os.sep.join(frame.filename.split(os.sep)[-2:])
            self.log_stream.write("#%s: %s:%s: %.1f KiB\n"
                  % (index, filename, frame.lineno, stat.size / 1024))
            line = linecache.getline(frame.filename, frame.lineno).strip()
            if line:
                self.log_stream.write('    %s\n' % line)

        other = top_stats[self.limit:]
        if other:
            size = sum(stat.size for stat in other)
            self.log_stream.write("%s other: %.1f KiB\n" % (len(other), size / 1024))
        self.log_stream.write("Total allocated size: %.1f KiB\n\n" % (total / 1024))
        self.log_stream.flush()


class EnvironmentModifier:
    """
    A class to cleanup the environment before a job starts
    """

    def __init__(self):
        self.keep_vars = set()
        self.set_vars = {}

    def keep(self, var):
        if type(var) == str:
            self.keep_vars.add(var)
        else:
            self.keep_vars.update(var)

    def set(self, var, value=None):
        if type(var) == dict:
            self.set_vars.update(var)
        else:
            self.set_vars[var] = value

    def modify_environment(self):
        import os
        import string

        orig_env = dict(os.environ)
        keys = list(os.environ.keys())
        for k in keys:
            if k not in self.keep_vars:
                del os.environ[k]
        for k, v in self.set_vars.items():
            if type(v) == str:
                os.environ[k] = string.Template(v).substitute(orig_env)
            else:
                os.environ[k] = str(v)

        for k, v in os.environ.items():
            logging.debug('environment var %s=%s' % (k, v))

    def __repr__(self):
        return repr(self.keep_vars) + ' ' + repr(self.set_vars)
