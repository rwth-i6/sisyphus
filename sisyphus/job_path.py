# Author: Jan-Thorsten Peter <peter@cs.rwth-aachen.de>

import os
import logging
import gzip
import pickle
import warnings
from functools import wraps

import sisyphus.global_settings as gs
from sisyphus.delayed_ops import DelayedBase
from sisyphus.hash import sis_hash_helper
from sisyphus.tools import finished_results_cache


def check_is_worker(get_func):
    """
    Prohibits calling a get function while being in the "graph" thread, thus
    causing bugs/inconsistent behavior in Sisyphus.
    """

    @wraps(get_func)
    def check(*args, **kwargs):
        if gs.DELAYED_CHECK_FOR_WORKER:
            from sisyphus.toolkit import running_in_worker

            assert running_in_worker()
        return get_func(*args, **kwargs)

    return check


class VariableNotSet(Exception):
    """Variable is not set"""

    pass


class NoBackup:
    """Used to mark that a Variable has no backup value set"""

    pass


class AbstractPath(DelayedBase):
    """
    Base class for Path and Variable.
    Their main function is to connect the outputs of a Job to the input of another Job.

    Side node: Parts of the code structure can be explained by the fact that the current
    implementation of this class use to be the Path class and Variable its subclass.
    """

    _sis_path = True
    cacheing_enabled = False

    # Update RelPath in toolkit if position of hash_overwrite is changed
    def __init__(self, path, creator=None, cached=False, hash_overwrite=None, tags=None, available=None):
        """
        :param str path: Path to file, if creator is given relative to it's output directory
        :param Job|None creator: Job that creates output file
        :param bool cached: use file caching, via gs.file_caching, e.g. using cache manager
        :param str|None hash_overwrite:
        :param set|None tags:
        :param function|None available: Overwrite function which tests if path is available.
                                        Gets path as input and must be pickleable
        """

        if gs.WARNING_ABSPATH and path.startswith(gs.BASE_DIR) and not hash_overwrite:
            logging.warning(
                "Creating absolute path inside current work directory: %s "
                "(disable with WARNING_ABSPATH=False)" % path
            )
        assert isinstance(path, str)
        self.creator = creator
        self.users = set()

        self.path = path
        self.cached = cached
        self.hash_overwrite = hash_overwrite
        self._tags = tags

        self._available = available

    @property
    def hash_overwrite(self):
        return self._hash_overwrite

    @hash_overwrite.setter
    def hash_overwrite(self, value):
        if value is not None:
            assert_msg = "sis_hash for path must be str or tuple of length 2"
            if isinstance(value, tuple):
                assert len(value) == 2, assert_msg
            else:
                assert isinstance(value, str), assert_msg
                value = (None, value)
        self._hash_overwrite = value

    def keep_value(self, value):
        if self.creator:
            self.creator.keep_value(value)
        else:
            logging.warning("Try to set keep value for input path: %s" % str(self))

    @property
    def tags(self):
        if self._tags is None:
            if self.creator is None:
                return set()
            else:
                return self.creator.tags
        else:
            return self._tags

    @tags.setter
    def tags(self, value):
        self._tags = value

    def add_user(self, user):
        """
        Adds user to this path

        :param sisyphus.job.Job user:
        """
        assert hasattr(self, "users"), "May happens during unpickling, change to add user if needed"
        self.users.add(user)

    def _sis_hash(self):
        if self.hash_overwrite is None:
            creator = self.creator
            path = self.path
        else:
            creator, path = self.hash_overwrite
        if hasattr(creator, "_sis_id"):
            creator = f"{creator._sis_id()}/{gs.JOB_OUTPUT}"
        return b"(Path, " + sis_hash_helper((creator, path)) + b")"

    @finished_results_cache.caching(get_key=lambda self, debug_info=None: ("available", self.rel_path()))
    def available(self, debug_info=None):
        """Returns True if the computations creating the path are completed
        :return:
        """

        # Use custom set function, check hasattr for backwards compatibility
        if hasattr(self, "_available") and self._available:
            return self._available(self)

        path = self.get_path()
        if self.creator is None:
            return os.path.isfile(path) or os.path.isdir(path)
        else:
            job_path_available = self.creator.path_available(self)
            if self.creator._sis_finished() and not job_path_available:
                if debug_info:
                    logging.warning(
                        "Job marked as finished but requested output is not available: %s %s" % (self, debug_info)
                    )
                else:
                    logging.warning("Job marked as finished but requested output is not available: %s" % self)
            return job_path_available

    # TODO Move this to toolkit cleanup together with job method
    def get_needed_jobs(self, visited):
        """Return all jobs leading to this path"""
        if self.creator is None:
            return set()
        else:
            return self.creator._sis_get_needed_jobs(visited)

    def rel_path(self):
        """
        :return: a string with the relative path to this file
        :rtype: str
        """
        if self.creator is None:
            return self.path
        else:
            return f"{self.creator._sis_path(gs.JOB_OUTPUT)}/{self.path}"

    def get_path(self) -> str:
        """
        :return: a string with the absolute path to this file
        :rtype: str
        """
        path = self.rel_path()
        if os.path.isabs(path):
            return path
        else:
            return f"{gs.BASE_DIR}/{path}"

    def get_cached_path(self) -> str:
        if Path.cacheing_enabled and self.cached:
            return gs.file_caching(self.get_path())
        else:
            return self.get_path()

    @check_is_worker
    def get(self):
        return self.get_path()

    def __lt__(self, other):
        """
        Define smaller than other by first comparing the creator sis id, next the path

        :param other:
        :return:
        """
        if not isinstance(other, AbstractPath):
            assert False, "Cannot compare path to none path"

        def creator_to_str(c):
            if isinstance(c, str):
                return c
            elif hasattr(c, "_sis_id"):
                return c._sis_id()
            elif c is None:
                return str(c)
            else:
                assert False, "User of path is not a job"

        s = "%s %s" % (creator_to_str(self.creator), self.path)
        o = "%s %s" % (creator_to_str(other.creator), other.path)
        return s < o

    def __eq__(self, other):
        if type(self) != type(other):
            return False

        # TODO Check how uninitialized object should behave here
        if len(self.__dict__) == len(other.__dict__) == 0:
            return True

        creator_equal = self.creator == other.creator
        path_equal = self.path == other.path
        return creator_equal and path_equal

    def __hash__(self):
        # TODO Check how uninitialized object should behave here
        return hash((self.__dict__.get("creator"), self.__dict__.get("path")))

    def __getstate__(self):
        """Skips exporting users
        :return:
        """
        d = self.__dict__.copy()
        del d["users"]
        return d

    def __setstate__(self, state):
        assert "users" not in state
        for k, v in state.items():
            setattr(self, k, v)
        if not hasattr(self, "users"):
            self.users = set()


class Path(AbstractPath):
    """
    Object do hold the connecting path to files:

    that are exchanged between jobs
    each path can have a creator or a direct pass to the target and many users.
    """

    path_type = "Path"

    def __str__(self):
        return self.get_cached_path()

    def __repr__(self):
        if gs.LEGACY_PATH_CONVERSION:
            return repr(str(self))
        else:
            return "<Path %s>" % self.get_path()

    def copy(self):
        """Creates a copy of this Path"""
        new = Path("")
        new.__setstate__(self.__getstate__())
        return new

    def copy_append(self, suffix):
        """Returns a copy of this Path with the given suffix appended to it and updates hash_overwrite"""
        new = self.copy()
        if self.hash_overwrite:
            c, o = self.hash_overwrite
            new.hash_overwrite = (c, o + suffix)
        new.path += suffix
        return new

    def join_right(self, other):
        """Returns copy of local Path joined with given string using '/' and updates hash_overwrite"""
        return self.copy_append("/" + other)

    def size(self):
        """DEPRECATED: Return file size if file exists, else return None"""
        warnings.warn(
            "Path.size() is deprecated, accessing files should be done explicitly", category=DeprecationWarning
        )

        assert self.available(), "Path not ready: %s" % str(self.get_path())
        return os.path.getsize(self.get_path())

    def estimate_text_size(self):
        """DEPRECATED: Returns rough estimated size of a text file
        file is not zipped => return original size
        file is zipped => multiply size by 3.5
        """
        warnings.warn(
            "Path.estimate_text_size() is deprecated, accessing files should be done explicitly",
            category=DeprecationWarning,
        )
        if self.is_zipped():
            return int(self.size() * 3.5)
        else:
            return self.size()

    def lines(self):
        """DEPRECATED: Returns the number of lines in file"""
        warnings.warn(
            "Path.lines() is deprecated, accessing files should be done explicitly", category=DeprecationWarning
        )

        if self.is_zipped():
            f = gzip.open(str(self))
        else:
            f = open(str(self))
        i = 0
        for line in f:
            i += 1
        return i

    def is_zipped(self):
        """DEPRECATED: Returns if file is zipped:

        Returns:
        None if file doesn't exists
        true if file is zipped
        false otherwise"""
        warnings.warn(
            "Path.is_zipped() is deprecated, accessing files should be done explicitly", category=DeprecationWarning
        )

        if not self.available():
            return None

        filename = self.get_path()
        # test file header, this value will be returned
        with open(filename, "br") as test_file:
            file_zipped = test_file.read(2) == b"\x1f\x8b"
        # check name, just as sanity check
        name_zipped = filename.endswith(".gz")

        if file_zipped and not name_zipped:
            logging.warning("File is zippped, but does not end with gz: %s", filename)
        if not file_zipped and name_zipped:
            logging.warning("File is not zippped, but ends with gz: %s", filename)

        return file_zipped

    # Filesystem functions
    def __fs_directory__(self):
        """Returns all items that should be listed by virtual filesystem
        :param job:
        :return:
        """
        yield "file"
        yield "f"
        if self.creator is not None:
            yield "creator"
            yield "c"
            yield "_" + self.creator._sis_id().replace(os.path.sep, "_")
        yield "users"
        yield "u"

    def __fs_get__(self, step):
        if "file".startswith(step):
            return "symlink", self.get_path()
        elif self.creator and (
            "creator".startswith(step) or ("_" + self.creator._sis_id().replace(os.path.sep, "_")).startswith(step)
        ):
            return None, self.creator
        elif "users".startswith(step):
            return None, self.users
        else:
            raise KeyError(step)

    def __fspath__(self) -> str:
        return self.get_cached_path()


class Variable(AbstractPath):
    path_type = "Variable"

    def __init__(self, path, creator=None, pickle=False, backup=NoBackup):
        """Encapsulates pickleable python objects to allow python objects to be used
        as output/input of jobs. Use the set and get method to interact with it.


        :param str path: Name of file where the output is stored
        :param Job|None creator: Job that creates this object
        :param pickle: Object should be pickled or stored as string
        :param backup: Returned if variable get is call but job is not finished
        """

        Path.__init__(self, path, creator)
        self.pickle = pickle
        self.backup = backup

    def is_set(self):
        return os.path.isfile(self.get_path())

    @check_is_worker
    @finished_results_cache.caching(
        get_key=lambda self: ("value", self.rel_path()),
        cache_if=lambda res, self: self.available()
        and (os.path.getsize(self.get_path()) < gs.CACHE_FINISHED_RESULTS_MAX_SIZE),
    )
    def get(self):
        if not self.is_set():
            if self.backup != NoBackup:
                return self.backup
            elif gs.RAISE_VARIABLE_NOT_SET_EXCEPTION:
                raise VariableNotSet(
                    "Variable is not set (%s) and RAISE_VARIABLE_NOT_SET_EXCEPTION == True" % self.get_path()
                )
            else:
                return "<UNFINISHED VARIABLE: %s>" % self.get_path()

        if self.pickle:
            with gzip.open(self.get_path(), "rb") as f:
                v = pickle.load(f)
        else:
            with open(self.get_path(), "rt", encoding="utf-8") as f:
                # using eval since literal_eval can not parse 'nan' or 'inf'
                v = eval(f.read(), {"nan": float("nan"), "inf": float("inf")})
        return v

    def set(self, value):
        if self.pickle:
            with gzip.open(self.get_path(), "wb") as f:
                pickle.dump(value, f)
        else:
            with open(self.get_path(), "wt", encoding="utf-8") as f:
                f.write("%s\n" % repr(value))

    # Filesystem functions
    def __fs_directory__(self):
        """Returns all items that should be listed by virtual filesystem"""
        yield "value"
        if self.creator is not None:
            yield "creator"
            yield "_" + self.creator._sis_id().replace(os.path.sep, "_")

    def __fs_get__(self, step):
        if "value".startswith(step):
            return None, self.get()
        elif (
            self.creator
            and "creator".startswith(step)
            or ("_" + self.creator._sis_id().replace(os.path.sep, "_")).startswith(step)
        ):
            return None, self.creator
        else:
            raise KeyError(step)

    def __str__(self):
        return str(self.get())

    def __repr__(self):
        if gs.LEGACY_VARIABLE_CONVERSION:
            return repr(self.get())
        else:
            value = ""
            if self.is_set():
                value = " %s" % self.get()
            return "<Variable %s%s>" % (self.rel_path(), value)
