# Author: Jan-Thorsten Peter <peter@cs.rwth-aachen.de>

import os
import logging
import gzip
import pickle

import sisyphus.tools as tools
import sisyphus.global_settings as gs


class Path:
    """
    Object do hold the connecting path to files:

    that are exchanged between jobs
    each path can have a creator or a direct pass to the target and many users.
    """

    _sis_path = True
    path_type = 'Path'
    cacheing_enabled = False

    # Update RelPath in toolkit if position of hash_overwrite is changed
    def __init__(self, path, creator=None, cached=False, hash_overwrite=None, tags=None,
                 available=None):
        """
        :param str path: Path to file, if creator is given relative to it's output directory
        :param Job|None creator: Job that creates output file
        :param bool cached: use file caching, via gs.file_caching, e.g. using cache manager
        :param str|None hash_overwrite:
        :param set|None tags:
        :param function|None available: Overwrite function which tests if path is available.
                                        Gets path as input and must be pickleable
        """

        self.creator = creator
        self.users = set()

        assert not isinstance(creator, str)
        self.path = path
        self.cached = cached
        self.hash_overwrite = hash_overwrite
        self._tags = tags

        self._available = available

    def keep_value(self, value):
        if self.creator:
            self.creator.keep_value(value)
        else:
            logging.warning(
                'Try to set keep value for input path: %s' %
                str(self))

    @property
    def tags(self):
        if self._tags is None:
            if self.creator is None:
                return set()
            elif isinstance(self.creator, str):
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
        assert hasattr(self, 'users'), "May happens during unpickling, change to add user if needed"
        self.users.add(user)

    def _sis_hash(self):
        assert not isinstance(self.creator, str)
        if self.hash_overwrite is None:
            creator = self.creator
            path = self.path
        else:
            overwrite = self.hash_overwrite
            assert_msg = "sis_hash for path must be str or tuple of length 2"
            if isinstance(overwrite, tuple):
                assert len(overwrite) == 2, assert_msg
                creator, path = overwrite
            else:
                assert isinstance(overwrite, str), assert_msg
                creator = None
                path = overwrite
        if hasattr(creator, '_sis_id'):
            creator = os.path.join(self.creator._sis_id(), gs.JOB_OUTPUT)
        return b'(Path, ' + tools.sis_hash_helper((creator, path)) + b')'

    def available(self, debug_info=None):
        """  Returns True if the computations creating the path are completed
        :return:
        """

        # Use custom set function, check hasattr for backwards compatibility
        if hasattr(self, '_available') and self._available:
            return self._available(self)

        path = self.get_path()
        if self.creator is None or isinstance(self.creator, str):
            return os.path.isfile(path) or os.path.isdir(path)
        else:
            job_path_available = self.creator.path_available(self)
            if self.creator._sis_finished() and not job_path_available:
                if debug_info:
                    logging.warning('Job marked as finished but requested output is not available: %s %s'
                                    % (self, debug_info))
                else:
                    logging.warning('Job marked as finished but requested output is not available: %s' % self)
            return job_path_available

    # TODO Move this to toolkit cleanup together with job method
    def get_needed_jobs(self, visited):
        """ Return all jobs leading to this path """
        assert(not isinstance(self.creator, str)), "This should only occur during running of worker"
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
            # creator path is in work dir
            if isinstance(self.creator, str):
                creator_path = os.path.join(self.creator, gs.JOB_OUTPUT)
            else:
                creator_path = self.creator._sis_path(gs.JOB_OUTPUT)
            return os.path.join(creator_path, self.path)

    def get_path(self):
        """
        :return: a string with the absolute path to this file
        :rtype: str
        """
        path = self.rel_path()
        if os.path.isabs(path):
            return path
        else:
            return os.path.join(gs.BASE_DIR, path)

    def __str__(self):
        if Path.cacheing_enabled and self.cached:
            return gs.file_caching(self.get_path())
        else:
            return self.get_path()

    def __repr__(self):
        return repr(str(self))

    def __lt__(self, other):
        """
        Define smaller then other by first comparing the creator sis id, next the path

        :param other:
        :return:
        """
        if not isinstance(other, Path):
            assert False, "Cannot compare path to none path"

        def creator_to_str(c):
            if isinstance(c, str):
                return c
            elif hasattr(c, '_sis_id'):
                return c._sis_id()
            elif c is None:
                return str(c)
            else:
                assert False, "User of path is not a job"

        return ("%s %s" % (creator_to_str(self.creator), self.path) <
                "%s %s" % (creator_to_str(other.creator), other.path))

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
        return hash((self.__dict__.get('creator'),
                     self.__dict__.get('path')))

    def __deepcopy(self):
        """ A Path should always be a singleton for one path => a deep copy is a reference to itself"""
        return self

    def __getstate__(self):
        """  Skips exporting users
        :return:
        """
        d = self.__dict__.copy()
        del d['users']
        return d

    def __setstate__(self, state):
        assert 'users' not in state
        self.__dict__.update(state)
        if not hasattr(self, 'users'):
            self.users = set()

    # Filesystem functions
    def __fs_directory__(self):
        """ Returns all items that should be listed by virtual filesystem
        :param job:
        :return:
        """
        yield 'file'
        yield 'f'
        if self.creator is not None:
            yield 'creator'
            yield 'c'
            yield '_' + self.creator._sis_id().replace(os.path.sep, '_')
        yield 'users'
        yield 'u'

    def __fs_get__(self, step):
        if 'file'.startswith(step):
            return 'symlink', self.get_path()
        elif self.creator and \
                ('creator'.startswith(step) or ('_'+self.creator._sis_id().replace(os.path.sep, '_')).startswith(step)):
            return None, self.creator
        elif 'users'.startswith(step):
            return None, self.users
        else:
            raise KeyError(step)

    def replace(self, other):
        """ Replace this path by other path:

        Args:
        other: This path will be replaced by other path
        """
        other.users.update(self.users)
        self.__dict__ = other.__dict__

    def size(self):
        """ Return file size if file exists, else return None """
        assert self.available(), "Path not ready: %s" % str(self.get_path())
        return os.path.getsize(self.get_path())

    def estimate_text_size(self):
        """ Returns estimated size of a text file
        file is not zipped => return original size
        file is zipped => multiply size by 3.5
        """
        if self.is_zipped():
            return int(self.size() * 3.5)
        else:
            return self.size()

    def lines(self):
        """ Returns the number of lines in file """

        if self.is_zipped():
            f = gzip.open(str(self))
        else:
            f = open(str(self))
        i = 0
        for line in f:
            i += 1
        return i

    def is_zipped(self):
        """ Returns if file is zipped:

        Returns:
        None if file doesn't exists
        true if file is zipped
        false otherwise"""

        if not self.available():
            return None

        filename = self.get_path()
        # test file header, this value will be returned
        with open(filename, 'br') as test_file:
            file_zipped = (test_file.read(2) == b'\x1f\x8b')
        # check name, just as sanity check
        name_zipped = filename.endswith('.gz')

        if file_zipped and not name_zipped:
            logging.warning(
                'File is zippped, but does not end with gz: %s',
                filename)
        if not file_zipped and name_zipped:
            logging.warning(
                'File is not zippped, but ends with gz: %s',
                filename)

        return file_zipped


class Variable(Path):
    path_type = 'Variable'

    def __init__(self, path, creator=None, pickle=False, backup=None):
        """ Encapsulates pickleable python objects to allow python objects to be used
        as output/input of jobs. Use the set and get method to interact with it.


        :param str path: Name of file where the output is stored
        :param Job|None creator: Job that creates this object
        :param pickle: Object should be pickled or stored as string
        :param backup: Returned if variable get is call but job is not finished
        """

        Path.__init__(self, path, creator)
        self.pickle = pickle
        self.cache_set = False
        self.cache = None
        self.backup = backup

    def get(self):
        if hasattr(self, 'cache_set') and self.cache_set:
            return self.cache
        if not os.path.isfile(self.get_path()):
            if self.backup is None:
                return "<UNFINISHED VARIABLE: %s>" % self.get_path()
            else:
                return self.backup
        if self.pickle:
            with gzip.open(self.get_path(), 'rb') as f:
                v = pickle.load(f)
        else:
            with open(self.get_path(), 'rt', encoding='utf-8') as f:
                # using eval since literal_eval can not parse 'nan' or 'inf'
                v = eval(f.read(), {'nan': float('nan'), 'inf': float('inf')})
        self.cache_set = True
        self.cache = v
        return v

    def set(self, value):
        if self.pickle:
            with gzip.open(self.get_path(), 'wb') as f:
                pickle.dump(value, f)
        else:
            with open(self.get_path(), 'wt', encoding='utf-8') as f:
                f.write('%s\n' % repr(value))

    # Filesystem functions
    def __fs_directory__(self):
        """ Returns all items that should be listed by virtual filesystem
        """
        yield 'value'
        if self.creator is not None:
            yield 'creator'
            yield '_' + self.creator._sis_id().replace(os.path.sep, '_')

    def __fs_get__(self, step):
        if 'value'.startswith(step):
            return None, self.get()
        elif self.creator and 'creator'.startswith(step) or\
                ('_' + self.creator._sis_id().replace(os.path.sep, '_')).startswith(step):
            return None, self.creator
        else:
            raise KeyError(step)

    def __str__(self):
        return str(self.get())

    def __repr__(self):
        return str(self.get())
