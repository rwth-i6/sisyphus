#!/usr/bin/env python3
# encoding: utf-8

"""
Module to contain all job related code

"""

from __future__ import annotations
import copy
import gzip
import inspect
import logging
import multiprocessing
import os
import pickle
import shutil
import subprocess
import sys
import time
import traceback
from typing import Optional, Any, List, Iterator, Type, TypeVar, Set, Dict

from sisyphus import block, tools
from sisyphus.task import Task
from sisyphus.job_path import AbstractPath, Path, Variable
from sisyphus.tools import finished_results_cache

# Definition of constants
import sisyphus.global_settings as gs

__author__ = "Jan-Thorsten Peter, Eugen Beck"
__email__ = "peter@cs.rwth-aachen.de"

sis_global_lock = multiprocessing.Lock()
SET_DEFAULT_WARNING_COUNT = 0  # Used to avoid spam in log


def get_args(f, args, kwargs):
    """Returns function arguments"""
    self = None
    parsed_args = inspect.getcallargs(f, self, *args, **kwargs)
    self = inspect.getfullargspec(f).args[0]
    if self != "self":
        logging.warning("Deleted obj attribute not named self attribute name %s object" % self)
    del parsed_args[self]
    return parsed_args


# cache to hold all jobs that where created so far to ensure to only create them once
created_jobs = {}


@finished_results_cache.caching(get_key=lambda path: ("job", path))
def job_finished(path):
    """Return True if given job is finished according to files in directory
    :param path: path to directory
    :return:
    """
    return os.path.isfile(os.path.join(path, gs.JOB_FINISHED_MARKER)) or os.path.isfile(
        os.path.join(path, gs.JOB_FINISHED_ARCHIVE)
    )


T = TypeVar("T", bound="Job")


class JobSingleton(type):
    """Meta class to ensure that every Job with the same hash value is
    only created once"""

    def __call__(cls: Type[T], *args, **kwargs) -> T:
        """Implemented to ensure that each job is created only once"""
        try:
            if "sis_tags" in kwargs:
                tags = kwargs["sis_tags"]
                if tags is not None:
                    for tag in tags:
                        for char in tag:
                            assert char in "-.0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz"
                del kwargs["sis_tags"]
            else:
                tags = None
            parsed_args = get_args(cls.__init__, args, kwargs)
        except TypeError:
            logging.error(
                "Wrong input arguments or missing __init__ function?\n"
                "Class: %s\nArguments: %s %s" % (str(cls), str(args), str(kwargs))
            )
            raise

        # create key
        sis_hash = cls._sis_hash_static(parsed_args)
        module_name = cls.__module__
        recipe_prefix = gs.RECIPE_PREFIX + "."
        if module_name.startswith(recipe_prefix):
            sis_name = module_name[len(recipe_prefix) :]
        else:
            sis_name = module_name
        sis_name = os.path.join(sis_name.replace(".", os.path.sep), cls.__name__)
        sis_id = "%s.%s" % (sis_name, sis_hash)

        # Update tags
        if tags is None:
            tags = set()
            for p in tools.extract_paths(parsed_args):
                tags.update(p.tags)

        # check cache
        if sis_id in created_jobs:
            job = created_jobs[sis_id]
        else:
            # create new object
            job = super(Job, cls).__new__(cls)
            assert isinstance(job, Job)
            job._sis_tags = tags

            # store _sis_id
            job._sis_id_cache = sis_id

            job._sis_init(*copy.deepcopy((args, kwargs, parsed_args)))
            created_jobs[sis_id] = job

        # Add block
        # skip in worker mode
        if block.active_blocks:
            for b in block.active_blocks:
                b.add_job(job)
                job._sis_add_block(b)

        # Update alias prefixes
        job._sis_alias_prefixes.add(gs.ALIAS_AND_OUTPUT_SUBDIR)

        # add stacktrace information.
        assert (
            isinstance(gs.JOB_ADD_STACKTRACE_WITH_DEPTH, int) or gs.JOB_ADD_STACKTRACE_WITH_DEPTH == float("inf")
        ) and gs.JOB_ADD_STACKTRACE_WITH_DEPTH >= 0
        if gs.JOB_ADD_STACKTRACE_WITH_DEPTH > 0:
            stacktrace = traceback.extract_stack(
                limit=gs.JOB_ADD_STACKTRACE_WITH_DEPTH if isinstance(gs.JOB_ADD_STACKTRACE_WITH_DEPTH, int) else None
            )
            job._sis_stacktrace.append(stacktrace)

        return job

    def state_init(cls, state):
        # restores saved state
        obj = cls.__new__(cls)
        for k, v in state.items():
            setattr(obj, k, v)
        return obj


class Job(metaclass=JobSingleton):
    """
    Object to hold the job descriptions.

    You derive your own job classes from this base class.

    All the arguments of ``__init__`` will be taken into account for the hash.
    In your derived class, you need to overwrite the ``tasks`` method.
    """

    __sis_version__ = None

    # This dict can be used to extend existing jobs with new parameters without changing the hash.
    #
    # If the new parameter is called 'foo' and old behavior would be reached by setting it to 'bar'
    # __sis_hash_exclude__ should be {'foo': 'bar'}.
    __sis_hash_exclude__ = {}
    # This list can be used to replace hash values e.g. if it is set to [('key_name', 'foo', 'bar')]
    # the parameter key_name='foo' will be changed to return the same hash as key_name='bar'
    __sis_hash_overwrite__ = []

    _lock_storage = []
    _lock_index = -1

    @classmethod
    def get_lock(cls):
        Job._lock_index = (Job._lock_index + 1) % gs.JOB_MAX_NUMBER_OF_LOCKS
        if len(Job._lock_storage) < gs.JOB_MAX_NUMBER_OF_LOCKS:
            Job._lock_storage.append(multiprocessing.Lock())
        return Job._lock_storage[Job._lock_index]

    def __new__(cls: Type[T], *args, **kwargs) -> T:
        # Make sure unpickled jobs stay singletons
        assert len(args) == 1 and len(kwargs) == 0 and isinstance(args[0], str)
        sis_cache_key = args[0]
        if sis_cache_key in created_jobs:
            logging.debug("Loaded %s sis_id from created job cache" % sis_cache_key)
            job = created_jobs[sis_cache_key]
        else:
            # create new object
            logging.debug("Could not find %s sis_id in created job cache" % sis_cache_key)
            job = super(Job, cls).__new__(cls)
            created_jobs[sis_cache_key] = job
            # other initialization here is not need, pickle will call __setstate__
        return job

    # Init
    def _sis_init(self, args, kwargs, parsed_args):
        for key, arg in parsed_args.items():
            if isinstance(arg, Job):
                logging.warning(
                    'The Job %s was used as argument "%s" in "%s", this might result in undesired behavior'
                    % (str(arg)[3:], key, self.__class__)
                )

        self._sis_aliases: Optional[Set[str]] = None
        self._sis_alias_prefixes = set()
        self._sis_vis_name = None
        self._sis_output_dirs = set()
        self._sis_outputs = {}
        self._sis_keep_value = None
        self._sis_hold_job = False
        self._sis_worker_wrapper = gs.worker_wrapper

        self._sis_blocks = set()
        self._sis_kwargs = parsed_args
        self._sis_task_rqmt_overwrite = {}

        self._sis_job_lock = Job.get_lock()
        self._sis_is_finished = False
        self._sis_finished_recent_check_time = float("-inf")
        self._sis_setup_since_restart = False

        self._sis_environment = tools.EnvironmentModifier(cleanup_env=gs.CLEANUP_ENVIRONMENT)
        if gs.CLEANUP_ENVIRONMENT:  # for compat, only set those below if CLEANUP_ENVIRONMENT is enabled
            self._sis_environment.keep(gs.DEFAULT_ENVIRONMENT_KEEP)
            self._sis_environment.set(gs.DEFAULT_ENVIRONMENT_SET)

        if gs.AUTO_SET_JOB_INIT_ATTRIBUTES:
            self.set_attrs(parsed_args)
        self._sis_inputs = set()
        self.__init__(*args, **kwargs)

        self._sis_inputs.update({p for p in tools.extract_paths([self.__dict__, args, kwargs]) if p.creator != self})

        for i in self._sis_inputs:
            i.add_user(self)

        self._sis_quiet = False
        self._sis_cleanable_cache = False
        self._sis_cleaned_or_not_cleanable = False
        self._sis_needed_for_which_targets = set()

        self._sis_stacktrace = []

    # Functions directly used to run the job
    def _sis_setup_directory(self, force=False):
        """Setup the working directory"""

        if self._sis_setup_since_restart and self._sis_setup() and not force:
            return

        basepath = self._sis_path()
        if os.path.islink(basepath) and not os.path.exists(basepath):
            # is a broken symlink
            logging.warning("Found broken link %s to %s while setting up %s" % (basepath, os.readlink(basepath), self))
            os.unlink(basepath)
            logging.warning("Removed broken link %s" % basepath)

        for dirname in [gs.JOB_WORK_DIR, gs.JOB_OUTPUT, gs.JOB_INPUT, gs.JOB_LOG_ENGINE]:
            path = self._sis_path(dirname)
            try:
                os.makedirs(path)
            except FileExistsError:
                assert os.path.isdir(path)

        for dirname in self._sis_output_dirs:
            path = str(dirname)
            if not os.path.isdir(path):
                os.makedirs(path)

        # link input jobs
        for input_path in self._sis_inputs:
            creator = input_path.creator
            if creator:
                job_id = creator._sis_id()
                if len(job_id) > 255:
                    # Many filesystems have a restriction of 255 bytes per filename (basename).
                    # We would hit that limit by the flattening below...
                    # To avoid that (but also risking further collisions...):
                    job_id = job_id.split("/")[-1]
                # replace / with _ to make the directory structure flat
                # I it would be possible to hit some cases where this could
                # cause a collision sorry if you are really that unlucky...
                link_name = os.path.join(self._sis_path(gs.JOB_INPUT), str(job_id).replace("/", "_"))
                if not os.path.exists(link_name) and not os.path.islink(link_name):
                    os.symlink(src=os.path.abspath(str(creator._sis_path())), dst=link_name, target_is_directory=True)

        # export the actual job
        with gzip.open(self._sis_path(gs.JOB_SAVE), "w") as f:
            pickle.dump(self, f)

        with open(self._sis_path(gs.JOB_INFO), "w", encoding="utf-8") as f:
            for tag in sorted(self.tags):
                f.write("TAG: %s\n" % tag)
            for i in sorted(p.get_path() for p in self._sis_inputs):
                f.write("INPUT: %s\n" % i)
            for key, value in sorted(self._sis_kwargs.items()):
                try:
                    f.write("PARAMETER: %s: %s\n" % (key, value))
                except UnicodeEncodeError as e:
                    f.write("PARAMETER: %s: <UnicodeEncodeError: %s>\n" % (key, e))
            if self._sis_aliases:
                for alias in sorted(self._sis_aliases):
                    f.write("ALIAS: %s\n" % alias)
            for stacktrace in self._sis_stacktrace:
                f.write("STACKTRACE:\n")
                f.writelines(traceback.format_list(stacktrace))
        self._sis_setup_since_restart = True

    def __getstate__(self):
        d = self.__dict__.copy()
        for key in [
            "_sis_job_lock",
            "_sis_blocks",
            "current_block",
            "_sis_cleanable_cache",
            "_sis_cleaned_or_not_cleanable",
            "_sis_worker_wrapper",
        ]:
            if key in d:
                del d[key]
        return d

    def __deepcopy__(self, memo):
        """A Job should always be a singleton for the same initialization => a deep copy is a reference to itself
        :param memo:
        :return:
        """
        return self

    def __setstate__(self, state):
        self.__dict__.update(state)
        if "_sis_alias" in state:
            self._sis_aliases = {state["_sis_alias"]}
        if not hasattr(self, "_sis_job_lock"):
            self._sis_job_lock = self.get_lock()
        self._sis_blocks = set()
        self._sis_cleanable_cache = False
        self._sis_cleaned_or_not_cleanable = False
        for i in self._sis_inputs:
            i.add_user(self)

        if block.active_blocks:
            for b in block.active_blocks:
                b.add_job(self)
                self._sis_add_block(b)

        logging.debug("Set state %s" % state["_sis_id_cache"])

    def __getnewargs__(self):
        logging.debug("Pickle: %s" % self._sis_id())
        return (self._sis_id(),)

    def _sis_update_possible(self):
        """
        True if it's possible that the job requirements change which is True if the update method is overwritten
        :return:
        """
        return self.update.__code__ is not Job.update.__code__

    def _sis_update_inputs(self):
        """Checks for new inputs
        returns True if inputs changed
        """
        with self._sis_job_lock:
            # TODO active blocks is currently not thread save, but it's not critical since it doesn't effect the graph
            previous_blocks = block.active_blocks
            block.active_blocks = self._sis_blocks
            previous = self._sis_inputs.copy()
            self.update()
            block.active_blocks = previous_blocks
            return previous != self._sis_inputs

    # Helper functions
    def _sis_path(self, path_type=None, task_id=None, abspath=False):
        """
        Adjust path according to the job path.

        :param str|None path_type:
        :param int|None task_id:
        :param bool abspath:
        :rtype: str
        """

        if gs.JOB_USE_TAGS_IN_PATH and self._sis_tags:
            tags = "." + ".".join(sorted(list(self._sis_tags)))
            tags = tags[:80]
        else:
            tags = ""

        # no path type given, return base path
        if path_type is None:
            path = os.path.join(gs.WORK_DIR, self._sis_id() + tags)
        else:
            path = path_type
            # Absolute path needs no adjustment
            if not os.path.isabs(path):
                path = os.path.join(gs.WORK_DIR, self._sis_id() + tags, path)

        # Add task id as suffix
        if task_id is not None:
            path += f".{task_id}"

        if abspath and not os.path.isabs(path):
            path = os.path.join(gs.BASE_DIR, path)
        return path

    # State logging
    def _sis_file_logging(self, log_name, task_id=None, update=None, combine=all, minimal_file_age=0):
        """
        :param str log_name:
        :param int|list[int]|None task_id: task_id for array job, if None assume whole job
        :param bool update: new value
        :param combine: function to combine all array jobs to one bool, e.g. all/any
        :param int|float minimal_file_age: in seconds
        :rtype: bool
        :return: logging via file system, True if file exist false if not
        """

        # Check single instances of job
        if isinstance(task_id, list):
            return combine([self._sis_file_logging(log_name, t_id, update=update) for t_id in task_id])

        # find logfile
        logfile = self._sis_path(log_name, task_id, abspath=True)
        current_state = os.path.isfile(logfile)

        if update is None:
            if current_state:
                return minimal_file_age <= 0 or minimal_file_age < time.time() - os.path.getmtime(logfile)
            else:
                return False
        else:
            # create file
            if update and not current_state:
                with open(logfile, "w"):
                    pass
            # remove file
            elif not update and current_state:
                os.remove(logfile)
            else:
                # no updated needed
                pass
            return update

    def _sis_link_to_team_share_dir(self):
        """Link local job to team directory"""
        assert self._sis_is_finished

        if gs.TEAM_SHARE_DIR:
            local_path = self._sis_path()
            team_path = os.path.join(gs.TEAM_SHARE_DIR, self._sis_id())
            if not os.path.islink(local_path) and not os.path.isdir(team_path):
                with self._sis_job_lock:
                    team_path_dir = os.path.dirname(team_path)
                    umask = os.umask(2)
                    if not os.path.isdir(team_path_dir):
                        os.makedirs(team_path_dir)
                    if not os.path.isdir(team_path) and os.path.islink(team_path):
                        # is link but not dir => broken link lets remove it
                        os.unlink(team_path)
                    if not os.path.islink(team_path):
                        os.symlink(src=os.path.abspath(local_path), dst=team_path, target_is_directory=True)
                    os.umask(umask)

    def _sis_finished(self):
        """Return True if job or task is finished"""
        if self._sis_is_finished:
            return True

        # This might run again and again recursively:
        # _sis_finished -> _sis_runnable -> _sis_all_path_available -> path_available -> _sis_finished
        # https://github.com/rwth-i6/sisyphus/issues/249
        # Don't check too often.
        if (time.monotonic() - self._sis_finished_recent_check_time) < gs.WAIT_PERIOD_BETWEEN_CHECKS:
            return False
        self._sis_finished_recent_check_time = time.monotonic()

        if job_finished(self._sis_path()):
            # Job is already marked as finished, skip check next time
            self._sis_is_finished = True
            return True
        else:
            if self._sis_setup() and self._sis_runnable():
                # check all task if they are finished
                for task in self._sis_tasks():
                    # job is only finished if all sub tasks are finished
                    if not task.finished():
                        return False
                # Mark job as finished
                self._sis_is_finished = True
                try:
                    with self._sis_job_lock, open(self._sis_path(gs.JOB_FINISHED_MARKER), "w"):
                        pass
                except PermissionError:
                    # It's probably not your directory, skip setting the
                    # finished marker
                    pass
                self._sis_link_to_team_share_dir()
                gs.on_job_finished(self)
                return True
            else:
                # Job is not even setup => can not be finished yet
                return False

    def _sis_cleanable(self):
        if self._sis_cleaned_or_not_cleanable:
            return False
        elif self._sis_cleanable_cache:
            return True
        else:
            if os.path.islink(self._sis_path()):
                self._sis_cleaned_or_not_cleanable = True
                return False
            cleanable = not os.path.isfile(self._sis_path(gs.JOB_FINISHED_ARCHIVE)) and self._sis_finished()
            if cleanable:
                self._sis_cleanable_cache = True
            return cleanable

    def _sis_cleanup(self):
        """Clean up job directory"""
        assert self._sis_finished()
        if not os.path.isfile(self._sis_path(gs.JOB_FINISHED_ARCHIVE)):
            logging.info("clean up: %s" % self._sis_path())
            with self._sis_job_lock:
                try:
                    if not gs.JOB_CLEANUP_KEEP_WORK:
                        shutil.rmtree(os.path.abspath(self._sis_path(gs.JOB_WORK_DIR)))
                    files_keep = [gs.JOB_OUTPUT, gs.JOB_INFO, gs.JOB_WORK_DIR]
                    if gs.JOB_CLEANUP_KEEP_INPUT:
                        files_keep.append(gs.JOB_INPUT)
                    files = [i for i in os.listdir(self._sis_path()) if i not in files_keep]
                    subprocess.check_call(
                        ["tar", "-czf", gs.JOB_FINISHED_ARCHIVE] + files, cwd=os.path.abspath(self._sis_path())
                    )
                    for i in range(4):  # try three times to clean directory
                        try:
                            if files:
                                subprocess.check_call(["rm", "-r"] + files, cwd=os.path.abspath(self._sis_path()))
                        except subprocess.CalledProcessError as e:
                            # Remove files from list that are already deleted
                            time.sleep(5)
                            files = set(files) & set([i for i in os.listdir(self._sis_path()) if i != gs.JOB_OUTPUT])
                            files = sorted(list(files))
                            if i == 3:
                                raise e
                        else:
                            break
                    self._sis_cleanable_cache = False
                    self._sis_cleaned_or_not_cleanable = True
                except (OSError, subprocess.CalledProcessError) as e:
                    # probably not our directory, just pass
                    logging.warning("Could not clean up %s: %s" % (self._sis_path(), str(e)))
                    self._sis_cleaned_or_not_cleanable = True

    def _sis_id(self):
        """
        :return: unique job identifier, "<sis_name>.<sis_hash>"
        :rtype: str
        """
        return self._sis_id_cache

    def _sis_hash(self):
        return self._sis_id_cache.encode()

    @classmethod
    def _sis_hash_static(cls, parsed_args: Dict[str, Any]) -> str:
        """
        :param parsed_args:
        :return: hash
        """
        h = cls.hash(parsed_args)
        assert isinstance(h, str), "hash return value must be str"
        allowed_characters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz.-_/"
        assert all(i in allowed_characters for i in h), (
            "hash should a only contain these characters: %s" % allowed_characters
        )
        return h

    def _sis_import_from_dirs(self, import_dirs, mode, use_alias=False):
        """
        If a finished version of this job is found in the given directories
        it's linked into the local work directory.

        TODO: a lot duplicated code with _sis_migrate_directory

        :param import_dirs:
        :return:
        """
        assert mode in ("copy", "symlink", "hardlink", "dryrun"), "Unsupported mode given: %s" % mode
        local_path = self._sis_path()

        if use_alias and not self._sis_aliases:
            return False

        with self._sis_job_lock:
            if not os.path.isdir(local_path) and os.path.islink(local_path):
                # we got a broken local link, delete it
                os.unlink(local_path)

            for import_dir in import_dirs:
                # Check if the job is already completed in the team directory
                if use_alias:
                    found_aliases = []
                    found_targets = set()
                    for alias in self._sis_aliases:
                        import_path = os.path.join(import_dir, alias)
                        if os.path.islink(import_path):
                            target = os.readlink(import_path)
                            found_aliases.append((import_path, target))
                            found_targets.add(target)

                    if not found_targets:
                        continue
                    elif len(found_targets) > 1:
                        logging.warning(
                            "Skip import: Multiple aliases with different targets "
                            "found for %s: %s" % (local_path, found_aliases)
                        )
                        continue
                    else:
                        import_path = found_targets.pop()
                else:
                    import_path = os.path.join(import_dir, self._sis_id())

                if os.path.isdir(import_path):
                    # Job found, check if job is finished
                    if job_finished(os.path.join(import_path)):
                        # ensure base directory exists
                        with sis_global_lock:
                            base_dir = os.path.dirname(local_path)
                            if not os.path.isdir(base_dir):
                                os.makedirs(base_dir)
                        # resolve all intermediate links
                        import_path = os.path.realpath(import_path)
                        # link job directory to local work directory
                        if not os.path.islink(local_path):
                            if mode == "copy":
                                logging.info("Copy import %s from %s" % (self._sis_id(), import_path))
                                shutil.copytree(src=os.path.abspath(import_path), dst=local_path, symlinks=True)
                            elif mode == "symlink":
                                logging.info("Symlink import %s from %s" % (self._sis_id(), import_path))
                                os.symlink(src=os.path.abspath(import_path), dst=local_path, target_is_directory=True)
                            elif mode == "hardlink":
                                logging.info("Hardlink import %s from %s" % (self._sis_id(), import_path))
                                shutil.copytree(
                                    src=os.path.abspath(import_path),
                                    dst=local_path,
                                    symlinks=True,
                                    copy_function=os.link,
                                )
                            else:
                                logging.info("Possible import %s from %s" % (self._sis_id(), import_path))
                        return True
        return False

    def _sis_setup(self):
        """True if job directory exists"""
        return os.path.isdir(self._sis_path())

    def _sis_state(self, engine):
        """Return the state of this job"""
        if self._sis_setup():
            # job is setup, check progress
            if self._sis_is_set_to_hold():
                return gs.STATE_HOLD
            if self._sis_finished():
                return gs.STATE_FINISHED

            # Check if required inputs were removed after setup
            if not self._sis_runnable():
                return gs.STATE_WAITING

            # check single tasks
            for task in self._sis_tasks():
                state = task.state(engine)
                if not state == gs.STATE_FINISHED:
                    return state
            # All states are finished:
            return gs.STATE_FINISHED
        elif self._sis_runnable():
            if self._sis_is_set_to_hold():
                return gs.STATE_HOLD
            else:
                return gs.STATE_RUNNABLE
        else:
            return gs.STATE_WAITING

    def _sis_all_path_available(self):
        """True if all current inputs are available no update of the inputs is done"""
        for path in list(self._sis_inputs):
            if not path.available(debug_info=self):
                return False
        return True

    def _sis_runnable(self):
        """True if all inputs are available, also checks if new inputs are requested"""

        if not self._sis_update_possible():
            # Short cut used for most jobs
            return self._sis_all_path_available()

        # Recursively check for new inputs
        while self._sis_all_path_available() and self._sis_update_inputs():
            pass

        # One last check in case sis_update_inputs was run in a parallel thread
        return self._sis_all_path_available()

    def _sis_migrate_directory(self, src, mode="link"):
        """Migrate from previously finished directory

        TODO: a lot duplicated code with _sis_import_from_dirs
        """

        dst = self._sis_path()
        if dst.startswith("./"):
            dst = dst[2:]

        # don't do anything if it's the same directory
        if src == dst:
            return False

        assert not os.path.isfile(dst), "Target directory is a file, remove it: %s" % dst
        # skip if current directory already exists
        if os.path.isdir(dst):
            logging.warning("Don't migrate since directory already exists: %s to %s" % (src, dst))
            return False
        if os.path.islink(dst):
            if os.path.isdir(dst):
                logging.warning("Don't migrate since directory already linked: %s to %s" % (src, dst))
                return False
            else:
                logging.warning("Remove broken link: %s" % dst)
                os.unlink(dst)

        # only migrate finished jobs
        if not job_finished(src):
            logging.warning("Don't migrate since src directory is not finished: %s to %s" % (src, dst))
            return False

        logging.info("%s: %s to %s" % (mode.title().replace("_", " "), src, dst))

        # make sure the main directory exists
        if not os.path.isdir(os.path.dirname(dst)) and mode != "fake":
            os.makedirs(os.path.dirname(dst))

        if mode == "dryrun":
            print("%s -> %s" % (src, dst))
        elif mode == "link":
            os.symlink(os.path.abspath(src), dst)
        elif mode == "copy":
            shutil.copytree(src, dst, symlinks=True)
        elif mode == "move":
            os.rename(src, dst)
        elif mode == "move_and_link":
            os.rename(src, dst)
            os.symlink(os.path.abspath(dst), src)
        elif mode == "hardlink_or_copy":
            tools.hardlink_or_copy(src, dst)
        elif mode == "hardlink_or_link":
            tools.hardlink_or_copy(src, dst, use_symlink_instead_of_copy=True)
        else:
            assert False, "Unknown mode: %s" % mode
        return True

    def __str__(self):
        alias = self.get_one_alias()
        if alias is not None:
            alias_prefix = "" if len(self._sis_alias_prefixes) == 0 else list(self._sis_alias_prefixes)[0]
            path_str = "%s %s" % (os.path.join(gs.ALIAS_DIR, alias_prefix, alias), self._sis_path())
        else:
            path_str = self._sis_path()
        return "Job<%s>" % path_str

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        if type(other) is not type(self):
            return False
        else:
            # TODO Check how uninitialized object should behave here
            if len(self.__dict__) == len(other.__dict__) == 0:
                return True
            return self._sis_id() == other._sis_id()

    def __hash__(self):
        # TODO Check how uninitialized object should behave here
        return hash(self.__dict__.get("_sis_id_cache"))

    def _sis_print_error(self, tasks=1, lines=0):
        """Print the last lines of the first tasks of this
        job that are in an error state"""
        for task in self._sis_tasks():
            if tasks <= 0:
                break
            if task.error():
                tasks -= 1
                task.print_error(lines)

    def _sis_move(self) -> None:
        """Move job directory aside"""
        path = self._sis_path()
        i = 1
        while os.path.isdir("%s.cleared.%04i" % (path, i)):
            i += 1
        trash_path = "%s.cleared.%04i" % (path, i)
        logging.info("Move: %s to %s" % (path, trash_path))
        os.rename(path, trash_path)

    def _sis_delete(self) -> None:
        """Delete job directory"""
        path = self._sis_path()
        logging.info("Delete: %s" % path)
        try:
            if os.path.islink(path):
                os.unlink(path)
            else:
                shutil.rmtree(path)
        except FileNotFoundError:
            logging.warning("File not Found: %s" % path)

    def _sis_tasks(self) -> List[Task]:
        """
        :return: all tasks of this job
        :rtype: list[sisyphus.task.Task]
        """
        if not self._sis_runnable():
            assert False, "Only runnable jobs can list needed tasks"
        if not hasattr(self, "_sis_task_cache"):
            cache = []
            for task in self.tasks():
                task.set_job(self)
                task_name = task._start
                if task_name in self._sis_task_rqmt_overwrite:
                    rqmt, replace = self._sis_task_rqmt_overwrite[task_name]
                    if replace:
                        task._rqmt = rqmt
                    else:
                        task._rqmt.update(rqmt)

                cache.append(task)
            self._sis_task_cache = cache

        assert self._sis_task_cache, "Job has no tasks defined: %s" % str(self)
        return self._sis_task_cache

    def _sis_next_task(self):
        """
        :return: next not finished task
        :rtype: sisyphus.task.Task|None
        """
        for task in self._sis_tasks():
            if not task.finished():
                return task
        return None

    # Functions used to calculate the dependencies

    # TODO Move this to toolkit cleanup together with path method
    def _sis_get_needed_jobs(self, visited):
        """It will return all jobs that are currently still needed to compute currently not finished jobs.
        This is used to decide if a job can be removed while cleaning up or not.
        """
        jobs = set()
        if self._sis_id() not in visited:
            # job not visited in this run, need to calculate dependencies

            if self._sis_finished():
                jobs = {self}
            else:
                # check input paths
                for i in self._sis_inputs:
                    jobs.update(i.get_needed_jobs(visited))
            visited[self._sis_id()] = jobs
        return visited[self._sis_id()]

    @tools.cache_result(clear_cache="clear_cache")
    def _sis_get_all_inputs(self, include_job_path=False):
        """Returns a dictionary with all input paths contributing to this job"""
        # updates all inputs till the furthest point we can get
        self._sis_runnable()
        inputs = {i.get_path(): i for i in self._sis_inputs}
        if include_job_path:
            job_path = os.path.join(gs.BASE_DIR, self._sis_path())
            inputs[job_path] = self
        for i in self._sis_inputs:
            if i.creator:
                inputs.update(i.creator._sis_get_all_inputs(include_job_path))
        return inputs

    def _sis_contains_required_inputs(self, required_inputs, include_job_path=False):
        """Returns True if all required inputs are used or created by this job
        :param required_inputs:
        :param include_job_path:
        :return:
        """
        return all(
            [
                i in self._sis_get_all_inputs(include_job_path=include_job_path) or i in self._sis_outputs
                for i in required_inputs
            ]
        )

    def _sis_print_tree(
        self, visited, info="", current_prefix="", next_prefix="", out=sys.stdout, required_inputs=set()
    ):
        """Print tree of dependencies"""
        # TODO Move this to a external function

        # updates all inputs till the furthest point we can get
        self._sis_runnable()

        # break if not all required inputs are given
        if not self._sis_contains_required_inputs(required_inputs, include_job_path=True):
            return

        if info != "":
            info = "/%s" % info

        if self._sis_setup():
            if self._sis_finished():
                info += " finished"
            else:
                info += " setup"
        elif self._sis_runnable():
            info += " runnable"
        else:
            info += " waiting"

        print_details = False
        try:
            job_nr = visited[self._sis_id()]
        except KeyError:
            job_nr = len(visited)
            print_details = True
            visited[self._sis_id()] = job_nr

        out.write("%s %s%s\n" % (current_prefix, os.path.join(gs.WORK_DIR, self._sis_id(), gs.JOB_OUTPUT), info))
        if print_details:
            visited[self._sis_id()] = len(visited)
            inputs = list(self._sis_inputs)

            inputs.sort(key=lambda x: x.creator._sis_id() if x.creator else " " + x.path)
            for pos, path in enumerate(inputs):
                if pos + 1 == len(inputs):
                    # last job, change prefix
                    next_current_prefix = next_prefix + "'-"
                    next_next_prefix = next_prefix + "  "
                else:
                    next_current_prefix = next_prefix + "|-"
                    next_next_prefix = next_prefix + "| "

                if path.creator is not None:
                    path.creator._sis_print_tree(
                        visited, path.path, next_current_prefix, next_next_prefix, out, required_inputs
                    )
                else:
                    out.write("%s %s path\n" % (next_current_prefix, path.path))
        else:
            out.write("%s'-   ...    \n" % next_prefix)

    def __lt__(self, other):
        if isinstance(other, Job):
            return self._sis_id() < other._sis_id()

    def __gt__(self, other):
        if isinstance(other, Job):
            return self._sis_id() > other._sis_id()

    # Filesystem functions
    def __fs_directory__(self):
        """Returns all items that should be listed by virtual filesystem
        :param job:
        :return:
        """
        for r in [
            "_work",
            "_base",
            "_output",
            # show job name
            "_" + self._sis_id().replace(os.path.sep, "_"),
        ]:
            yield r
        for r in dir(self):
            if not (r.startswith("_") or hasattr(getattr(self, r), "__call__")):
                yield r

    def __fs_get__(self, step):
        if step == "_work":
            return "symlink", os.path.abspath(self.work_path())
        elif step == "_base":
            return "symlink", os.path.abspath(self._sis_path())
        elif step == "_output":
            return "symlink", os.path.abspath(self._sis_path(gs.JOB_OUTPUT))
        elif step == "_" + self._sis_id().replace(os.path.sep, "_"):
            return "symlink", "_base"
        elif hasattr(self, step):
            return None, getattr(self, step)
        else:
            raise KeyError(step)

    def __fs_symlink__(self, mountpoint, full_path, history):
        if not full_path.startswith("/jobs/"):
            return os.path.abspath(os.path.join(mountpoint, "jobs", self._sis_id()))
        elif any(isinstance(job, Job) for job in history):
            return os.path.abspath(os.path.join(mountpoint, "jobs", self._sis_id()))

    # marking functions

    def _sis_add_block(self, block):
        self._sis_blocks.add(block)

    # interface functions

    def job_id(self):
        """Returns a unique string to identify this job"""
        return self._sis_id()

    def get_aliases(self) -> Optional[Set[str]]:
        return self._sis_aliases

    def get_one_alias(self):
        if self._sis_aliases is not None:
            # if multiple aliases exist pick one at random
            try:
                return next(iter(self._sis_aliases))
            except StopIteration:
                return None
        return None

    def add_alias(self, alias: str):
        if self._sis_aliases is None:
            self._sis_aliases = set()
        self._sis_aliases.add(alias)
        return self

    def get_vis_name(self):
        return self._sis_vis_name

    def set_vis_name(self, vis_name):
        self._sis_vis_name = vis_name
        return self

    def work_path(self):
        return self._sis_path(gs.WORK_DIR)

    def add_input(self, path):
        """
        :param AbstractPath path:
        :return: path
        :rtype: AbstractPath
        """
        assert isinstance(path, AbstractPath)
        self._sis_inputs.add(path)
        self._sis_get_all_inputs(clear_cache=True)
        path.add_user(self)
        return path

    @property
    def tags(self):
        return self._sis_tags

    def path_available(self, path):
        """Returns True if given path is available yet

        :param path: path to check
        :return:
        """
        assert isinstance(path, AbstractPath)
        assert path.creator == self
        return self._sis_finished()

    def set_default(self, name, value):
        """Deprecated helper function, will be removed in the future. Don't use it!"""

        global SET_DEFAULT_WARNING_COUNT
        if SET_DEFAULT_WARNING_COUNT < 10:
            logging.warning(
                "set_default is deprecated, please set the variable manually (%s, %s, %s)" % (self, name, value)
            )
            SET_DEFAULT_WARNING_COUNT += 1
        elif SET_DEFAULT_WARNING_COUNT == 10:
            logging.warning("stop reporting set_default warning")
            SET_DEFAULT_WARNING_COUNT += 1
        else:
            pass

        if getattr(self, name) is None:
            setattr(self, name, value)
            if isinstance(value, AbstractPath):
                self.add_input(value)

    def output_path(self, filename, directory=False, cached=False):
        """
        Adds output path, if directory is True a
        directory will will be created automatically.

        :param str filename:
        :param bool directory:
        :param bool cached:
        :rtype: Path
        """
        path = Path(filename, self, cached)
        if path.get_path() in self._sis_outputs:
            logging.warning("Added output %s more than once to %s" % (filename, self))
            return self._sis_outputs[path.get_path()]
        self._sis_outputs[path.get_path()] = path
        if directory:
            self._sis_output_dirs.add(path)
        return path

    def output_var(self, filename, pickle=False, backup=None):
        """Adds output path which contains a python object,
        if directory is True a directory will will be created automatically
        """
        path = Variable(filename, self, pickle=pickle, backup=backup)
        assert path.get_path() not in self._sis_outputs
        self._sis_outputs[path.get_path()] = path
        return path

    def set_rqmt(self, task_name, rqmt):
        """Overwrites the given requirements for this job

        If this function is called more than once, only the effects of the last call are applied.

        :param str task_name: Which task will be affected
        :param rqmt: the new requirements
        :return:
        """
        """ Overwrites the automatic requirements for this job """
        self._sis_task_rqmt_overwrite[task_name] = rqmt.copy(), True
        return self

    def update_rqmt(self, task_name, rqmt):
        """Updates the given requirements for this job, values not set in rqmt will not be affected.

        If this function is called more than once, only the effects of the last call are applied.

        :param str task_name: Which task will be affected
        :param rqmt: the new requirements
        :return:
        """
        self._sis_task_rqmt_overwrite[task_name] = rqmt.copy(), False
        return self

    def set_env(self, key: str, value: str, *, verbatim: bool = True):
        """
        Set environment variable. This environment var will be set at job startup in the worker.

        :param key: variable name
        :param value:
        :param verbatim: True: set it as-is; False: use string.Template(value).substitute(orig_env)
        """
        if verbatim:
            self._sis_environment.set_verbatim(key, value)
        else:
            self._sis_environment.set(key, value)

    def tasks(self) -> Iterator[Task]:
        """
        :return: yields Task's
        :rtype: list[sisyphus.task.Task]
        """
        yield Task("run")

    def keep_value(self, value=None):
        """Return keep_value, if value is given also set keep value"""
        if value is not None:
            assert 0 <= value < 100
            self._sis_keep_value = value
        return self._sis_keep_value

    def set_keep_value(self, value):
        """Set keep value and return self"""
        assert 0 <= value < 100
        self._sis_keep_value = value
        return self

    def sh(self, command, *args, **kwargs):
        """Calls a external shell and
        replaces {args} with job inputs, outputs, args
        and executes the command"""
        return tools.sh(command, *args, sis_quiet=self._sis_quiet, sis_replace=self.__dict__, **kwargs)

    def set_attrs(self, attrs):
        """Adds all attrs to self, used in constructor e.gl:
        self.set_attrs(locals())"""
        for k, v in attrs.items():
            if k == "self" and self == v:
                # Ignore self
                pass
            elif hasattr(self, k):
                logging.warning(
                    "Not automatically over writing setting "
                    "%s, %s. %s is already defined for Object %s" % (k, str(v), k, self)
                )
            else:
                setattr(self, k, v)

    # Overwritable default functions
    def update(self):
        """Run after all inputs are computed,
        allowing the job to analyse the given input
        and ask for additional inputs before running.
        """
        pass

    def info(self):
        """Returns information about the currently running job
        to be displayed on the web interface and the manager view
        :return: string to be displayed or None if not available
        :rtype: str
        """
        pass

    @classmethod
    def hash(cls, parsed_args: Dict[str, Any]) -> str:
        """
        :param parsed_args:
        :return: hash for job given the arguments
        """
        d = {}
        for k, v in parsed_args.items():
            if k not in cls.__sis_hash_exclude__ or cls.__sis_hash_exclude__[k] != v:
                d[k] = v

        for k, org, replacement in cls.__sis_hash_overwrite__:
            if k in d and d[k] == org:
                d[k] = replacement
        if cls.__sis_version__ is None:
            return tools.sis_hash(d)
        else:
            return tools.sis_hash((d, cls.__sis_version__))

    def hold(self):
        """A job set to hold will not be started, but all required jobs will be run.
        :return:
        """
        self._sis_hold_job = True

    def _sis_is_set_to_hold(self):
        """Return True if job is set to hold"""
        return self._sis_hold_job or os.path.exists(self._sis_path(gs.STATE_HOLD))
