import os
import logging
import traceback
import sys
import time
import subprocess as sp
from ast import literal_eval

import sisyphus.tools as tools
import sisyphus.global_settings as gs


class Task(object):
    """
    Object to hold information what function should be run with which requirements.
    """

    def __init__(self, start, resume=None, rqmt={}, args=[[]], mini_task=False,
                 update_rqmt=None, parallel=0, tries=1, continuable=False):
        """
        :param str start: name of the function which will be executed on start
        :param str resume: name of the function which will be executed on resume, often set equal to start
        :param dict[str] rqmt: job requirements
            Might contain:
                "cpu": number of cpus
                "gpu": number of gpus
                "mem": amount of memory, in GB
                "time": amount of time, in hours
        :param list[list[object]|object] args: job arguments
        :param bool mini_task: will be run on engine for short jobs if True
        :param (dict[str],dict[str])->dict[str] update_rqmt: function to update job requirements for interrupted jobs
        :param int parallel: the max. number of jobs to submit to a queue, defaults to the number of args
        :param int tries: how often this task is resubmitted after failure
        :param bool continuable: If set to True this task will not set a finished marker, useful for tasks that can be
                                 continued for arbitrarily long, e.g. adding more epochs to neural network training
        """
        self._start = start
        self._resume = resume
        self._rqmt = rqmt
        if mini_task:
            self._rqmt['engine'] = 'short'
        self._update_rqmt = update_rqmt if update_rqmt else gs.update_engine_rqmt
        self._args = list(args)
        self._parallel = len(self._args) if parallel == 0 else parallel
        self.mini_task = mini_task
        self.reset_cache()
        self.last_state = None
        self.tries = tries
        self.continuable = continuable

    def __repr__(self):
        return "<Task %r job=%r>" % (self._start, getattr(self, "_job", None))

    def reset_cache(self):
        self._state_cache = {}
        self._state_cache_time = {}

    def set_job(self, job):
        """
        :param sisyphus.job.Job job:
        """
        self._job = job
        for name in self._start, self._resume:
            try:
                if name is not None:
                    getattr(self._job, name)
            except AttributeError:
                logging.critical("Trying to create a task with an invalid function name")
                logging.critical("Job name: %s" % str(job))
                logging.critical("Function name: %s" % str(name))
                raise

    def get_f(self, name):
        return getattr(self._job, name)

    def task_ids(self):
        """
        :return: list with all valid task ids
        :rtype: list[int]
        """
        return list(range(1, self._parallel + 1))

    def rqmt(self):
        if callable(self._rqmt):
            rqmt = self._rqmt()
        else:
            rqmt = self._rqmt

        # Ensure that the requested memory is a float representing GB
        if 'mem' in rqmt:
            rqmt['mem'] = tools.str_to_GB(rqmt['mem'])
        if 'time' in rqmt:
            rqmt['time'] = tools.str_to_hours(rqmt['time'])
        return rqmt

    def name(self):
        return self._start

    def resumeable(self):
        return self._resume is not None

    def run(self, task_id, resume_job=False, logging_thread=None):
        """
        This function is executed to run this job.

        :param int task_id:
        :param bool resume_job:
        :param sisyphus.worker.LoggingThread logging_thread:
        """

        logging.debug("Task name: %s id: %s" % (self.name(), task_id))
        job = self._job

        logging.info("Start Job: %s Task: %s" % (job, self.name()))
        logging.info("Inputs:")
        for i in self._job._sis_inputs:
            logging.info(str(i))

            # each input must be at least X seconds old
            # if an input file is too young it's may not synced in a network filesystem yet
            try:
                input_age = time.time() - os.stat(i.get_path()).st_mtime
                time.sleep(max(0, gs.WAIT_PERIOD_MTIME_OF_INPUTS - input_age))
            except FileNotFoundError:
                logging.warning('Input path does not exist: %s' % i.get_path())

            if i.creator and gs.ENABLE_LAST_USAGE:
                # mark that input was used
                try:
                    os.unlink(os.path.join(i.creator, gs.JOB_LAST_USER, os.getlogin()))
                except OSError as e:
                    if e.errno not in (2, 13):
                        # 2: file not found
                        # 13: permission denied
                        raise e

                try:
                    user_path = os.path.join(i.creator, gs.JOB_LAST_USER, os.getlogin())
                    os.symlink(os.path.abspath(job._sis_path()), user_path)
                    os.chmod(user_path, 0o775)
                except OSError as e:
                    if e.errno not in (2, 13, 17):
                        # 2: file not found
                        # 13: permission denied
                        # 17: file exists
                        raise e

        tools.get_system_informations(sys.stdout)

        sys.stdout.flush()

        try:
            if resume_job:
                if self._resume is not None:
                    task = self._resume
                else:
                    task = self._start
                    logging.warning('No resume function set (changed tasks after job was initialized?) '
                                    'Fallback to normal start function: %s' % task)
            else:
                task = self._start
            assert task is not None, "Error loading task"
            # save current directory and change into work directory
            with tools.execute_in_dir(self.path(gs.JOB_WORK_DIR)):
                f = getattr(self._job, task)

                # get job arguments
                for arg_id in self._get_arg_idx_for_task_id(task_id):
                    args = self._args[arg_id]
                    if not isinstance(args, (list, tuple)):
                        args = [args]
                    logging.info("-" * 60)
                    logging.info("Starting subtask for arg id: %d args: %s" % (arg_id, str(args)))
                    logging.info("-" * 60)
                    f(*args)
        except sp.CalledProcessError as e:
            if e.returncode == 137:
                # TODO move this into engine class
                logging.error("Command got killed by SGE (probably out of memory):")
                logging.error("Cmd: %s" % e.cmd)
                logging.error("Args: %s" % str(e.args))
                logging.error("Return-Code: %s" % e.returncode)
                logging_thread.out_of_memory = True
                logging_thread.stop()
            else:
                logging.error("Executed command failed:")
                logging.error("Cmd: %s" % e.cmd)
                logging.error("Args: %s" % str(e.args))
                logging.error("Return-Code: %s" % e.returncode)
                logging_thread.stop()
                self.error(task_id, True)
        except Exception:
            # Job failed
            logging.error("Job failed, traceback:")
            sys.excepthook(*sys.exc_info())
            logging_thread.stop()
            self.error(task_id, True)
            # TODO handle failed job
        else:
            # Job finished normally
            logging_thread.stop()
            if not self.continuable:
                self.finished(task_id, True)
            sys.stdout.flush()
            sys.stderr.flush()
            logging.info("Job finished successful")

    def task_name(self):
        return '%s.%s' % (self._job._sis_id(), self.name())

    def path(self, path_type=None, task_id=None):
        if path_type not in (None, gs.JOB_WORK_DIR, gs.JOB_SAVE, gs.JOB_LOG_ENGINE):
            path_type = '%s.%s' % (path_type, self.name())
        return self._job._sis_path(path_type, task_id)

    def check_state(self, state, task_id=None, update=None, combine=all, minimal_time_since_change=0):
        """
        :param state: name of state
        :param int|list[int]|None task_id:
        :param bool|None update: if not None change state to this value
        :param combine: how states are combines, e.g. only finished if all jobs are finished => all,
                        error state is true if only one or more is has the error flag => any
        :param minimal_time_since_change: only true if state change is at least that old
        :return: if this state is currently set or not
        :rtype: bool
        """

        if task_id is None:
            task_id = self.task_ids()

        current_state = self._job._sis_file_logging(state + '.' + self.name(), task_id, update=update,
                                                    combine=combine, minimal_file_age=minimal_time_since_change)
        return current_state

    def finished(self, task_id=None, update=None):
        minimal_time_since_change = 0
        if not gs.SKIP_IS_FINISHED_TIMEOUT:
            minimal_time_since_change = gs.WAIT_PERIOD_JOB_FS_SYNC + gs.WAIT_PERIOD_JOB_CLEANUP
        if self.check_state(gs.STATE_FINISHED, task_id, update=update, combine=all,
                            minimal_time_since_change=minimal_time_since_change):
            return True
        else:
            return False

    def error(self, task_id=None, update=None):
        """
        :param int|list[int]|None task_id:
        :param bool|None update:
        :return: true if job or task is in error state.
        :rtype: bool
        """

        if update:
            # set error flag
            self.check_state(gs.STATE_ERROR, task_id, update=update, combine=any)
            return True
        if isinstance(task_id, int):
            task_ids = [task_id]
        elif task_id is None:
            task_ids = self.task_ids()
        elif isinstance(task_id, list):
            task_ids = task_id
        else:
            raise Exception("unexpected task_id %r" % (task_id,))

        assert isinstance(task_ids, list)

        for task_id in task_ids:
            error_file = self._job._sis_path(gs.STATE_ERROR + '.' + self.name(), task_id)
            error_file = os.path.realpath(error_file)
            if os.path.isfile(error_file):  # task is in error state
                # move log file and remove error file if a usued try is left
                for i in range(1, self.tries):
                    log_file = self._job._sis_path(gs.JOB_LOG + '.' + self.name(), task_id)
                    new_name = "%s.error.%02i" % (log_file, i)
                    if not os.path.isfile(new_name):
                        if os.path.isfile(log_file):
                            os.rename(log_file, new_name)
                            os.remove(error_file)
                        break

            if os.path.isfile(error_file):
                # task is still in error state
                return True
        return False

    def started(self, task_id=None):
        """ True if job execution has started """
        path = self.path(gs.JOB_LOG, task_id)
        state = os.path.isfile(path)
        return state

    def print_error(self, lines=0):
        for task_id in self.task_ids():
            if self.error(task_id):
                logging.error("Job: %s Task: %s %s" % (self._job._sis_id(), self.name(), task_id))
                logpath = self.path(gs.JOB_LOG, task_id)
                if os.path.exists(logpath):
                    with open(logpath) as log:
                        logging.error("Logfile:")
                        print()
                        if lines > 0:
                            print("".join(log.readlines()[-lines:]), end='')
                        else:
                            print(log.read())

    def state(self, engine, task_id=None, force=False):
        if force or time.time() - self._state_cache_time.get(task_id, -20) >= 10:
            state = self._get_state(engine, task_id)
            self._state_cache[task_id] = state
            self._state_cache_time[task_id] = time.time()
        return self._state_cache[task_id]

    def _get_state(self, engine, task_id=None):
        """ Store return of helper as value as last state """
        self.last_state = self._get_state_helper(engine, task_id)
        return self.last_state

    def _get_state_helper(self, engine, task_id=None):
        """ Return state of this task given by external code """
        # Handling external states
        if self.finished(task_id):
            return gs.STATE_FINISHED
        elif self.error(task_id):
            return gs.STATE_ERROR
        else:
            # Task is not finished and not in error state, time to check the engine
            if task_id is None:
                # Check all task_id of this task, return the 'worst' state
                engine_states = [self.state(engine, i) for i in self.task_ids()]
                for engine_state in (
                        gs.STATE_ERROR,
                        gs.STATE_QUEUE_ERROR,
                        gs.STATE_INTERRUPTED,
                        gs.STATE_RUNNABLE,
                        gs.STATE_QUEUE,
                        gs.STATE_RUNNING,
                        gs.STATE_RETRY_ERROR,
                        gs.STATE_FINISHED):
                    if engine_state in engine_states:
                        return engine_state
                logging.critical("Could not determine state of task: %s" % str(engine_states))
                assert False  # This code point should be unreachable
            else:
                # check state for the given task id
                if engine is None:
                    engine_state = gs.STATE_UNKNOWN
                else:
                    engine_state = engine.task_state(self, task_id)
                    assert engine_state in (gs.STATE_QUEUE, gs.STATE_QUEUE_ERROR, gs.STATE_RUNNING, gs.STATE_UNKNOWN)

                    # force cache update to avoid caching problems if last state was not also UNKNOWN
                    if engine_state == gs.STATE_UNKNOWN and self.last_state and \
                       self.last_state != gs.STATE_UNKNOWN and self.started(task_id):
                        engine.reset_cache()
                        engine_state = engine.task_state(self, task_id)
                        assert engine_state in (gs.STATE_QUEUE, gs.STATE_QUEUE_ERROR,
                                                gs.STATE_RUNNING, gs.STATE_UNKNOWN)

                if engine_state == gs.STATE_UNKNOWN:
                    if self.started(task_id):
                        # check again if it finished or crashed while retrieving the state
                        if self.finished(task_id):
                            return gs.STATE_FINISHED
                        elif self.error(task_id):
                            return gs.STATE_ERROR
                        # job logging file got updated recently, assume job is still running.
                        # used to avoid wrongly marking jobs as interrupted do to slow filesystem updates
                        elif self.running(task_id):
                            return gs.STATE_RUNNING
                        history = [] if engine is None else engine.get_submit_history(self)
                        if history and len(history[task_id]) > gs.MAX_SUBMIT_RETRIES:
                            # More then three tries to run this task, something is wrong
                            return gs.STATE_RETRY_ERROR
                        else:
                            # Task was started, but isn't running anymore => interrupted
                            return gs.STATE_INTERRUPTED
                    else:
                        return gs.STATE_RUNNABLE
                else:
                    if engine_state == gs.STATE_RUNNING and self.running(task_id) is False:
                        # Warn if job is running but doesn't update logging file anymore
                        logging.warning('Job marked as running but logging file has not been updated: '
                                        '%s assume it is running' % str(self._job))
                    return engine_state

    def running(self, task_id):
        """
        :return: True if usage file changed recently, None if usage file doesn't exist False otherwise
        """
        usage_file = self._job._sis_path(gs.PLOGGING_FILE + '.' + self.name(), task_id, abspath=True)
        maximal_file_age = gs.WAIT_PERIOD_JOB_FS_SYNC + gs.PLOGGING_UPDATE_FILE_PERIOD + gs.WAIT_PERIOD_JOB_CLEANUP
        if not os.path.isfile(usage_file):
            return None
        return maximal_file_age > time.time() - os.path.getmtime(usage_file)

    def _get_arg_idx_for_task_id(self, task_id):
        """
        :param int task_id:
        :rtype: list[int]
        """
        assert task_id > 0, "this function assumes task_ids start at 1"
        nargs = len(self._args)
        chunk_size = nargs // self._parallel
        overflow = nargs % self._parallel
        if task_id - 1 < overflow:
            start = (chunk_size + 1) * (task_id - 1)
            return range(start, start + chunk_size + 1)
        else:
            start = (chunk_size + 1) * overflow + chunk_size * (task_id - 1 - overflow)
            return range(start, start + chunk_size)

    def update_rqmt(self, initial_rqmt, submit_history, task_id):
        """ Update task requirements of interrupted job """
        initial_rqmt = initial_rqmt.copy()
        initial_rqmt['mem'] = tools.str_to_GB(initial_rqmt['mem'])
        initial_rqmt['time'] = tools.str_to_hours(initial_rqmt['time'])
        usage_file = self._job._sis_path(gs.PLOGGING_FILE + '.' + self.name(), task_id, abspath=True)

        try:
            last_usage = literal_eval(open(usage_file).read())
        except (SyntaxError, IOError):
            # we don't know anything if no usage file is writen or is invalid, just reuse last rqmts
            return initial_rqmt

        rresources = last_usage['requested_resources']
        if 'mem' in rresources:
            rresources['mem'] = tools.str_to_GB(rresources['mem'])
        if 'time' in rresources:
            rresources['time'] = tools.str_to_hours(rresources['time'])
        new_rqmt = self._update_rqmt(initial_rqmt=initial_rqmt, last_usage=last_usage)
        new_rqmt = gs.check_engine_limits(new_rqmt, self)
        return new_rqmt

    def get_process_logging_path(self, task_id):
        return self._job._sis_path(gs.PLOGGING_FILE + '.' + self.name(), task_id, abspath=True)

    def __str__(self):
        return "Task < workdir(%s) name(%s) ids(%s) >" % (
            self.path(), self.name(), ','.join(str(i) for i in self.task_ids()))

    def get_worker_call(self, task_id=None):
        if isinstance(gs.SIS_COMMAND, list):
            call = gs.SIS_COMMAND[:]
        else:
            call = gs.SIS_COMMAND.split()
        call += [gs.CMD_WORKER, os.path.relpath(self.path()), self.name()]
        if task_id is not None:
            call.append(str(task_id))
        return call
