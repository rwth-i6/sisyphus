# Author: Jan-Thorsten Peter <peter@cs.rwth-aachen.de>

import collections
import logging
import os
import psutil
from ast import literal_eval

import sisyphus.global_settings as gs
import sisyphus.tools as tools


class EngineBase:
    """
    An engine manages the execution of jobs, e.g. locally, or in a queuing system like SGE.
    """

    @staticmethod
    def get_task_id(task_id):
        """ Gets task id either from args or the environment"""
        raise NotImplementedError

    def task_state(self, task, task_id):
        raise NotImplementedError

    def start_engine(self):
        raise NotImplementedError

    def stop_engine(self):
        raise NotImplementedError

    def reset_cache(self):
        raise NotImplementedError

    def submit_call(self, call, logpath, rqmt, name, task_name, task_ids):
        """
        :param list[str] call:
        :param str logpath:
        :param dict[str] rqmt:
        :param str name:
        :param str task_name:
        :param list[int] task_ids:
        :return: ENGINE_NAME, submitted (list of (list of task ids, job id))
        :rtype: (str, list[(list[int],str|int)])
        """
        raise NotImplementedError

    def get_default_rqmt(self, task):
        raise NotImplementedError

    def get_used_engine(self, engine_selector):
        return self

    @tools.cache_result()
    def get_submit_history(self, task):
        submit_log = os.path.relpath(task.path(gs.ENGINE_SUBMIT))
        id_to_rqmt = collections.defaultdict(list)
        if os.path.isfile(submit_log):
            if hasattr(task, 'submit_history_cache'):
                last_update, id_to_rqmt = task.submit_history_cache
            else:
                last_update = None

            current_mtime = os.path.getmtime(submit_log)
            if not last_update or last_update != current_mtime:
                with open(submit_log) as submit_file:
                    for line in submit_file:
                        ids, r = literal_eval(line)  # list of ids, submitted requirement for this id
                        for i in ids:
                            id_to_rqmt[i].append(r)
                task.submit_history_cache = (current_mtime, id_to_rqmt)
        return id_to_rqmt

    def add_defaults_to_rqmt(self, task, rqmt):
        s = self.get_default_rqmt(task).copy()
        s.update(rqmt)
        return s

    def get_rqmt(self, task, task_id, update=True):
        """
        Get the requirements submitted for this task

        :param sisyphus.task.Task task:
        :param int task_id:
        :param bool update:
        """
        rqmt = task.rqmt()

        # find last requirements
        rqmt = self.add_defaults_to_rqmt(task, rqmt)
        rqmt_hist = self.get_submit_history(task)[task_id]
        if rqmt_hist and rqmt_hist[0] == rqmt:
            # job has been submitted before and the rqmt given by the recipe did not change
            rqmt.update(rqmt_hist[-1])
            if update:
                rqmt = task.update_rqmt(rqmt, task_id)

        if 'mem' in rqmt:
            rqmt['mem'] = tools.str_to_GB(rqmt['mem'])
        if 'time' in rqmt:
            rqmt['time'] = tools.str_to_hours(rqmt['time'])
        return gs.check_engine_limits(rqmt, task)

    def job_state(self, job):
        """ Return current state of job """

        if not job._sis_setup():
            if job._sis_runnable():
                return gs.STATE_RUNNABLE
            else:
                return gs.STATE_WAITING
        elif job._sis_finished():
            return gs.STATE_FINISHED

        for task in job._sis_tasks():
            if not task.finished():
                return self.task_state(task)
        return gs.STATE_FINISHED

    def get_job_used_resources(self, current_process):
        """
        Should be overwritten by subclass if a better way to measure the used resources is available, e.g. cgroups.
        This function should only be used by the worker.

        :param psutil.Process current_process:
        :param engine_selector:
        """
        d = {}

        # get memory usage and convert it to GB
        mem_info = current_process.memory_info()
        d['rss'] = mem_info.rss / 1024**3
        d['vms'] = mem_info.vms / 1024**3

        d['cpu'] = current_process.cpu_percent()
        for child in current_process.children(recursive=True):
            try:
                mem_info = child.memory_info()
                d['rss'] += mem_info.rss / 1024**3
                d['vms'] += mem_info.vms / 1024**3
                d['cpu'] += child.cpu_percent()
            except psutil.NoSuchProcess:
                # Quietly continue if job disappeared
                continue
        return d

    def submit(self, task):
        """ Prepares all relevant commands and calls submit_call of subclass to actual
        pass job to relevant engine

        :param sisyphus.task.Task task: Task to submit
        :return: None
        """

        call = task.get_worker_call()
        logpath = os.path.relpath(task.path(gs.JOB_LOG_ENGINE))
        task_ids = [task_id for task_id in task.task_ids()
                    if task.state(self, task_id, True) in [gs.STATE_RUNNABLE, gs.STATE_INTERRUPTED_RESUMABLE]]

        # update rqmts and collect them
        rqmt_to_ids = {}
        for task_id in task_ids:
            rqmt = self.get_rqmt(task, task_id)

            key = tools.sis_hash(rqmt)
            if key not in rqmt_to_ids:
                rqmt_to_ids[key] = (rqmt, set())
            rqmt_, ids = rqmt_to_ids[key]
            assert task_id not in ids
            assert rqmt == rqmt_
            ids.add(task_id)

        # the actuary job submitting part
        submit_log = os.path.relpath(task.path(gs.ENGINE_SUBMIT))
        for rqmt_key, (rqmt, task_ids) in rqmt_to_ids.items():
            task_ids = sorted(task_ids)
            logging.info('Submit to queue: %s %s %s' % (str(task.path()), task.name(), str(task_ids)))
            engine_name, engine_info = self.submit_call(call, logpath, rqmt,
                                                        task.task_name(), task.name(), task_ids)
            logging.debug('Command: (%s) Tasks ids: (%s)' % (' '.join(call), ' '.join([str(i) for i in task_ids])))
            logging.debug('Requirements: %s' % (str(rqmt)))

            submit_info = rqmt.copy()
            submit_info['engine_info'] = engine_info
            submit_info['engine_name'] = engine_name
            with open(submit_log, 'a') as submit_file:
                submit_file.write('%s\n' % str((task_ids, submit_info)))

        task.reset_cache()

    def init_worker(self, task):
        """
        This method will be call before the task is started by the worker.
        e.g. SGE uses this method to link the SGE log file to the desired position.

        :param task:
        :return:
        """
        pass


class EngineSelector(EngineBase):
    """
    The EngineSelector engine wraps multiple other engines
    and schedules the jobs according to the requirements (rqmt).

    Tasks with mini_task=True will use the engine selector 'short',
    so usually that engine should be specified as well.
    """

    def __init__(self, engines, default_engine):
        """
        :param dict[str,EngineBase] engines:
        :param str default_engine:
        """
        assert isinstance(default_engine, str), "default_engine must be a string: %r" % (default_engine,)
        for k, v in engines.items():
            assert isinstance(k, str) and isinstance(v, EngineBase), ("engines must only contain strings as keys "
                                                                      "and Engines as value: (%r, %r)" % (k, v))
        self.engines = engines
        self.default_engine = default_engine

    def get_used_engine(self, engine_selector):
        """
        :param str engine_selector: name in self.engines
        :rtype: EngineBase
        """
        assert engine_selector in self.engines, "Unknown engine selector: %r" % engine_selector
        return self.engines[engine_selector].get_used_engine(engine_selector)

    def get_used_engine_by_rqmt(self, rqmt):
        """
        :param dict[str] rqmt:
        :rtype: EngineBase
        """
        engine_selector = rqmt.get('engine', self.default_engine)
        return self.get_used_engine(engine_selector)

    def get_job_used_resources(self, current_process):
        assert NotImplementedError, "Used active engine first via get_used_engine first"

    def task_state(self, task, task_id):
        """ Return state of task """
        return self.get_used_engine_by_rqmt(task.rqmt()).task_state(task, task_id)

    def for_all_engines(self, f):
        """ Tell all engines to stop """
        visited = set()
        for engine in self.engines.values():
            eid = id(engine)
            if eid not in visited:
                visited.add(eid)
                f(engine)

    def start_engine(self):
        self.for_all_engines(lambda e: e.start_engine())

    def stop_engine(self):
        """ Tell all engines to stop """
        self.for_all_engines(lambda e: e.stop_engine())

    def reset_cache(self):
        self.for_all_engines(lambda e: e.reset_cache())

    # def get_task_id(self, task_id):
    #     # This function should only be used while the worker
    #     assert NotImplementedError, "Used active engine first via get_used_engine first"

    #     """ Gets task id either from args or the environment"""
    #     if task_id is not None:
    #         # task id passed via argument
    #         return task_id
    #     return self.get_used_engine(engine_selector).get_task_id(task_id, engine_selector)

    # def get_logpath(self, logpath, task_name, task_id):
    #     """ Returns log file for the currently running task """
    #     return self.get_used_engine(engine_selector).get_logpath(logpath, task_name, task_id, engine_selector)

    def submit_call(self, call, logpath, rqmt, name, task_name, task_ids):
        engine_selector = rqmt.get('engine', self.default_engine)
        # update call to contain selected engine
        new_call = []
        added = False
        for i in call:
            new_call.append(i)
            if not added and i == gs.CMD_WORKER:
                new_call.append('--engine')
                new_call.append(engine_selector)
                added = True
        return self.get_used_engine(engine_selector).submit_call(new_call, logpath, rqmt, name, task_name, task_ids)

    def get_default_rqmt(self, task):
        return self.get_used_engine_by_rqmt(task.rqmt()).get_default_rqmt(task)
