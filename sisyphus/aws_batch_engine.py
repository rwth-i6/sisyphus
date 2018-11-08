""" This is an experimental implementation for the aws batch engine.
You need to setup shared file system, I used efs, a docker image which contains all needed packages and mounts
the shared file system and a queue. The docker image must be run in privileged mode.
An example docker image can be found under docs/aws_batch_docker.

 .. moduleauthor:: Jan-Thorsten Peter <peter@cs.rwth-aachen.de>

"""

import os
import subprocess
import json

import time
import logging

import getpass  # used to get username
import math
import threading

from xml.dom import minidom
import xml.etree.cElementTree
from collections import defaultdict, namedtuple

import sisyphus.global_settings as gs
from sisyphus.engine import EngineBase
from sisyphus.global_settings import STATE_RUNNING, STATE_UNKNOWN, STATE_QUEUE, STATE_QUEUE_ERROR

ENGINE_NAME = 'aws'
TaskInfo = namedtuple('TaskInfo', ["job_id", "task_id", "state"])


def escape_name(name, task_id):
    return name.replace('/', '-').replace('.', '-') + '-' + str(task_id)


def try_to_multiply(y, x, backup_value=None):
    """ Tries to convert y to float multiply it by x and convert it back
    to a rounded string.
    return backup_value if it fails
    return y if backup_value == None """

    try:
        return str(int(float(y) * x))
    except ValueError:
        if backup_value is None:
            return y
        else:
            return backup_value


class AWSBatchEngine(EngineBase):

    def __init__(self, default_rqmt, job_queue, job_definition,
                 ignore_failed_jobs_after_x_seconds=900,
                 ignore_succeded_jobs_after_x_seconds=10,
                 cache_result_for_x_seconds=30):
        self._task_info_cache_last_update = 0
        self.default_rqmt = default_rqmt
        self.job_queue = job_queue
        self.job_definition = job_definition
        self.ignore_failed_jobs_after_x_seconds = ignore_failed_jobs_after_x_seconds
        self.ignore_succeded_jobs_after_x_seconds = ignore_succeded_jobs_after_x_seconds
        self.cache_result_for_x_seconds = cache_result_for_x_seconds


        self.lock = threading.Lock()
        self._task_info_cache = {}

    def json_call(self, command, input_dict):
        out = subprocess.check_output(command + ['--output', 'json',
                                                 '--cli-input-json', json.dumps(input_dict)])
        return json.loads(out)

    def system_call(self, command, send_to_stdin=None):
        """
        :param list[str] command: qsub command
        :param str|None send_to_stdin: shell code, e.g. the command itself to execute
        :return: stdout, stderr, retval
        :rtype: list[bytes], list[bytes], int
        """
        system_command = command

        logging.debug('shell_cmd: %s' % ' '.join(system_command))
        p = subprocess.Popen(system_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if send_to_stdin:
            send_to_stdin = send_to_stdin.encode()
        out, err = p.communicate(input=send_to_stdin, timeout=30)

        def fix_output(o):
            """
            split output and drop last empty line
            :param bytes o:
            :rtype: list[bytes]
            """
            o = o.split(b'\n')
            if o[-1] != b'':
                print(o[-1])
                assert(False)
            return o[:-1]

        out = fix_output(out)
        err = fix_output(err)
        retval = p.wait(timeout=30)

        return out, err, retval

    def submit_call(self, call, logpath, rqmt, name, task_name, task_ids):
        """
        :param list[str] call:
        :param str logpath:
        :param dict[str] rqmt:
        :param str name:
        :param str task_name:
        :param list[int] task_ids:
        :return: ENGINE_NAME, submitted (list of (list of task ids, job id))
        :rtype: str, list[(list[int],int)]
        """
        if not task_ids:
            # skip empty list
            return

        submitted = []
        cpu = rqmt.get('cpu', 1)
        mem = int(rqmt.get('mem', 1) * 1024) # AWS uses MiB
        # TODO time
        # time = rqmt.get('mem', 1)
        for task_id in task_ids:
            call_with_id = call[:] + [str(task_id)]
            logfile = self.get_logpath(logpath, task_name, task_id)

            aws_call = {
                "jobName": escape_name(name, task_id),
                "jobQueue": self.job_queue,
                "jobDefinition": self.job_definition,
                "containerOverrides": {
                    "vcpus": cpu,
                    "memory": mem,
                    "command": ['cd', os.getcwd(), '&&'] + call_with_id + ['--redirect_output']
                }
            }
            job_id = self.json_call(['aws', 'batch', 'submit-job'], aws_call)['jobId']

            submitted.append((task_id, job_id))
        return ENGINE_NAME, submitted

    def reset_cache(self):
        self._task_info_cache_last_update = -10

    def queue_state(self):
        with self.lock:
            if time.time() - self._task_info_cache_last_update < self.cache_result_for_x_seconds:
                    # use cached value
                    return self._task_info_cache
            self._task_info_cache_last_update = time.time()

            for state in ['SUCCEEDED', 'FAILED', 'SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING', 'RUNNING']:
                out = subprocess.check_output(['aws', 'batch', 'list-jobs',
                                               '--output', 'json',
                                               '--job-queue', self.job_queue,
                                               '--job-status', state])
                for job in json.loads(out)["jobSummaryList"]:
                    name = job['jobName']
                    status = job['status']
                    if state == 'FAILED':
                        stopped_at = job['stoppedAt'] / 1000
                        age = stopped_at - time.time()
                        if age > self.ignore_failed_jobs_after_x_seconds:
                            if name in self._task_info_cache:
                                del self._task_info_cache[name]
                        else:
                            self._task_info_cache[name] = status
                    elif state == 'SUCCEEDED':
                        stopped_at = job['stoppedAt'] / 1000
                        age = stopped_at - time.time()
                        if age > self.ignore_succeded_jobs_after_x_seconds and name in self._task_info_cache:
                            del self._task_info_cache[name]
                    else:
                        self._task_info_cache[name] = status
            return self._task_info_cache

    def task_state(self, task, task_id):
        """ Return task state:
        'r' == STATE_RUNNING
        'qw' == STATE_QUEUE
        not found == STATE_UNKNOWN
        everything else == STATE_QUEUE_ERROR
        """

        name = task.task_name()
        task_name = escape_name(name, task_id)
        queue_state = self.queue_state()
        qs = queue_state.get(task_name)

        # task name should be uniq
        if qs is None:
            return STATE_UNKNOWN
        if qs in ['SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING']:
            return STATE_QUEUE
        if qs == 'RUNNING':
            return STATE_RUNNING
        if qs == 'FAILED':
            return STATE_QUEUE_ERROR
        logging.warning('Unknown AWS engine state %s' % qs)
        return STATE_UNKNOWN

    def start_engine(self):
        """ No starting action required with the current implementation """
        pass

    def stop_engine(self):
        """ No stopping action required with the current implementation """
        pass

    def get_task_id(self, task_id, engine_selector):
        if task_id is not None:
            # task id passed via argument
            return task_id
        logging.warning("Job in local engine started without task_id, "
                        "worker is probably started manualy. Continue with task_id=1")
        return 1

    def get_default_rqmt(self, task):
        return self.default_rqmt

    @staticmethod
    def get_logpath(logpath_base, task_name, task_id, engine_selector=None):
        """ Returns log file for the currently running task """
        path = os.path.join(logpath_base, gs.ENGINE_LOG)
        path = '%s.%s.%i' % (path, task_name, task_id)
        return path
