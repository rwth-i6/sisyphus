from typing import Any
import os
import subprocess

import time
import logging

import getpass  # used to get username
import math

import xml.etree.cElementTree
from collections import defaultdict, namedtuple

import sisyphus.global_settings as gs
from sisyphus.engine import EngineBase
from sisyphus.global_settings import STATE_RUNNING, STATE_UNKNOWN, STATE_QUEUE, STATE_QUEUE_ERROR

ENGINE_NAME = "sge"
TaskInfo = namedtuple("TaskInfo", ["job_id", "task_id", "state"])


def escape_name(name):
    """
    :param str name:
    :rtype: str
    """
    return name.replace("/", ".")


def try_to_multiply(y, x, backup_value=None):
    """
    Tries to convert y to float multiply it by x and convert it back
    to a rounded string.
    return backup_value if it fails
    return y if backup_value == None

    :param str y:
    :param int|float x:
    :param str|None backup_value:
    :rtype: str
    """

    try:
        return str(int(float(y) * x))
    except ValueError:
        if backup_value is None:
            return y
        else:
            return backup_value


class PBSEngine(EngineBase):
    def __init__(self, default_rqmt, gateway=None, auto_clean_eqw=True, ignore_jobs=None, pe_name="mpi"):
        """

        :param dict default_rqmt: dictionary with the default rqmts
        :param str gateway: ssh to that node and run all sge commands there
        :param bool auto_clean_eqw: if True jobs in eqw will be set back to qw automatically
        :param list[str] ignore_jobs: list of job ids that will be ignored during status updates.
                                      Useful if a job is stuck inside of SGE and can not be deleted.
                                      Job should be listed as "job_number.task_id" e.g.: ['123.1', '123.2', '125.1']
        :param str pe_name: used to select parallel environment (PE), when multi_node_slots is set in rqmt,
                            as `-pe <pe_name> <multi_node_slots>`.
                            The default "mpi" is somewhat arbitrarily chosen as we have it in our environment.
        """
        self._task_info_cache_last_update = 0
        self.gateway = gateway
        self.default_rqmt = default_rqmt
        self.auto_clean_eqw = auto_clean_eqw
        if ignore_jobs is None:
            ignore_jobs = []
        self.ignore_jobs = ignore_jobs
        self.pe_name = pe_name

    def _system_call_timeout_warn_msg(self, command: Any) -> str:
        if self.gateway:
            return f"SSH command timeout: {command!s}"
        return f"Command timeout: {command!s}"

    def system_call(self, command, send_to_stdin=None):
        """
        :param list[str] command: qsub command
        :param str|None send_to_stdin: shell code, e.g. the command itself to execute
        :return: stdout, stderr, retval
        :rtype: list[bytes], list[bytes], int
        """
        if self.gateway:
            system_command = ["ssh", "-x", self.gateway] + [" ".join(["cd", os.getcwd(), "&&"] + command)]
        else:
            # no gateway given, skip ssh local
            system_command = command

        logging.debug("shell_cmd: %s" % " ".join(system_command))
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
            o = o.split(b"\n")
            if o[-1] != b"":
                print(o[-1])
                assert False
            return o[:-1]

        out = fix_output(out)
        err = fix_output(err)
        retval = p.wait(timeout=30)

        # Check for ssh error
        err_ = []
        for raw_line in err:
            lstart = "ControlSocket"
            lend = "already exists, disabling multiplexing"
            line = raw_line.decode("utf8").strip()
            if line.startswith(lstart) and line.endswith(lend):
                # found ssh connection problem
                ssh_file = line[len(lstart) : len(lend)].strip()
                logging.warning("SSH Error %s" % line.strip())
                try:
                    os.unlink(ssh_file)
                    logging.info("Delete file %s" % ssh_file)
                except OSError:
                    logging.warning("Could not delete %s" % ssh_file)
            else:
                err_.append(raw_line)

        return out, err_, retval

    def options(self, rqmt):
        out = []
        l_out = []
        try:
            mem = "%igb" % math.ceil(float(rqmt["mem"]))
        except ValueError:
            mem = rqmt["mem"]

        l_out.append("mem=%s" % mem)
        l_out.append("ngpus=%s" % rqmt.get("gpu", 0))
        l_out.append("ncpus=%s" % rqmt.get("cpu", 1))
        out.append("-lselect=1:%s" % ":".join(l_out))

        qsub_args = rqmt.get("qsub_args", [])
        if isinstance(qsub_args, str):
            qsub_args = qsub_args.split()
        out += qsub_args
        return out

    def submit_call(self, call, logpath, rqmt, name, task_name, task_ids):
        """
        :param list[str] call:
        :param str logpath:
        :param dict[str] rqmt:
        :param str name:
        :param str task_name:
        :param list[int] task_ids:
        :return: ENGINE_NAME, submitted (list of (list of task ids, job id))
        :rtype: (str, list[(list[int],str)])
        """
        if not task_ids:
            # skip empty list
            return

        submitted = []
        start_id, end_id, step_size = (None, None, None)
        for task_id in task_ids:
            if start_id is None:
                start_id = task_id
            elif end_id is None:
                end_id = task_id
                step_size = end_id - start_id
            elif task_id == end_id + step_size:
                end_id = task_id
            else:
                # this id doesn't fit pattern, this should only happen if only parts of the jobs are restarted
                job_id = self.submit_helper(call, logpath, rqmt, name, task_name, start_id, end_id, step_size)
                submitted.append((list(range(start_id, end_id, step_size)), job_id))
                start_id, end_id, step_size = (task_id, None, None)
        assert start_id is not None
        if end_id is None:
            end_id = start_id
            step_size = 1
        job_id = self.submit_helper(call, logpath, rqmt, name, task_name, start_id, end_id, step_size)
        submitted.append((list(range(start_id, end_id, step_size)), job_id))
        return ENGINE_NAME, submitted

    def submit_helper(self, call, logpath, rqmt, name, task_name, start_id, end_id, step_size):
        """
        :param list[str] call:
        :param str logpath:
        :param dict[str] rqmt:
        :param str name:
        :param str task_name:
        :param int start_id:
        :param int end_id:
        :param int step_size:
        :rtype: str|None
        """
        name = escape_name(name)
        qsub_call = ["qsub", "-N", name, "-k oed", "-j", "oe", "-o", logpath, "-S", "/bin/bash", "-m", "n"]
        qsub_call += self.options(rqmt)

        if start_id > 1 and end_id > 1:
            qsub_call += ["-J", "%i-%i:%i" % (start_id, end_id, step_size)]
        command = " ".join(call) + "\n"
        while True:
            try:
                out, err, retval = self.system_call(qsub_call, command)
            except subprocess.TimeoutExpired:
                logging.warning(self._system_call_timeout_warn_msg(command))
                time.sleep(gs.WAIT_PERIOD_SSH_TIMEOUT)
                continue
            break

        job_id = None
        if len(out) == 1:
            pass
        else:
            logging.error("Error to submit job, return value: %i" % retval)
            logging.error("QSUB command: %s" % " ".join(qsub_call))
            for line in out:
                logging.error("Output: %s" % line.decode())
            for line in err:
                logging.error("Error: %s" % line.decode())

            # reset cache, after error
            self.reset_cache()
        return job_id

    def reset_cache(self):
        self._task_info_cache_last_update = -10

    def queue_state(self):
        """Return s list with all currently running tasks in this queue"""

        if time.time() - self._task_info_cache_last_update < 30:
            # use cached value
            return self._task_info_cache

        # get qstat output
        system_command = ["qstat -f -F json"]
        while True:
            try:
                out, err, retval = self.system_call(system_command)
            except subprocess.TimeoutExpired:
                logging.warning(self._system_call_timeout_warn_msg(system_command))
                time.sleep(gs.WAIT_PERIOD_SSH_TIMEOUT)
                continue
            break

        import json
        job_dict = json.loads(b"\n".join(out))
        task_infos = defaultdict(list)
        username = getpass.getuser()
        for job_id, job in job_dict["Jobs"].items():
            try:
                job_user = job["Job_Owner"].split("@")[0]
                if job_user != username:
                    continue
                state = job["job_state"]
                name = job["Job_Name"]
                task = 1  # TODO
                task_infos[(name, task)].append((job_id, state))
            except Exception:
                logging.warning("Failed to parse squeue output: %s" % str(job))

        self._task_info_cache = task_infos
        self._task_info_cache_last_update = time.time()
        return task_infos

    def output_path(self):
        """Return s list with all currently running tasks in this queue"""

        if time.time() - self._task_info_cache_last_update < 30:
            # use cached value
            return self._task_info_cache

        # get qstat output
        system_command = ["qstat -f -F json"]
        while True:
            try:
                out, err, retval = self.system_call(system_command)
            except subprocess.TimeoutExpired:
                logging.warning(self._system_call_timeout_warn_msg(system_command))
                time.sleep(gs.WAIT_PERIOD_SSH_TIMEOUT)
                continue
            break

        import json
        job_dict = json.loads(b"\n".join(out))
        job_id = os.getenv("PBS_JOBID")
        job = job_dict["Jobs"][job_id]
        return job["Output_Path"].split(":")[-1]

    def task_state(self, task, task_id):
        """Return task state:
        'r' == STATE_RUNNING
        'qw' == STATE_QUEUE
        not found == STATE_UNKNOWN
        everything else == STATE_QUEUE_ERROR
        """

        name = task.task_name()
        name = escape_name(name)
        task_name = (name, task_id)
        queue_state = self.queue_state()
        qs = queue_state[task_name]

        # task name should be uniq
        if len(qs) > 1:
            logging.warning(
                "More then one matching SGE task, use first match < %s > matches: %s" % (str(task_name), str(qs))
            )

        if qs == []:
            return STATE_UNKNOWN
        state = qs[0][1]
        if state in ["R"]:
            return STATE_RUNNING
        elif state == "Q":
            return STATE_QUEUE
        elif state == "Eqw":
            if self.auto_clean_eqw:
                logging.info("Clean job in error state: %s, %s, %s" % (name, task_id, qs))
                self.system_call(["qmod", "-cj", "%s.%s" % (qs[0][0], task_id)])
            return STATE_QUEUE_ERROR
        else:
            return STATE_QUEUE_ERROR

    def start_engine(self):
        """No starting action required with the current implementation"""
        pass

    def stop_engine(self):
        """No stopping action required with the current implementation"""
        pass

    @staticmethod
    def get_task_id(task_id):
        assert task_id is None, "PBS task should not be started with task id, it's given via $PBS_TASKNUM"
        task_id = os.getenv("PBS_TASKNUM")
        if task_id in ["undefined", None]:
            # SGE without an array job
            logging.critical("Job started without task_id, this should not happen! Continue with task_id=1")
            return 1
        else:
            return int(task_id)

    def get_default_rqmt(self, task):
        return self.default_rqmt

    def init_worker(self, task):
        # setup log file by linking to engine logfile
        task_id = PBSEngine.get_task_id(None)
        logpath = os.path.relpath(task.path(gs.JOB_LOG, task_id))
        if os.path.isfile(logpath):
            os.unlink(logpath)

        engine_logpath = self.output_path()

        try:
            if os.path.isfile(engine_logpath):
                os.link(engine_logpath, logpath)
            else:
                logging.warning("Could not find engine logfile: %s Create soft link anyway." % engine_logpath)
                os.symlink(os.path.relpath(engine_logpath, os.path.dirname(logpath)), logpath)
        except FileExistsError:
            pass
