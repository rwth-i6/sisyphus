# Author: Jan-Thorsten Peter <peter@cs.rwth-aachen.de>

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


class SonOfGridEngine(EngineBase):
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
            system_command = ["ssh", "-x", self.gateway, "-o", "BatchMode=yes"] + [
                " ".join(["cd", os.getcwd(), "&&"] + command)
            ]
        else:
            # no gateway given, skip ssh local
            system_command = command

        logging.debug("shell_cmd: %s" % " ".join(system_command))
        if send_to_stdin:
            send_to_stdin = send_to_stdin.encode()
        try:
            p = subprocess.run(system_command, input=send_to_stdin, capture_output=True, timeout=30)
        except subprocess.TimeoutExpired:
            logging.warning(self._system_call_timeout_warn_msg(system_command))
            return [], ["TimeoutExpired".encode()], -1

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

        out = fix_output(p.stdout)
        err = fix_output(p.stderr)
        retval = p.returncode

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
        try:
            mem = "%iG" % math.ceil(float(rqmt["mem"]))
        except ValueError:
            mem = rqmt["mem"]
        # mem = try_to_multiply(s['mem'], 1024*1024*1024) # convert to Gigabyte if possible

        out.append("-l")
        out.append("h_vmem=%s" % mem)

        out.append("-l")

        if "rss" in rqmt:
            try:
                rss = "%iG" % math.ceil(float(rqmt["rss"]))
            except ValueError:
                rss = rqmt["rss"]
            # rss = try_to_multiply(s['rss'], 1024*1024*1024) # convert to Gigabyte if possible
            out.append("h_rss=%s" % rss)
        else:
            out.append("h_rss=%s" % mem)

        try:
            file_size = "%iG" % math.ceil(float(rqmt["file_size"]))
        except (ValueError, KeyError):
            # If a different default value is wanted it can be overwritten by adding
            # 'file_size' to the default_rqmt of this engine.
            file_size = rqmt.get("file_size", "50G")

        out.append("-l")
        out.append("h_fsize=%s" % file_size)

        out.append("-l")
        out.append("gpu=%s" % rqmt.get("gpu", 0))

        out.append("-l")
        out.append("num_proc=%s" % rqmt.get("cpu", 1))

        # Try to convert time to float, calculate minutes from it
        # and convert it back to an rounded string
        # If it fails use string directly
        task_time = try_to_multiply(rqmt["time"], 60 * 60)  # convert to seconds if possible

        out.append("-l")
        out.append("h_rt=%s" % task_time)

        if rqmt.get("multi_node_slots", None):
            out.extend(["-pe", self.pe_name, str(rqmt["multi_node_slots"])])

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
        qsub_call = ["qsub", "-cwd", "-N", name, "-j", "y", "-o", logpath, "-S", "/bin/bash", "-m", "n"]
        qsub_call += self.options(rqmt)

        qsub_call += ["-t", "%i-%i:%i" % (start_id, end_id, step_size)]
        command = " ".join(call) + "\n"
        while True:
            out, err, retval = self.system_call(qsub_call, command)
            if retval != 0:
                time.sleep(gs.WAIT_PERIOD_SSH_TIMEOUT)
                continue
            break

        ref_output = ["Your", "job-array", '("%s")' % name, "has", "been", "submitted"]
        ref_output = [i.encode() for i in ref_output]

        job_id = None
        if len(out) == 1:
            sout = out[0].split()
            if len(sout) == 7 and sout[3].startswith(b'("') and sout[3].endswith(b'")'):
                if sout[3][2:-2] != name.encode() and name.encode().startswith(sout[3][2:-2]):
                    # SGE can cutoff the job-name. Fix that.
                    ref_output[2] = sout[3]
            if retval != 0 or len(err) > 0 or len(sout) != 7 or sout[0:2] + sout[3:] != ref_output:
                print(retval, len(err), len(sout), sout[0:2], sout[3:], ref_output)
                logging.error("Error to submit job")
                logging.error("QSUB command: %s" % " ".join(qsub_call))
                for line in out:
                    logging.error("Output: %s" % line.decode())
                for line in err:
                    logging.error("Error: %s" % line.decode())
                # reset cache, after error
                self.reset_cache()
            else:
                sjob_id = sout[2].decode().split(".")
                assert len(sjob_id) == 2
                assert sjob_id[1] == "%i-%i:%i" % (start_id, end_id, step_size)
                job_id = sjob_id[0]

                logging.info("Submitted with job_id: %s %s" % (job_id, name))
                for task_id in range(start_id, end_id, step_size):
                    self._task_info_cache[(name, task_id)].append((job_id, "qw"))

                if False:  # for debugging
                    logging.warning("Boost job!")
                    subprocess.check_call(("qalter", "-p", "300", job_id))

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
        system_command = ["qstat", "-xml", "-u", getpass.getuser()]
        while True:
            out, err, retval = self.system_call(system_command)
            if retval != 0:
                time.sleep(gs.WAIT_PERIOD_SSH_TIMEOUT)
                continue
            break

        xml_data = "".join(i.decode("utf8") for i in out)

        # parse qstat output
        try:
            etree = xml.etree.cElementTree.fromstring(xml_data)
        except xml.etree.cElementTree.ParseError:
            logging.warning(
                "qstat -xml parsing error, retrying\n"
                "command: %s\n"
                "stdout: %s\n"
                "stderr: %s\n"
                "return value: %s" % (system_command, out, err, retval)
            )
            time.sleep(gs.WAIT_PERIOD_QSTAT_PARSING)
            return self.queue_state()

        task_infos = defaultdict(list)
        for job in etree.iter("job_list"):
            job_info = {}
            for attr in job:
                text = attr.text
                if text is not None:
                    text = text.strip()
                job_info[attr.tag] = text

            name = job_info["JB_name"].strip()
            state = job_info["state"].strip()
            task_ids = job_info.get("tasks", None)
            job_number = job_info["JB_job_number"].strip()

            def parse_task_ids(string):
                """
                Return list with all task ids of this task

                :param str|None string:
                :rtype: list[int|None]
                """

                if string is None:
                    # No task id
                    return [None]

                try:
                    # just one task id
                    return [int(string)]
                except ValueError:
                    pass

                if "," in string:
                    # multiple task ids
                    tasks_list = []
                    for i in string.split(","):
                        tasks_list += parse_task_ids(i)
                    return tasks_list

                if ":" in string:
                    # taks list
                    start_end, step_size = string.split(":")
                    start, end = start_end.split("-")
                    return list(range(int(start), int(end) + 1, int(step_size)))
                logging.warning("Can not parse task: %s : %s" % (str(name), str(string)))
                return []

            for task_id in parse_task_ids(task_ids):
                # Check if this task should be ignored, all sisyphus jobs have a task id
                if task_id is not None and "%s.%i" % (job_number, task_id) not in self.ignore_jobs:
                    task_infos[(name, task_id)].append((job_number, state))

        self._task_info_cache = task_infos
        self._task_info_cache_last_update = time.time()
        return task_infos

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
        if state in ["r", "t", "Rr", "Rt"]:
            return STATE_RUNNING
        elif state == "qw":
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
        assert task_id is None, "SGE task should not be started with task id, it's given via $SGE_TASK_ID"
        task_id = os.getenv("SGE_TASK_ID")
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
        task_id = SonOfGridEngine.get_task_id(None)
        logpath = os.path.relpath(task.path(gs.JOB_LOG, task_id))
        if os.path.isfile(logpath):
            os.unlink(logpath)

        engine_logpath = os.getenv("SGE_STDERR_PATH")
        try:
            if os.path.isfile(engine_logpath):
                os.link(engine_logpath, logpath)
            else:
                logging.warning("Could not find engine logfile: %s Create soft link anyway." % engine_logpath)
                os.symlink(os.path.relpath(engine_logpath, os.path.dirname(logpath)), logpath)
        except FileExistsError:
            pass

    def get_logpath(self, logpath_base, task_name, task_id):
        """Returns log file for the currently running task"""
        return os.getenv("SGE_STDERR_PATH")
