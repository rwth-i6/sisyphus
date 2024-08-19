# Author: Wilfried Michel <michel@cs.rwth-aachen.de>

from typing import Any
import os
import subprocess

import time
import logging

from collections import defaultdict, namedtuple

import sisyphus.global_settings as gs
from sisyphus.engine import EngineBase
from sisyphus.global_settings import STATE_RUNNING, STATE_UNKNOWN, STATE_QUEUE, STATE_QUEUE_ERROR

ENGINE_NAME = "lsf"
TaskInfo = namedtuple("TaskInfo", ["job_id", "task_id", "state"])


def escape_name(name):
    return name.replace("/", ".")


def try_to_multiply(y, x, backup_value=None):
    """Tries to convert y to float multiply it by x and convert it back
    to a rounded string.
    return backup_value if it fails
    return y if backup_value == None"""

    try:
        return str(int(float(y) * x))
    except ValueError:
        if backup_value is None:
            return y
        else:
            return backup_value


class LoadSharingFacilityEngine(EngineBase):
    def __init__(self, default_rqmt, gateway=None, auto_clean_eqw=True):
        self._task_info_cache_last_update = 0
        self.gateway = gateway
        self.default_rqmt = default_rqmt
        self.auto_clean_eqw = auto_clean_eqw

    def _system_call_timeout_warn_msg(self, command: Any) -> str:
        if self.gateway:
            return f"SSH command timeout: {command!s}"
        return f"Command timeout: {command!s}"

    def _system_call_error_warn_msg(self, command: Any) -> str:
        if self.gateway:
            return f"SSH command error: {command!s}"
        return f"Command error: {command!s}"

    def system_call(self, command, send_to_stdin=None):
        if self.gateway:
            system_command = ["ssh", "-x", self.gateway] + [" ".join(["cd", os.getcwd(), "&&"] + command)]
        else:
            # no gateway given, skip ssh local
            system_command = command

        logging.debug("shell_cmd: %s" % " ".join(system_command))
        if send_to_stdin:
            send_to_stdin = send_to_stdin.encode()

        p = subprocess.run(system_command, input=send_to_stdin, capture_output=True, timeout=30)

        def fix_output(o):
            # split output and drop last empty line
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

        return (out, err_, retval)

    def options(self, rqmt):
        out = []

        mem = try_to_multiply(rqmt["mem"], 1024)  # convert to Megabyte if possible

        out.append("-M %s" % mem)

        if "rss" in rqmt:
            rss = try_to_multiply(rqmt["rss"], 1024)  # convert to Megabyte if possible
            out.append("-v %s" % rss)
        else:
            out.append("-v %s" % mem)

        if rqmt.get("gpu", 0) > 0:
            out.append("-a gpu")

        out.append("-n %s" % rqmt.get("cpu", 1))

        # Try to convert time to float, calculate minutes from it
        # and convert it back to an rounded string
        # If it fails use string directly
        task_time = try_to_multiply(rqmt["time"], 60)  # convert to minutes if possible

        out.append("-W %s" % task_time)

        if rqmt.get("multi_node_slots", None):
            raise NotImplementedError("Multi-node slots are not implemented for LSF")

        bsub_args = rqmt.get("bsub_args", [])
        if isinstance(bsub_args, str):
            bsub_args = bsub_args.split()
        out += bsub_args
        return out

    def submit_call(self, call, logpath, rqmt, name, task_name, task_ids):
        if not task_ids:
            # skip empty list
            return

        submitted = []
        start_id, end_id, entrycounter, submitstring, submitlist = (None, None, 0, "", [])
        for task_id in task_ids:
            if start_id is None:
                start_id = task_id
            elif (end_id is None and task_id == start_id + 1) or task_id == end_id + 1:
                end_id = task_id
            else:
                # this id doesn't fit pattern, this should only happen if only parts of the jobs are restarted
                if end_id is None:
                    submitstring += "%i," % (start_id)
                    submitlist += [start_id]
                    start_id = task_id
                else:
                    submitstring += "%i-%i," % (start_id, end_id)
                    submitlist += list(range(start_id, end_id + 1))
                    start_id, end_id = (task_id, None)
                entrycounter += 1
                # The submitstring must not get longer than 255 chars. Assume job_id's are 4 digit numbers at max
                if entrycounter == 20:
                    job_id = self.submit_helper(call, logpath, rqmt, name, task_name, submitstring[:-1])
                    submitted.append((submitlist, job_id))
                    entrycounter, submitstring, submitlist = (0, "", [])
        assert start_id is not None
        if end_id is None:
            end_id = start_id
        submitstring += "%i-%i," % (start_id, end_id)
        submitlist += list(range(start_id, end_id + 1))
        job_id = self.submit_helper(call, logpath, rqmt, name, task_name, submitstring[:-1])
        submitted.append((submitlist, job_id))
        return (ENGINE_NAME, submitted)

    def submit_helper(self, call, logpath, rqmt, name, task_name, rangestring):
        name = escape_name(name)
        bsub_call = [
            "bsub",
            "-J",
            "%s[%s]" % (name, rangestring),
            "-o",
            "%s/%s.%%J.%%I" % (logpath, name.split(".")[-1]),
        ]

        bsub_call += self.options(rqmt)
        # TODO these are commands very depended on the RWTH cluster, should be changed to be an option
        command = (
            ". /usr/local_host/etc/bashrc; module unload intel; module load gcc/7; "
            "module load python/3.6.0; module load cuda/80; module load intelmkl/2018; "
            + " ".join(call + ["--redirect_output"])
            + "\n"
        )

        while True:
            logging.info("bsub_call: %s" % bsub_call)
            logging.info("command: %s" % command)
            try:
                out, err, retval = self.system_call(bsub_call, command)
            except subprocess.TimeoutExpired:
                logging.warning(self._system_call_timeout_warn_msg(bsub_call))
                time.sleep(gs.WAIT_PERIOD_SSH_TIMEOUT)
                continue
            break

        ref_output = ["Job", "is", "submitted", "to", "queue"]
        ref_output = [i.encode() for i in ref_output]

        job_id = None
        if len(out) == 1:
            sout = out[0].split()
            if retval != 0 or len(err) > 0 or len(sout) != 7 or sout[0:1] + sout[2:6] != ref_output:
                print(retval, len(err), len(sout), sout[0:2], sout[3:], ref_output)
                logging.error("Error to submit job")
                logging.error("BSUB command: %s" % " ".join(bsub_call))
                for line in out:
                    logging.error("Output: %s" % line.decode())
                for line in err:
                    logging.error("Error: %s" % line.decode())
                # reset cache, after error
                self.reset_cache()
            else:
                job_id = sout[1].decode()[1:-1]

                logging.info("Submitted with job_id: %s %s" % (job_id, name))
                for entry in rangestring.split(","):
                    if "-" in entry:
                        start_id, end_id = entry.split("-")
                        for task_id in range(int(start_id), int(end_id) + 1):
                            self._task_info_cache[(name, task_id)].append((job_id, "PEND"))
                    else:
                        self._task_info_cache[(name, int(entry))].append((job_id, "PEND"))

        else:
            logging.error("Error to submit job, return value: %i" % retval)
            logging.error("BSUB command: %s" % " ".join(bsub_call))
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
        """Returns list with all currently running tasks in this queue"""

        if time.time() - self._task_info_cache_last_update < 30:
            # use cached value
            return self._task_info_cache

        # get bjobs output
        system_command = ["bjobs", "-w"]
        while True:
            try:
                out, err, retval = self.system_call(system_command)
                if retval != 0:
                    raise subprocess.CalledProcessError(
                        retval, system_command, self._system_call_error_warn_msg(system_command)
                    )
            except subprocess.TimeoutExpired:
                logging.warning(self._system_call_timeout_warn_msg(system_command))
                time.sleep(gs.WAIT_PERIOD_SSH_TIMEOUT)
                continue
            break

        task_infos = defaultdict(list)
        for line in out[1:]:
            line = line.decode()
            try:
                field = line.split()
                name = "[".join(field[6].split("[")[:-1])
                state = field[2]
                task = int(field[6].split("[")[-1].split("]")[0])
                number = field[0]
                task_infos[(name, task)].append((number, state))
            except Exception:
                logging.warning("Failed to parse bjobs -w output: %s" % line)

        self._task_info_cache = task_infos
        self._task_info_cache_last_update = time.time()
        return task_infos

    def task_state(self, task, task_id):
        """Return task state:
        'RUN', 'PROV' == STATE_RUNNING
        'PEND', 'WAIT' == STATE_QUEUE
        not found == STATE_UNKNOWN
        everything else == STATE_QUEUE_ERROR
        """

        name = task.task_name()
        name = escape_name(name).encode()
        task_name = (name, task_id)
        try:
            queue_state = self.queue_state()
        except subprocess.CalledProcessError:
            return STATE_QUEUE_ERROR
        qs = queue_state[task_name]

        # task name should be uniq
        if len(qs) > 1:
            logging.warning(
                "More then one matching LSF task, use first match < %s > matches: %s" % (str(task_name), str(qs))
            )

        if qs == []:
            return STATE_UNKNOWN
        state = qs[0][1]
        if state in ["RUN", "PROV"]:
            return STATE_RUNNING
        elif state in ["PEND", "WAIT"]:
            return STATE_QUEUE
        else:
            return STATE_QUEUE_ERROR

    def start_engine(self):
        """No starting action required with the current implementation"""
        pass

    def stop_engine(self):
        """No stopping action required with the current implementation"""
        pass

    def get_task_id(self, task_id):
        assert task_id is None, "LSB task should not be started with task id, it's given via $LSB_JOBINDEX"
        task_id = os.getenv("LSB_JOBINDEX")
        if task_id in ["undefined", None]:
            # LSB without an array job
            logging.critical("Job started without task_id, this should not happen! Continue with task_id=1")
            return 1
        else:
            return int(task_id)

    def get_default_rqmt(self, task):
        return self.default_rqmt

    @staticmethod
    def get_logpath(logpath_base, task_name, task_id, engine_selector=None):
        """Returns log file for the currently running task"""
        return os.path.join(logpath_base, "%s.%s.%i" % (task_name, os.getenv("LSB_JOBID"), task_id))
