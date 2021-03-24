# Author: Wilfried Michel <michel@cs.rwth-aachen.de>

from collections import defaultdict, namedtuple
from enum import Enum
import getpass  # used to get username
import logging
import math
import os
import subprocess
import time
from xml.dom import minidom
import xml.etree.cElementTree

import sisyphus.global_settings as gs
from sisyphus.engine import EngineBase
from sisyphus.global_settings import STATE_RUNNING, STATE_UNKNOWN, STATE_QUEUE, STATE_QUEUE_ERROR

ENGINE_NAME = 'slurm'
TaskInfo = namedtuple('TaskInfo', ["job_id", "task_id", "state"])


def escape_name(name):
    return name.replace('/', '.')


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


class SimpleLinuxUtilityForResourceManagementEngine(EngineBase):

    class MemoryAllocationType(Enum):
        PER_CPU = "per_cpu"
        PER_NODE = "per_node"

    def __init__(self, default_rqmt, gateway=None, has_memory_resource=True, auto_clean_eqw=True, ignore_jobs=[], memory_allocation_type=MemoryAllocationType.PER_CPU):
        """

        :param dict default_rqmt: dictionary with the default rqmts
        :param str gateway: ssh to that node and run all sge commands there
        :param bool has_memory_resource: Set to False if the Slurm setup was not configured for managing memory
        :param bool auto_clean_eqw: if True jobs in eqw will be set back to qw automatically
        :param list[str] ignore_jobs: list of job ids that will be ignored during status updates.
                                      Useful if a job is stuck inside of Slurm and can not be deleted.
                                      Job should be listed as "job_number.task_id" e.g.: ['123.1', '123.2', '125.1']
        """
        self._task_info_cache_last_update = 0
        self.gateway = gateway
        self.default_rqmt = default_rqmt
        self.has_memory_resource = has_memory_resource
        self.auto_clean_eqw = auto_clean_eqw
        self.ignore_jobs = ignore_jobs
        self.memory_allocation_type = memory_allocation_type

    def system_call(self, command, send_to_stdin=None):
        """
        :param list[str] command: qsub command
        :param str|None send_to_stdin: shell code, e.g. the command itself to execute
        :return: stdout, stderr, retval
        :rtype: list[bytes], list[bytes], int
        """
        if self.gateway:
            system_command = ['ssh', '-x', self.gateway] + [' '.join(['cd', os.getcwd(), '&&'] + command)]
        else:
            # no gateway given, skip ssh local
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

        # Check for ssh error
        err_ = []
        for raw_line in err:
            lstart = 'ControlSocket'
            lend = 'already exists, disabling multiplexing'
            line = raw_line.decode('utf8').strip()
            if line.startswith(lstart) and line.endswith(lend):
                # found ssh connection problem
                ssh_file = line[len(lstart):len(lend)].strip()
                logging.warning('SSH Error %s' % line.strip())
                try:
                    os.unlink(ssh_file)
                    logging.info('Delete file %s' % ssh_file)
                except OSError:
                    logging.warning('Could not delete %s' % ssh_file)
            else:
                err_.append(raw_line)

        return out, err_, retval

    def options(self, rqmt):
        out = []
        if self.has_memory_resource:
          try:
              mem = "%iG" % math.ceil(float(rqmt['mem']))
          except ValueError:
              mem = rqmt['mem']

        if self.memory_allocation_type == self.MemoryAllocationType.PER_CPU:
            out.append('--mem-per-cpu=%s' % mem)
        elif self.memory_allocation_type == self.MemoryAllocationType.PER_NODE:
            out.append('--mem=%s' % mem)

        if 'rss' in rqmt:
            pass  # there is no option in SLURM?

        if rqmt.get('gpu', 0) > 0:
            out.append('--gres=gpu:%s' % rqmt.get('gpu', 0))

        out.append('--cpus-per-task=%s' % rqmt.get('cpu', 1))

        # Try to convert time to float, calculate minutes from it
        # and convert it back to an rounded string
        # If it fails use string directly
        task_time = try_to_multiply(rqmt['time'], 60)  # convert to minutes if possible

        out.append('--time=%s' % task_time)
        out.append('--export=all')

        sbatch_args = rqmt.get('sbatch_args', [])
        if isinstance(sbatch_args, str):
            sbatch_args = sbatch_args.split()
        out += sbatch_args
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
        :rtype: str, list[(list[int],int)]
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
        """
        name = escape_name(name)
        sbatch_call = [
            'sbatch',
            '-J',
            name,
            '-o',
            logpath + '/%x.%A.%a',
            '--mail-type=None']
        sbatch_call += self.options(rqmt)

        sbatch_call += ['-a', '%i-%i:%i' % (start_id, end_id, step_size)]
        command = '"' + ' '.join(call) + '"'
        sbatch_call += ['--wrap=%s' % ' '.join(call)]
        try:
            out, err, retval = self.system_call(sbatch_call)
        except subprocess.TimeoutExpired:
            logging.warning('SSH command timeout %s' % str(command))
            time.sleep(gs.WAIT_PERIOD_SSH_TIMEOUT)
            return self.submit_helper(call, logpath, rqmt, name, task_name, start_id, end_id, step_size)

        ref_output = ['Submitted', 'batch', 'job']
        ref_output = [i.encode() for i in ref_output]

        job_id = None
        if len(out) == 1:
            sout = out[0].split()
            if retval != 0 or len(err) > 0 or len(sout) != 4 or sout[0:3] != ref_output:
                print(retval, len(err), len(sout), sout[0:3], ref_output)
                logging.error("Error to submit job")
                logging.error("SBATCH command: %s" % ' '.join(sbatch_call))
                for line in out:
                    logging.error("Output: %s" % line.decode())
                for line in err:
                    logging.error("Error: %s" % line.decode())
                # reset cache, after error
                self.reset_cache()
            else:
                job_id = sout[3].decode().split('.')

                logging.info("Submitted with job_id: %s %s" % (job_id, name))
                for task_id in range(start_id, end_id, step_size):
                    self._task_info_cache[(name, task_id)].append((job_id, 'PD'))

        else:
            logging.error("Error to submit job, return value: %i" % retval)
            logging.error("SBATCH command: %s" % ' '.join(sbatch_call))
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
        """ Returns list with all currently running tasks in this queue """

        if time.time() - self._task_info_cache_last_update < 30:
            # use cached value
            return self._task_info_cache

        # get bjobs output
        system_command = ['squeue', '-h', '--array', '-u', getpass.getuser(),
                          '-O', 'arrayjobid,arraytaskid,state,name:1000']
        try:
            out, err, retval = self.system_call(system_command)
        except subprocess.TimeoutExpired:
            logging.warning('SSH command timeout %s' % str(system_command))
            time.sleep(gs.WAIT_PERIOD_SSH_TIMEOUT)
            return self.queue_state()

        task_infos = defaultdict(list)
        for line in out:
            line = line.decode()
            try:
                field = line.split()
                name = field[3]
                state = field[2]
                task = 1 if field[1] == "N/A" else int(field[1])
                number = field[0]
                task_infos[(name, task)].append((number, state))
            except Exception:
                logging.warning("Failed to parse squeue output: %s" % line)

        self._task_info_cache = task_infos
        self._task_info_cache_last_update = time.time()
        return task_infos

    def task_state(self, task, task_id):
        """ Return task state:
        'RUNNING' == STATE_RUNNING
        'PENDING' == STATE_QUEUE
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
                'More then one matching SLURM task, use first match < %s > matches: %s' %
                (str(task_name), str(qs)))

        if qs == []:
            return STATE_UNKNOWN
        state = qs[0][1]
        if state in ['RUNNING', 'COMPLETING']:
            return STATE_RUNNING
        elif state in ['PENDING', 'CONFIGURING']:
            return STATE_QUEUE
        else:
            return STATE_UNKNOWN

    def start_engine(self):
        """ No starting action required with the current implementation """
        pass

    def stop_engine(self):
        """ No stopping action required with the current implementation """
        pass

    @staticmethod
    def get_task_id(task_id):
        assert task_id is None, "SLURM task should not be started with task id, it's given via $SLURM_ARRAY_TASK_ID"
        task_id = os.getenv('SLURM_ARRAY_TASK_ID')
        if task_id in ['N/A', None]:
            # SLURM without an array job
            logging.critical("Job started without task_id, this should not happen! Continue with task_id=1")
            return 1
        else:
            return int(task_id)

    def get_default_rqmt(self, task):
        return self.default_rqmt

    def init_worker(self, task):
        # setup log file by linking to engine logfile
        task_id = self.get_task_id(None)
        logpath = os.path.relpath(task.path(gs.JOB_LOG, task_id))
        if os.path.isfile(logpath):
            os.unlink(logpath)

        engine_logpath = os.path.dirname(logpath) + '/engine/' + os.getenv('SLURM_JOB_NAME') + \
            '.' + os.getenv('SLURM_ARRAY_JOB_ID') + '.' + os.getenv('SLURM_ARRAY_TASK_ID')
        try:
            if os.path.isfile(engine_logpath):
                os.link(engine_logpath, logpath)
            else:
                logging.warning("Could not find engine logfile: %s Create soft link anyway." % engine_logpath)
                os.symlink(os.path.relpath(engine_logpath, os.path.dirname(logpath)), logpath)
        except FileExistsError:
            pass

    def get_logpath(self, logpath_base, task_name, task_id):
        """ Returns log file for the currently running task """
        return os.path.join(logpath_base, "%s.%s.%i" % (task_name, os.getenv('SLURM_ARRAY_JOB_ID'), task_id))
