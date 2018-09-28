import logging
import multiprocessing
import threading
import queue
import os
import socket
import subprocess
import psutil
import time
from sisyphus.engine import EngineBase
from sisyphus import tools
import sisyphus.global_settings as gs
from collections import namedtuple
from ast import literal_eval

# we are only using cpu so far anyway...
ENGINE_NAME = 'local'
LOCAL_DEFAULTS = {
    'cpu': 1,
    'mem': 1,
    'time': 1,
}


def run_task(call, logpath):
    """ Simple function to run task """
    with open(logpath, 'a') as logfile:
        subprocess.check_call(call, stdout=logfile, stderr=logfile)

# name is the unique name combined of job id and task name, task_name is only the task name
TaskQueueInstance = namedtuple('TaskQueueInstance', ['call', 'logpath', 'rqmt', 'name', 'task_name', 'task_id'])


def get_process_logging_path(task_path, task_name, task_id):
    path = os.path.join(task_path, gs.PLOGGING_FILE)
    path = '%s.%s.%i' % (path, task_name, task_id)
    return path


class sync_object(object):

    """ Object to be used by the with statement to sync an object via queue
    e.g.:

    self.sobj = sync_object({})
    with self.sobj as sobj:
        sobj[7] = 9

    # other process
    with self.sobj as sobj:
        assert( sobj == { 7: 9 } )
    """

    def __init__(self, obj):
        self.queue = queue.Queue(1)
        self.obj = obj
        self.queue.put(obj)

    def __enter__(self):
        self.obj = self.queue.get()
        return self.obj

    def __exit__(self, type, value, traceback):
        self.queue.put(self.obj)


class LocalEngine(threading.Thread, EngineBase):

    """ Simple engine to execute running tasks locally,
        the only checked resource is cpus so far
    """

    def __init__(self, cpus=1, gpus=0):
        """
        :param int cpus: number of CPUs to use
        :param int gpus: number of GPUs to use
        """
        # TODO change max_cpu to ressorces and remove multiprocessing stuff
        # resources
        self.lock = threading.Lock()
        self.max_cpu = cpus
        self.cpu = multiprocessing.Value('i', cpus)
        self.max_gpu = gpus
        self.gpu = multiprocessing.Value('i', gpus)
        self.running_subprocess = []
        self.started = False
        self.running = multiprocessing.Value('B', 1)  # set to 0 to stop engine

        threading.Thread.__init__(self)

    def start_engine(self):
        if self.started:
            return
        # control input
        self.input_queue = queue.Queue()
        self.waiting_tasks = sync_object({})

        # control output / which tasks are currently running
        self.running_tasks = sync_object({})

        self.start()
        self.started = True

    def get_default_rqmt(self, task):
        return LOCAL_DEFAULTS

    def start_task(self, task):
        """
        :param TaskQueueInstance task:
        """
        task_id = task.task_id
        logpath = self.get_logpath(task.logpath, task.task_name, task_id)

        # Start new task
        with open(logpath, 'a') as logfile:
            call = task.call[:]
            sp = subprocess.Popen(call, stdout=logfile, stderr=logfile, start_new_session=True)
            self.running_subprocess.append(sp)
            pid = sp.pid
            process = psutil.Process(pid)
            return process

    def check_finished_tasks(self):
        with self.lock:
            logging.debug('Check for finished subprocesses')
            still_running = []
            # Let jobs started in this process finish
            for p in self.running_subprocess:
                if p.poll() is None:
                    still_running.append(p)
            self.running_subprocess = still_running

            logging.debug('Check for finished tasks')
            with self.running_tasks as running_tasks:
                for process, task in list(running_tasks.values()):
                    logging.debug('Task state: %s %i PID: %s %s' % (task.task_name, task.task_id,
                                                                    process.pid, process.is_running()))
                    if not process.is_running():
                        self.task_done(running_tasks, task)

    def enough_free_resources(self, rqmt):
        cpu = rqmt.get('cpu')
        gpu = rqmt.get('gpu', 0)
        assert(cpu)
        if self.cpu.value > self.max_cpu or self.gpu.value > self.max_gpu:
            logging.warning('Requested resources are higher than available resources\n'
                            'Available resources CPU: %i GPU: %i\n'
                            'Requested resources %s' % (self.max_cpu, self.max_gpu, rqmt))
        if self.cpu.value < cpu:
            return False
        if self.gpu.value < gpu:
            return False
        return True

    def reserve_resources(self, rqmt):
        self.cpu.value -= rqmt.get('cpu')
        self.gpu.value -= rqmt.get('gpu', 0)
        assert 0 <= self.cpu.value <= self.max_cpu
        assert 0 <= self.gpu.value <= self.max_gpu

    def release_resources(self, rqmt):
        self.cpu.value += rqmt.get('cpu')
        self.gpu.value += rqmt.get('gpu', 0)
        assert 0 <= self.cpu.value <= self.max_cpu
        assert 0 <= self.gpu.value <= self.max_gpu

    @tools.default_handle_exception_interrupt_main_thread
    def run(self):
        next_task = None
        try:
            while self.running.value:
                self.check_finished_tasks()

                wait = True  # wait if no new job is started
                # get next task
                logging.debug('Check for new task (Free CPU cores: %i, GPUs: %i)' % (self.cpu.value, self.gpu.value))
                with self.waiting_tasks as waiting_tasks:  # get object for synchronisation
                    if next_task is None and not self.input_queue.empty():
                        next_task = self.input_queue.get()
                        logging.debug('Found new task: %s' % str(next_task))

                    # run next task if the capacities are available
                    if next_task is not None:
                        with self.running_tasks as running_tasks:
                            # if enough free resources => run job
                            if self.enough_free_resources(next_task.rqmt):
                                self.reserve_resources(next_task.rqmt)
                                name = (next_task.name, next_task.task_id)
                                logging.debug('Start task %s' % str(name))
                                try:
                                    del waiting_tasks[name]
                                except KeyError:
                                    logging.warning('Could not delete %s from waiting queue. '
                                                    'This should not happen! Probably a bug...' % str(name))
                                # Start job:
                                process = self.start_task(next_task)
                                running_tasks[name] = (process, next_task)
                                next_task = None
                                wait = False

                if wait:
                    # check only once per second for new jobs
                    # if no job has been started
                    time.sleep(1)
        except KeyboardInterrupt as e:
            #  KeyboardInterrupt is handled in manger
            pass

    def stop_engine(self):
        logging.debug('Got stop signal')
        self.running.value = False
        self.check_finished_tasks()
        with self.running_tasks as running_tasks:
            if len(running_tasks) > 0:
                logging.warning("Still running tasks in local engine: %i" % len(running_tasks))
                for (task_name, task_id), value in running_tasks.items():
                    logging.warning(' Running task: %s %i PID: %s' % (task_name, task_id, value[0].pid))

    def submit_call(self, call, logpath, rqmt, name, task_name, task_ids):
        # run one thread for each task id
        for task_id in task_ids:
            call_with_id = call[:] + [str(task_id)]

            task = TaskQueueInstance(call_with_id, logpath, rqmt, name, task_name, task_id)
            with self.waiting_tasks as waiting_tasks:
                self.input_queue.put(task)
                waiting_tasks[(name, task_id)] = task
        return ENGINE_NAME, socket.gethostname()

    def task_done(self, running_tasks, task):
        name = (task.name, task.task_id)
        logging.debug('Task Done %s' % str(name))
        try:
            del running_tasks[name]
        except KeyError:
            logging.warning(
                'Could not delete %s from waiting queue. This should not happen! Probably a bug...' % str(name))

        # release used resources
        self.release_resources(task.rqmt)

    def task_state(self, task, task_id):
        name = task.task_name()
        task_name = (name, task_id)

        # Check waiting tasks
        with self.waiting_tasks as waiting_tasks:
            if task_name in waiting_tasks:
                return gs.STATE_QUEUE

        # Check running tasks
        with self.running_tasks as running_tasks:
            if task_name in running_tasks:
                return gs.STATE_RUNNING

        if self.try_to_recover_task(task, task_id):
            return gs.STATE_RUNNING
        else:
            return gs.STATE_UNKNOWN

    def reset_cache(self):
        # the local engine needs no cache
        pass

    def try_to_recover_task(self, task, task_id):
        process_logging_filename = task.get_process_logging_path(task_id)
        if not os.path.isfile(process_logging_filename):
            # Nothing to do here
            return False

        # Check if task is already running
        try:
            with open(process_logging_filename) as f:
                d = literal_eval(f.read())
            pid = d['pid']
            process = psutil.Process(pid)

            # Recover instance
            rqmt = d['requested_resources']
            logpath = os.path.relpath(task.path(gs.JOB_LOG_ENGINE))
            call_with_id = gs.SIS_COMMAND + ['worker', os.path.relpath(task.path()), task.name(), str(task_id)]
            name = task.task_name()
            task_name = task.name()
            task_instance = TaskQueueInstance(call_with_id, logpath, rqmt, name, task_name, task_id)

            if call_with_id != process.cmdline()[1:]:
                logging.debug('Job changed, ignore this job: %i %s %s' % (pid, process.cmdline(), task_instance.call))
                return False

            if os.path.abspath(os.getcwd()) != process.cwd():
                logging.debut('Job changed, ignore this job: %i %s %s' % (pid, os.getcwd(), process.cwd()))
                return False

            with self.running_tasks as running_tasks:
                name = (task_instance.name, task_id)
                running_tasks[name] = (process, task_instance)
                self.reserve_resources(rqmt)
            logging.debug('Loaded job: %i %s %s' % (pid, process.cmdline(), task_instance.call))
            return True

        except Exception as e:
            logging.debug('Failed to load running job: %s' % e)
        return False

    def get_task_id(self, task_id, engine_selector):
        if task_id is not None:
            # task id passed via argument
            return task_id
        logging.critical("Job in local engine started without task_id, "
                         "this should not happen! Continue with task_id=1")
        return 1

    @staticmethod
    def get_logpath(logpath_base, task_name, task_id, engine_selector=None):
        """ Returns log file for the currently running task """
        path = os.path.join(logpath_base, gs.ENGINE_LOG)
        path = '%s.%s.%i' % (path, task_name, task_id)
        return path
