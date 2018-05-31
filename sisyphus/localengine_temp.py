import logging
import multiprocessing
import os
import socket
import subprocess
import sys
import psutil
import time
from collections import namedtuple
from ast import literal_eval


from sisyphus.global_settings import STATE_RUNNING, STATE_QUEUE, STATE_UNKNOWN, ENGINE_LOG, PLOGGING_FILE

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
Task = namedtuple('Task', ['call', 'logpath', 'rqmt', 'name', 'task_name', 'task_id'])


def get_logpath(task_path, task_name, task_id):
    path = os.path.join(task_path, ENGINE_LOG)
    path = '%s.%s.%i' % (path, task_name, task_id)
    return path

def get_process_logging_path(task_path, task_name, task_id):
    path = os.path.join(task_path, PLOGGING_FILE)
    path = '%s.%s.%i' % (path, task_name, task_id)
    return path


class Worker(multiprocessing.Process):

    def __init__(self, task, engine):
        self.task = task
        self.engine = engine

        multiprocessing.Process.__init__(self)
        self.start()

    def run(self):
        task_id = self.task.task_id
        logpath = get_logpath(self.task.logpath, self.task.task_name, task_id)
        process_info_path = get_process_logging_path(self.task.logpath, self.task.task_name, task_id)
        try:
            pid = None
            try:
                if process_info_path:
                    with open(process_info_path) as f:
                        d = literal_eval(f.read())
                    pid = d['pid']
            except Exception:
                print('Failed to load running job')
                pass

            if pid is None:
                with open(logpath, 'a') as logfile:
                    call = self.task.call[:]
                    process = subprocess.Popen(call, stdout=logfile, stderr=logfile, start_new_session=True)
                    pid = process.pid

            ps_process = psutil.Process(pid)
            while ps_process.is_running():
                time.sleep(0.1)
        finally:
            self.engine.task_done(self.task)

    def run2(self):
        try:
            task_id = self.task.task_id
            logpath = get_logpath(self.task.logpath, self.task.task_name, task_id)

            with open(logpath, 'a') as logfile:
                call = self.task.call[:]
                process = subprocess.Popen(call, stdout=logfile, stderr=logfile, start_new_session=True)
                pid = process.pid
                psprocess = psutil.Process(process.pid)

                # # TODO add this exception again
                # # except subprocess.CalledProcessError as e:
                # #  logging.error('Local engine failed to start task: %s task id: %s' % (self.task, task_id))
                # try:
                #     process.wait()
                # # Not pretty to have nested exceptions, but it works and is easy to implement and understand
                # except KeyboardInterrupt:
                #     try:
                #         # first interupt, try to let process finish
                #         process.wait()
                #     except KeyboardInterrupt:
                #         try:
                #             # second interupt, try to terminate process
                #             logging.warning('Terminate task: %s %s %s' % (self.task.name, self.task.task_name, task_id))
                #             process.terminate()
                #             process.wait()
                #         except KeyboardInterrupt:
                #             # third interupt, kill process
                #             logging.warning('Kill task: %s %s %s' % (self.task.name, self.task.task_name, task_id))
                #             process.kill()
                #             process.wait()  # this should return immediately now
        finally:
            self.engine.task_done(self.task)


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
        self.queue = multiprocessing.Queue(1)
        self.obj = obj
        self.queue.put(obj)

    def __enter__(self):
        self.obj = self.queue.get()
        return self.obj

    def __exit__(self, type, value, traceback):
        self.queue.put(self.obj)


class LocalEngine(multiprocessing.Process):

    """ Simple engine to execute running tasks locally,
        the only checked resource is cpus so far
    """

    def __init__(self, max_cpu=1, start_engine=True):
        # resources
        self.max_cpu = multiprocessing.Value('i', max_cpu)
        self.cpu = multiprocessing.Value('i', max_cpu)

        if start_engine:
            # control input
            self.input_queue = multiprocessing.Queue()
            self.waiting_tasks = sync_object({})

            # control output / which tasks are currently runnning
            self.running_tasks = sync_object({})

            self.running = multiprocessing.Value('B', 1)  # set to 0 to stop engine
            multiprocessing.Process.__init__(self)
            self.start()

    def add_defaults_to_rqmt(self, rqmt):
        s = LOCAL_DEFAULTS.copy()
        s.update(rqmt)
        return s

    def start_task(self, task):
        task_id = task.task_id
        logpath = get_logpath(task.logpath, task.task_name, task_id)
        process_info_path = get_process_logging_path(task.logpath, task.task_name, task_id)

        pid = None
        process = None
        # Check if task is already running
        try:
            if process_info_path:
                with open(process_info_path) as f:
                    d = literal_eval(f.read())
                pid = d['pid']
                process = psutil.Process(pid)
                print('Loaded job: %i %s %s' % (pid, process.cmdline(), task.call))

        except Exception:
            print('Failed to load running job')
            pass

        # Start new task
        if pid is None:
            with open(logpath, 'a') as logfile:
                call = task.call[:]
                process = subprocess.Popen(call, stdout=logfile, stderr=logfile, start_new_session=True)
                pid = process.pid
        process = psutil.Process(pid)
        print(process.cmdline())
        return process

    def run(self):
        next_task = None
        try:
            while self.running.value:
                logging.debug('Check for finished tasks')
                with self.running_tasks as running_tasks:
                    for process, task in list(running_tasks.values()):
                        if not process.is_running():
                            self.task_done(running_tasks, task)

                wait = True  # wait if no new job is started
                # get next task
                logging.debug('Check for new task')
                with self.waiting_tasks as waiting_tasks:  # get object for synchronisation
                    if next_task is None and not self.input_queue.empty():
                        next_task = self.input_queue.get()
                        logging.debug('Found new task: %s' % str(next_task))

                    # run next task if the capacities are available
                    if next_task is not None:
                        cpu = next_task.rqmt.get('cpu')
                        assert(cpu)
                        with self.running_tasks as running_tasks:
                            # if enough free cpu cores => run job
                            if self.cpu.value >= cpu or self.cpu.value == self.max_cpu.value:
                                self.cpu.value -= cpu  # <0 if cpu requerments > local max_cpu
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
        except KeyboardInterrupt:
            sys.exit(1)
            try:
                # try to exit normal
                logging.warning('Got user interrupt  try to exit nicely')
                self.stop()
            except KeyboardInterrupt:
                try:
                    logging.warning('Got user interrupt a second time try to exit harder')
                    # try again...
                    self.stop()
                except KeyboardInterrupt:
                    # kill the system
                    logging.warning('Got user interrupt again exit immediately')
                    sys.exit(1)

        # TODO Update
        #for task in join_list:
        #    task.join()

    def stop(self):
        logging.debug('Got stop signal')
        self.running.value = False
        waited_for_tasks = False
        while True:
            with self.running_tasks as running_tasks:
                if len(running_tasks) == 0:
                    break
                else:
                    waited_for_tasks = True
                    logging.warning("Wait for running jobs in local engine: %i" % len(running_tasks))
                    for task in running_tasks:
                        logging.warning(' Running task: %s %s' % (task))
                    time.sleep(1)
        if waited_for_tasks:
            logging.warning('All running jobs finished')

    def submit(self, call, logpath, rqmt, name, task_name, task_ids):
        # run one thread for each task
        for task_id in task_ids:
            call_with_id = call[:] + [str(task_id)]

            task = Task(call_with_id, logpath, rqmt, name, task_name, task_id)
            with self.waiting_tasks as waiting_tasks:
                self.input_queue.put(task)
                waiting_tasks[(name, task_id)] = task
        return (ENGINE_NAME, socket.gethostname())

    def task_done(self, running_tasks, task):
        #with self.running_tasks as running_tasks:
            name = (task.name, task.task_id)
            logging.debug('Task Done %s' % str(name))
            try:
                del running_tasks[name]
            except KeyError:
                logging.warning(
                    'Could not delete %s from waiting queue. This should not happen! Probably a bug...' % str(name))

            # release used resources
            cpu = task.rqmt.get('cpu', 1)
            self.cpu.value += cpu

    def task_state(self, name, task_id):
        task_name = (name, task_id)

        # Check waiting tasks
        with self.waiting_tasks as waiting_tasks:
            if task_name in waiting_tasks:
                return STATE_QUEUE

        # Check running tasks
        with self.running_tasks as running_tasks:
            if task_name in running_tasks:
                return STATE_RUNNING

        return STATE_UNKNOWN

    def reset_cache(self):
        # the local engine has no cache
        pass
