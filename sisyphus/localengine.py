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
ENGINE_NAME = "local"
LOCAL_DEFAULTS = {
    "cpu": 1,
    "mem": 1,
    "time": 1,
}


def run_task(call, logpath):
    """Simple function to run task"""
    with open(logpath, "a") as logfile:
        subprocess.check_call(call, stdout=logfile, stderr=logfile)


# name is the unique name combined of job id and task name, task_name is only the task name
TaskQueueInstance = namedtuple("TaskQueueInstance", ["call", "logpath", "rqmt", "name", "task_name", "task_id"])


def get_process_logging_path(task_path, task_name, task_id):
    path = os.path.join(task_path, gs.PLOGGING_FILE)
    path = "%s.%s.%i" % (path, task_name, task_id)
    return path


class sync_object(object):
    """Object to be used by the with statement to sync an object via queue
    e.g.::

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
    """Simple engine to execute running tasks locally.
    CPU and GPU are always checked, all other requirements only if given during initialisation.
    """

    def __init__(self, cpus=1, gpus=0, available_gpus="", **kwargs):
        """The parameter cpus and gpus are kept for backwards compatibility, if cpu and gpu are given
        they will overwrite the values of cpus and gpus.

        :param int cpus: number of CPUs that can be used
        :param int gpus: number of GPUs that can be used
        :param **kwargs: other consumable resources e.g. mem (in GB)
        """
        # resources
        self.lock = threading.Lock()
        # There is a mismatch between the requested resources names (?pus) and internal name (?pu)
        # Keep old naming for backwards compatibility, but internal name will overwrite default values
        self.max_resources = {"cpu": cpus, "gpu": gpus}
        self.max_resources.update(kwargs)
        self.free_resources = self.max_resources.copy()
        assert gpus == 0 or len(available_gpus.split(",")) == gpus
        self.available_gpus = {g: True for g in available_gpus.split(",") if g != ""}

        self.running_subprocess = []
        self.started = False
        self.running = multiprocessing.Value("B", 1)  # set to 0 to stop engine

        threading.Thread.__init__(self)

    def start_engine(self):
        if self.started:
            return
        # control input
        self.runnable_tasks = sync_object([])
        self.waiting_tasks = sync_object({})

        # control output / which tasks are currently running
        self.running_tasks = sync_object({})

        self.start()
        self.started = True

    def get_default_rqmt(self, task):
        return LOCAL_DEFAULTS

    def start_task(self, task, selected_gpus):
        """
        :param TaskQueueInstance task:
        :param str selected_gpus:
        :rtype: psutil.Process
        """
        # Start new task
        call = task.call[:]
        env = os.environ.copy()
        env["CUDA_VISIBLE_DEVICES"] = selected_gpus
        sp = subprocess.Popen(call, env=env, start_new_session=True)
        self.running_subprocess.append(sp)
        pid = sp.pid
        process = psutil.Process(pid)
        return process

    def check_finished_tasks(self):
        with self.lock:
            logging.debug("Check for finished subprocesses")
            still_running = []
            # Let jobs started in this process finish
            for p in self.running_subprocess:
                if p.poll() is None:
                    still_running.append(p)
            self.running_subprocess = still_running

            logging.debug("Check for finished tasks")
            with self.running_tasks as running_tasks:
                for process, task, _ in list(running_tasks.values()):
                    logging.debug(
                        "Task state: %s %i PID: %s %s"
                        % (task.task_name, task.task_id, process.pid, process.is_running())
                    )
                    if not process.is_running():
                        self.task_done(running_tasks, task)

    def enough_free_resources(self, rqmt):
        for key, max_available in self.max_resources.items():
            free = self.free_resources[key]
            requested = rqmt.get(key, 0)
            if max_available < requested:
                logging.warning(
                    "Requested resources are higher than maximal available resources\n"
                    "Available resources %s\n"
                    "Requested resources %s" % (self.max_resources, rqmt)
                )
            if free < requested:
                return False
        return True

    def reserve_resources(self, rqmt, selected_devices=None):
        self.free_resources = {key: free - rqmt.get(key, 0) for key, free in self.free_resources.items()}
        for key, max_available in self.max_resources.items():
            free = self.free_resources[key]
            assert 0 <= free <= max_available

        # reserve specific GPUs
        if selected_devices is not None:
            if selected_devices != "":
                for name in selected_devices.split(","):
                    self.available_gpus[name] = False
        else:
            selected_devices = []
            for name, free in self.available_gpus.items():
                if len(selected_devices) == rqmt.get("gpu", 0):
                    break
                if free:
                    self.available_gpus[name] = False
                    selected_devices.append(name)
            assert len(selected_devices) == rqmt.get("gpu", 0)
        return ",".join(selected_devices)

    def release_resources(self, rqmt, selected_devices):
        self.free_resources = {key: free + rqmt.get(key, 0) for key, free in self.free_resources.items()}
        for key, max_available in self.max_resources.items():
            free = self.free_resources[key]
            assert 0 <= free <= max_available
        if selected_devices != "":
            for name in selected_devices.split(","):
                self.available_gpus[name] = True

    @tools.default_handle_exception_interrupt_main_thread
    def run(self):
        try:
            while self.running.value:
                self.check_finished_tasks()

                wait = True  # wait if no new job is started
                # check runnable tasks
                logging.debug("Check for new tasks (Free resources %s)" % self.free_resources)
                # get object for synchronisation
                with self.waiting_tasks as waiting_tasks, self.runnable_tasks as runnable_tasks:
                    runnable_task_idx = 0

                    # run next task if the capacities are available
                    while runnable_task_idx < len(runnable_tasks):
                        next_task = runnable_tasks[runnable_task_idx]
                        with self.running_tasks as running_tasks:
                            # if enough free resources => run job
                            if self.enough_free_resources(next_task.rqmt):
                                selected_gpus = self.reserve_resources(next_task.rqmt)
                                name = (next_task.name, next_task.task_id)
                                logging.debug("Start task %s" % str(name))
                                try:
                                    del waiting_tasks[name]
                                except KeyError:
                                    logging.warning(
                                        "Could not delete %s from waiting queue. "
                                        "This should not happen! Probably a bug..." % str(name)
                                    )
                                # Start job:
                                process = self.start_task(next_task, selected_gpus)
                                running_tasks[name] = (process, next_task, selected_gpus)
                                del runnable_tasks[runnable_task_idx]
                                wait = False
                            else:
                                runnable_task_idx += 1

                if wait:
                    # check only once per second for new jobs
                    # if no job has been started
                    time.sleep(1)
        except KeyboardInterrupt:
            #  KeyboardInterrupt is handled in manager
            pass

    def stop_engine(self):
        logging.debug("Got stop signal")
        self.running.value = False
        self.check_finished_tasks()
        with self.running_tasks as running_tasks:
            if len(running_tasks) > 0:
                logging.warning("Still running tasks in local engine: %i" % len(running_tasks))
                for (task_name, task_id), value in running_tasks.items():
                    logging.warning(" Running task: %s %i PID: %s" % (task_name, task_id, value[0].pid))

    def submit_call(self, call, logpath, rqmt, name, task_name, task_ids):
        if rqmt.get("multi_node_slots", None):
            raise NotImplementedError("Multi-node slots are not implemented for local engine")
        # run one thread for each task id
        for task_id in task_ids:
            call_with_id = call[:] + [str(task_id)]
            call_with_id += ["--redirect_output"]

            task = TaskQueueInstance(call_with_id, logpath, rqmt, name, task_name, task_id)
            with self.waiting_tasks as waiting_tasks, self.runnable_tasks as runnable_tasks:
                runnable_tasks.append(task)
                waiting_tasks[(name, task_id)] = task
        return ENGINE_NAME, socket.gethostname()

    def task_done(self, running_tasks, task):
        name = (task.name, task.task_id)
        selected_gpus = running_tasks[name][2]
        logging.debug("Task Done %s" % str(name))
        try:
            del running_tasks[name]
        except KeyError:
            logging.warning(
                "Could not delete %s from waiting queue. This should not happen! Probably a bug..." % str(name)
            )

        # release used resources
        self.release_resources(task.rqmt, selected_gpus)

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
            pid = d["pid"]
            process = psutil.Process(pid)

            # Recover instance
            rqmt = d["requested_resources"]
            logpath = os.path.relpath(task.path(gs.JOB_LOG_ENGINE))
            call_with_id = task.get_worker_call(task_id)
            name = task.task_name()
            task_name = task.name()
            task_instance = TaskQueueInstance(call_with_id, logpath, rqmt, name, task_name, task_id)

            if call_with_id[1:] != process.cmdline()[1:]:
                logging.warning("Job %s changed, recovering it anyway." % name)
                logging.debug("Job changed: %i %s %s" % (pid, process.cmdline(), task_instance.call))

            with self.running_tasks as running_tasks:
                name = (task_instance.name, task_id)
                used_gpus = process.environ().get("CUDA_VISIBLE_DEVICES", "")
                running_tasks[name] = (process, task_instance, used_gpus)
                self.reserve_resources(rqmt, selected_devices=used_gpus)
            logging.debug("Loaded job: %i %s %s" % (pid, process.cmdline(), task_instance.call))
            return True

        except Exception as e:
            logging.debug("Failed to load running job: %s" % e)
        return False

    def get_task_id(self, task_id):
        if task_id is not None:
            # task id passed via argument
            return task_id
        logging.warning(
            "Job in local engine started without task_id, "
            "worker is probably started manualy. Continue with task_id=1"
        )
        return 1
