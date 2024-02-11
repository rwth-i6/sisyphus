import gzip
import logging
import os
import pickle
import pprint
import psutil
import pwd
import socket
import sys
import shutil
import subprocess
import time
from threading import Thread, Condition

import sisyphus.global_settings as gs
import sisyphus.job_path
import sisyphus.toolkit


def format_time(t):
    minutes, seconds = divmod(int(t), 60)
    hours, minutes = divmod(minutes, 60)
    return "%d:%02d:%02d" % (hours, minutes, seconds)


def format_bytes(b):
    return format_number(
        b, factor=1024, mapping=["B", "kB", "MB", "GB", "TB", "PB", "EB", "ZB" "YB"], use_decimal_after=3
    )


def format_number(number, factor=1000, mapping=[""] + ["e+%02i" % i for i in range(3, 31, 3)], use_decimal_after=1):
    if use_decimal_after is None:
        use_decimal_after = len(mapping)
    residual = 0
    count = 0
    result = number
    while result >= factor and count < len(mapping) - 1:
        result, residual = divmod(result, factor)
        count += 1
    if count < use_decimal_after:
        return "%i%s" % (result, mapping[count])
    else:
        return "%.2f%s" % ((result * factor + residual) / factor, mapping[count])


class LoggingThread(Thread):
    """Thread to log memory and time consumption of job"""

    def __init__(self, job, task, task_id):
        """
        :param sisyphus.job.Job job:
        :param sisyphus.task.Task task:
        :param int task_id:
        """
        self.job = job
        self.task = task
        self.task_id = task_id
        self.start_time = None
        super().__init__(daemon=True)
        self.out_of_memory = False
        self._cond = Condition()
        self.__stop = False
        self.rqmt = gs.active_engine.get_rqmt(task, task_id, update=False)

    def run(self):
        start_time = time.time()
        last_rss = None
        current_process = psutil.Process(os.getpid())
        max_resources = {}

        usage_file_path = self.task.get_process_logging_path(self.task_id)
        user = os.geteuid()
        try:
            user = (pwd.getpwuid(user).pw_name,)
        except KeyError:
            pass

        def log_usage(current):
            with open(usage_file_path, "w") as usage_file:
                usage = {
                    "max": max_resources,
                    "current": current,
                    "pid": os.getpid(),
                    "user": user,
                    "used_time": (time.time() - start_time) / 3600.0,
                    "host": socket.gethostname(),
                    "current_time": time.ctime(),
                    "out_of_memory": self.out_of_memory,
                    "requested_resources": self.rqmt,
                }
                usage_file.write("%s\n" % pprint.pformat(usage))

        last_log_value = 0
        last_log_time = 0
        max_resources = resources = gs.active_engine.get_job_used_resources(current_process)
        while not self.__stop:
            try:
                resources = gs.active_engine.get_job_used_resources(current_process)
            except psutil.AccessDenied as e:
                logging.warning(
                    "Logging thread got psutil.AccessDenied Exception, subprocess probably ended %s" % str(e)
                )
                continue
            # Only print log if rss changed at least bey PLOGGING_MIN_CHANGE
            if last_rss is None or abs(last_rss - resources["rss"]) / last_rss > gs.PLOGGING_MIN_CHANGE:
                if not gs.PLOGGING_QUIET:
                    logging.info(
                        "Run time: {time} CPU: {cpu:.2f}% RSS: {rss} VMS: {vms}".format(
                            time=format_time(time.time() - start_time),
                            cpu=resources["cpu"],
                            rss=format_bytes(resources["rss"] * 1024**3),
                            vms=format_bytes(resources["vms"] * 1024**3),
                        )
                    )
                last_rss = resources["rss"]

            # store max used resources
            for k, v in resources.items():
                c = max_resources.get(k)
                if c is None or c < v:
                    max_resources[k] = v

            # update log file:
            # at least every PLOGGING_UPDATE_FILE_PERIOD seconds or
            # if rss usage grow relative more then PLOGGING_MIN_CHANGE
            # if (max_resources['rss'] > last_log_value and time.time() - last_log_time > 30) or \
            if (
                time.time() - last_log_time > gs.PLOGGING_UPDATE_FILE_PERIOD
                or (max_resources["rss"] - last_log_value) / last_log_value > gs.PLOGGING_MIN_CHANGE
            ):
                log_usage(resources)
                last_log_value = max_resources["rss"]
                last_log_time = time.time()

            with self._cond:
                self._cond.wait(gs.PLOGGING_INTERVAL)

            # if less then 2% or less then 256MB are free
            # if max_mem * 0.98 < last_rss:
            #     if max_mem and (max_mem - last_rss) / max_mem < 0.02 or max_mem - last_rss < 2**28:
            #     self.task.check_state(gs.JOB_CLOSE_TO_MAX_MEM, task_id=self.task_id, update=True)

        log_usage(resources)
        logging.info(
            "Max resources: Run time: {time} CPU: {cpu}% RSS: {rss} VMS: {vms}"
            "".format(
                time=format_time(time.time() - start_time),
                cpu=max_resources["cpu"],
                rss=format_bytes(max_resources["rss"] * 1024**3),
                vms=format_bytes(max_resources["vms"] * 1024**3),
            )
        )

    def stop(self):
        with self._cond:
            self.__stop = True
            self._cond.notify_all()
        self.join()


def worker(args):
    # Change job into error state in case of any exception
    gs.active_engine = gs.engine().get_used_engine(args.engine)
    sisyphus.toolkit._sis_running_in_worker = True

    try:
        worker_helper(args)
    except Exception:
        task_id = gs.active_engine.get_task_id(args.task_id)
        error_file = "%s.%s.%i" % (args.jobdir + os.path.sep + gs.STATE_ERROR, args.task_name, task_id)
        log_file = "%s.%s.%i" % (args.jobdir + os.path.sep + gs.JOB_LOG, args.task_name, task_id)
        if not os.path.isfile(error_file) and not os.path.isdir(error_file):
            # create error file
            try:
                os.link(log_file, error_file)
            except OSError:
                if not os.path.isfile(error_file):
                    with open(error_file, "w"):
                        pass
        raise
    finally:
        sisyphus.toolkit._sis_running_in_worker = False


def worker_helper(args):
    """This program is run on the client side when running the job"""

    # Redirect stdout and stderr by starting a subprocess
    # Starting a subprocess was the only reliable way I found to redirect all outputs to a file,
    # including the output of other subprocesses. Let me know if you have a nicer solution to this problem.
    if args.redirect_output:
        task_id = gs.active_engine.get_task_id(args.task_id)
        log_file = "%s.%s.%i" % (args.jobdir + os.path.sep + gs.JOB_LOG, args.task_name, task_id)

        # Make sure Sisyphus is called again with the same python executable.
        # Adding the executable in front of the call could cause problems with the worker_wrapper
        if shutil.which(os.path.basename(sys.executable)) != sys.executable:
            os.environ["PATH"] = os.path.dirname(sys.executable) + ":" + os.environ["PATH"]

        call = sys.argv
        del call[call.index("--redirect_output")]

        with open(log_file, "a") as logfile:
            if logfile.tell() > 0:
                logfile.write("\n" + ("#" * 80) + "\nRETRY OR CONTINUE TASK\n" + ("#" * 80) + "\n\n")
            subprocess.check_call(call, stdout=logfile, stderr=logfile)
        return

    with gzip.open(os.path.join(args.jobdir, gs.JOB_SAVE)) as f:
        job = pickle.load(f)

    if not job._sis_runnable():
        for path in job._sis_inputs:
            if path.available():
                logging.info("Path available:     %s" % path)
            else:
                logging.error("Path not available: %s" % path)
        assert False, "Job isn't runnable, probably some inputs are not ready"

    # Make sure that the own job outputs are not cached
    for name, path in job._sis_outputs.items():
        path.cached = False

    # find task
    task = None
    for task_check in job._sis_tasks():
        if task_check.name() == args.task_name:
            task = task_check
            break

    assert task is not None, [t.name() for t in job._sis_tasks()]  # No task with that name found!

    # Work around for bug, the wrong job can be linked to the task
    # for some reason, this sets it back. TODO: find the real problem
    task._job = job

    task_id = gs.active_engine.get_task_id(args.task_id)
    logging.debug("Task id: %s" % str(task_id))
    logging_thread = LoggingThread(job, task, task_id)
    logging_thread.start()

    sisyphus.job_path.Path.cacheing_enabled = True
    resume_job = False
    gs.active_engine.init_worker(task)

    # cleanup environment
    if getattr(task._job, "_sis_environment", None):
        task._job._sis_environment.modify_environment()

    try:
        # run task
        task.run(task_id, resume_job, logging_thread=logging_thread)
    finally:
        logging_thread.stop()
