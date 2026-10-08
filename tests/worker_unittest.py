import argparse
import os
import sys
import tempfile
import unittest
from unittest import mock

import sisyphus.global_settings as gs
from sisyphus import Job, Task
from sisyphus.localengine import LocalEngine
from sisyphus.tools import execute_in_dir
from sisyphus import worker


def _record_call(name):
    with open("calls", "a") as f:
        f.write(name + "\n")


class ResumeTestJob(Job):
    def __init__(self, test_name, resumable=True):
        self.test_name = test_name
        self.resumable = resumable

    def run(self):
        _record_call("run")

    def resume_run(self):
        _record_call("resume_run")

    def tasks(self):
        yield Task("run", resume="resume_run" if self.resumable else None)


class WorkerResumeTest(unittest.TestCase):
    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory()
        self._exec_in_dir = execute_in_dir(self._tmp_dir.name)
        self._exec_in_dir.__enter__()
        self._base_dir = gs.BASE_DIR
        gs.BASE_DIR = self._tmp_dir.name
        self._active_engine = gs.active_engine
        gs.active_engine = LocalEngine()

    def tearDown(self):
        gs.active_engine = self._active_engine
        gs.BASE_DIR = self._base_dir
        self._exec_in_dir.__exit__(None, None, None)
        self._tmp_dir.cleanup()

    def _setup_job(self, *, resumable=True, started=False):
        job = ResumeTestJob(test_name=self.id(), resumable=resumable)
        job._sis_setup_directory()
        if started:
            with open(job._sis_path(gs.JOB_LOG + ".run", 1), "w") as f:
                f.write("log of the previous attempt\n")
        return job

    @staticmethod
    def _run_worker(job, **kwargs):
        args = dict(jobdir=job._sis_path(), task_name="run", task_id=1, engine="short", redirect_output=False)
        args["resume_job"] = None
        args.update(kwargs)
        worker.worker_helper(argparse.Namespace(**args))

    @staticmethod
    def _calls(job):
        calls_file = os.path.join(job._sis_path(gs.JOB_WORK_DIR), "calls")
        if not os.path.isfile(calls_file):
            return []
        with open(calls_file) as f:
            return f.read().split()

    @staticmethod
    def _error(job):
        return os.path.isfile(job._sis_path(gs.STATE_ERROR + ".run", 1))

    def test_first_attempt_runs_start(self):
        job = self._setup_job()
        self._run_worker(job)
        self.assertEqual(self._calls(job), ["run"])
        self.assertFalse(self._error(job))

    def test_continued_attempt_runs_resume(self):
        job = self._setup_job(started=True)
        self._run_worker(job)
        self.assertEqual(self._calls(job), ["resume_run"])
        self.assertFalse(self._error(job))

    def test_force_resume_runs_start(self):
        job = self._setup_job(started=True)
        self._run_worker(job, resume_job="no")
        self.assertEqual(self._calls(job), ["run"])

    def test_continued_attempt_not_resumable_is_interrupted_not_resumable(self):
        job = self._setup_job(resumable=False, started=True)
        self._run_worker(job)
        self.assertEqual(self._calls(job), [])
        self.assertFalse(self._error(job))
        (task,) = job._sis_tasks()
        # the usage file of the worker is not recent anymore, the job is not known to the engine anymore
        with mock.patch.object(gs, "WAIT_PERIOD_JOB_FS_SYNC", 0), mock.patch.object(
            gs, "PLOGGING_UPDATE_FILE_PERIOD", 0
        ), mock.patch.object(gs, "WAIT_PERIOD_JOB_CLEANUP", 0):
            self.assertEqual(task._get_state_helper(None, 1), gs.STATE_INTERRUPTED_NOT_RESUMABLE)

    def test_first_attempt_not_resumable_runs_start(self):
        job = self._setup_job(resumable=False)
        self._run_worker(job)
        self.assertEqual(self._calls(job), ["run"])
        self.assertFalse(self._error(job))

    def _redirect_call(self, job, **kwargs):
        argv = ["sis", gs.CMD_WORKER, job._sis_path(), "run", "1", "--redirect_output"]
        with mock.patch.object(sys, "argv", argv), mock.patch.dict(os.environ), mock.patch.object(
            worker.subprocess, "check_call"
        ) as check_call:
            self._run_worker(job, redirect_output=True, **kwargs)
        (call,), _ = check_call.call_args
        return call

    def test_redirect_output_passes_resume_job(self):
        job = self._setup_job()
        self.assertEqual(self._redirect_call(job)[-2:], ["--resume_job", "no"])
        # the outer worker created the log file, so the next attempt is a continuation
        self.assertEqual(self._redirect_call(job)[-2:], ["--resume_job", "yes"])

    def test_redirect_output_keeps_given_resume_job(self):
        job = self._setup_job(started=True)
        call = self._redirect_call(job, resume_job="no")
        self.assertNotIn("--resume_job", call)


if __name__ == "__main__":
    unittest.main()
