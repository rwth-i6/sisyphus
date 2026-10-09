import os
import subprocess
import tempfile
import unittest
from unittest import mock

import sisyphus.global_settings as gs
from sisyphus import Job, Task
from sisyphus.localengine import LocalEngine
from sisyphus.tools import execute_in_dir


class FailingTestJob(Job):
    def __init__(self, test_name, failure):
        self.test_name = test_name
        self.failure = failure

    def run(self):
        if self.failure == "command":
            subprocess.check_call(["false"])
        raise Exception("job failed")

    def tasks(self):
        yield Task("run")


class PreemptingLocalEngine(LocalEngine):
    def __init__(self, preempted):
        super().__init__()
        self.preempted = preempted

    def is_job_being_preempted(self):
        return self.preempted


class TaskErrorTest(unittest.TestCase):
    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory()
        self._exec_in_dir = execute_in_dir(self._tmp_dir.name)
        self._exec_in_dir.__enter__()
        self._base_dir = gs.BASE_DIR
        gs.BASE_DIR = self._tmp_dir.name
        self._active_engine = gs.active_engine

    def tearDown(self):
        gs.active_engine = self._active_engine
        gs.BASE_DIR = self._base_dir
        self._exec_in_dir.__exit__(None, None, None)
        self._tmp_dir.cleanup()

    def _run_failing_task(self, *, failure, preempted):
        gs.active_engine = PreemptingLocalEngine(preempted=preempted)
        job = FailingTestJob(test_name=self.id(), failure=failure)
        job._sis_setup_directory()
        (task,) = job._sis_tasks()
        task.run(1, logging_thread=mock.Mock())
        return os.path.isfile(job._sis_path(gs.STATE_ERROR + ".run", 1))

    def test_failed_command_is_error(self):
        self.assertTrue(self._run_failing_task(failure="command", preempted=False))

    def test_exception_is_error(self):
        self.assertTrue(self._run_failing_task(failure="exception", preempted=False))

    def test_failed_command_while_preempted_is_no_error(self):
        self.assertFalse(self._run_failing_task(failure="command", preempted=True))

    def test_exception_while_preempted_is_no_error(self):
        self.assertFalse(self._run_failing_task(failure="exception", preempted=True))


if __name__ == "__main__":
    unittest.main()
