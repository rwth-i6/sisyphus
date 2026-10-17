import os
import subprocess
import tempfile
import time
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


class ArrayTestJob(Job):
    def __init__(self, test_name, num_tasks):
        self.test_name = test_name
        self.num_tasks = num_tasks

    def run(self, i):
        pass

    def tasks(self):
        yield Task("run", args=range(self.num_tasks))


class FixedStateEngine:
    def __init__(self, states):
        self.states = states

    def task_state(self, task, task_id):
        return self.states[task_id]

    def reset_cache(self):
        pass

    def get_submit_history(self, task):
        return {task_id: [{}] for task_id in task.task_ids()}


class TaskStateTest(unittest.TestCase):
    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory()
        self._exec_in_dir = execute_in_dir(self._tmp_dir.name)
        self._exec_in_dir.__enter__()
        self._base_dir = gs.BASE_DIR
        gs.BASE_DIR = self._tmp_dir.name

    def tearDown(self):
        gs.BASE_DIR = self._base_dir
        self._exec_in_dir.__exit__(None, None, None)
        self._tmp_dir.cleanup()

    def _setup_task(self, num_tasks):
        job = ArrayTestJob(test_name=self.id(), num_tasks=num_tasks)
        job._sis_setup_directory()
        (task,) = job._sis_tasks()
        return task

    @staticmethod
    def _write(path, age):
        with open(path, "w"):
            pass
        t = time.time() - age
        os.utime(path, (t, t))

    def _write_stale_logs(self, task, task_id):
        self._write(task.path(gs.JOB_LOG, task_id), age=1000)
        self._write(task.get_process_logging_path(task_id), age=1000)

    def _write_finished_marker(self, task, task_id, age):
        self._write(task.path(gs.STATE_FINISHED, task_id), age=age)

    def test_unknown_with_young_finished_marker_is_running(self):
        task = self._setup_task(num_tasks=1)
        self._write_stale_logs(task, 1)
        self._write_finished_marker(task, 1, age=1)
        engine = FixedStateEngine({1: gs.STATE_UNKNOWN})
        self.assertEqual(task.state(engine, 1), gs.STATE_RUNNING)

    def test_unknown_with_old_finished_marker_is_finished(self):
        task = self._setup_task(num_tasks=1)
        self._write_stale_logs(task, 1)
        self._write_finished_marker(task, 1, age=1000)
        engine = FixedStateEngine({1: gs.STATE_UNKNOWN})
        self.assertEqual(task.state(engine, 1), gs.STATE_FINISHED)

    def test_unknown_without_finished_marker_is_interrupted(self):
        task = self._setup_task(num_tasks=1)
        self._write_stale_logs(task, 1)
        engine = FixedStateEngine({1: gs.STATE_UNKNOWN})
        self.assertEqual(task.state(engine, 1), gs.STATE_INTERRUPTED_NOT_RESUMABLE)

    def test_array_with_young_finished_marker_is_running(self):
        task = self._setup_task(num_tasks=2)
        self._write_stale_logs(task, 1)
        self._write_finished_marker(task, 1, age=1)
        engine = FixedStateEngine({1: gs.STATE_UNKNOWN, 2: gs.STATE_RUNNING})
        self.assertEqual(task.state(engine), gs.STATE_RUNNING)


if __name__ == "__main__":
    unittest.main()
