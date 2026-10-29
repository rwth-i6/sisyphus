import logging
import os
import subprocess
import tempfile
import time
import unittest
from unittest import mock

import sisyphus.global_settings as gs
import sisyphus.task
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

    def test_unknown_with_young_finished_marker_is_finished(self):
        task = self._setup_task(num_tasks=1)
        self._write_stale_logs(task, 1)
        self._write_finished_marker(task, 1, age=1)
        engine = FixedStateEngine({1: gs.STATE_UNKNOWN})
        self.assertTrue(task.finished(1))
        self.assertEqual(task.state(engine, 1), gs.STATE_FINISHED)

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

    @staticmethod
    def _write_heartbeat(task, task_id, mtime):
        for path in (task.path(gs.JOB_LOG, task_id), task.get_process_logging_path(task_id)):
            with open(path, "w"):
                pass
            os.utime(path, (mtime, mtime))

    @staticmethod
    def _heartbeat_max_age():
        return gs.WAIT_PERIOD_JOB_FS_SYNC + gs.PLOGGING_UPDATE_FILE_PERIOD + gs.WAIT_PERIOD_JOB_CLEANUP

    @staticmethod
    def _num_heartbeat_stats(task, task_id, stat_mock):
        paths = {task.get_process_logging_path(task_id), task.path(gs.JOB_LOG, task_id)}
        return sum(1 for call in stat_mock.call_args_list if call.args[0] in paths)

    def _scan_warnings(self, scan):
        with self.assertLogs(level="WARNING") as cm:
            scan()
            logging.warning("end of scan")
        return [r.getMessage() for r in cm.records][:-1]

    def test_running_heartbeat_check_throttled(self):
        task = self._setup_task(num_tasks=1)
        engine = FixedStateEngine({1: gs.STATE_RUNNING})
        t0 = time.time()
        self._write_heartbeat(task, 1, mtime=t0)
        with mock.patch.object(sisyphus.task, "time") as clock, mock.patch("os.stat", wraps=os.stat) as stat_mock:
            clock.time.return_value = t0
            self.assertEqual(task.state(engine, 1, force=True), gs.STATE_RUNNING)
            self.assertEqual(self._num_heartbeat_stats(task, 1, stat_mock), 1)

            stat_mock.reset_mock()
            clock.time.return_value = t0 + 10
            self.assertEqual(task.state(engine, 1, force=True), gs.STATE_RUNNING)
            self.assertEqual(self._num_heartbeat_stats(task, 1, stat_mock), 0)

            stat_mock.reset_mock()
            clock.time.return_value = t0 + self._heartbeat_max_age() + 1
            self.assertEqual(task.state(engine, 1, force=True), gs.STATE_RUNNING)
            self.assertEqual(self._num_heartbeat_stats(task, 1, stat_mock), 2)

    def test_running_heartbeat_check_not_blocking_scan(self):
        task = self._setup_task(num_tasks=1)
        engine = FixedStateEngine({1: gs.STATE_RUNNING})
        t0 = time.time()
        self._write_heartbeat(task, 1, mtime=t0)
        heartbeat_paths = {task.get_process_logging_path(1), task.path(gs.JOB_LOG, 1)}
        real_stat = os.stat

        def _blocked_stat(path, *args, **kwargs):
            assert path not in heartbeat_paths, "heartbeat stat would block the scan"
            return real_stat(path, *args, **kwargs)

        with mock.patch.object(sisyphus.task, "time") as clock:
            clock.time.return_value = t0
            self.assertEqual(task.state(engine, 1, force=True), gs.STATE_RUNNING)
            clock.time.return_value = t0 + 10
            with mock.patch("os.stat", _blocked_stat):
                self.assertEqual(task.state(engine, 1, force=True), gs.STATE_RUNNING)

    def test_running_stale_heartbeat_warns_once_per_task(self):
        task = self._setup_task(num_tasks=3)
        engine = FixedStateEngine({1: gs.STATE_RUNNING, 2: gs.STATE_RUNNING, 3: gs.STATE_RUNNING})
        t = time.time()
        for task_id in (1, 2):
            self._write_heartbeat(task, task_id, mtime=t - 1000)
        max_age = self._heartbeat_max_age()
        with mock.patch.object(sisyphus.task, "time") as clock:

            def _scan(*, recent_task_ids):
                for task_id in recent_task_ids:
                    self._write_heartbeat(task, task_id, mtime=t)
                clock.time.return_value = t
                return self._scan_warnings(lambda: self.assertEqual(task.state(engine, force=True), gs.STATE_RUNNING))

            warnings = _scan(recent_task_ids=[3])
            self.assertEqual(len(warnings), 2)
            self.assertIn("Task: run 1,", warnings[0])
            self.assertIn("Task: run 2,", warnings[1])

            t += max_age + 1
            self.assertEqual(_scan(recent_task_ids=[3]), [])

            t += max_age + 1
            self.assertEqual(_scan(recent_task_ids=[1, 3]), [])

            t += max_age + 1
            warnings = _scan(recent_task_ids=[3])
            self.assertEqual(len(warnings), 1)
            self.assertIn("Task: run 1,", warnings[0])

    def test_running_stale_heartbeat_warns_again_after_resubmit(self):
        task = self._setup_task(num_tasks=1)
        engine = FixedStateEngine({1: gs.STATE_RUNNING})
        self._write_stale_logs(task, 1)
        self.assertEqual(len(self._scan_warnings(lambda: task.state(engine, 1, force=True))), 1)
        task.reset_cache()
        self.assertEqual(len(self._scan_warnings(lambda: task.state(engine, 1, force=True))), 1)

    def test_running_without_heartbeat_no_warning(self):
        task = self._setup_task(num_tasks=1)
        engine = FixedStateEngine({1: gs.STATE_RUNNING})
        self.assertIsNone(task.running(1))
        self.assertEqual(self._scan_warnings(lambda: task.state(engine, 1, force=True)), [])

    def test_running_heartbeat_stat_error_is_absent(self):
        task = self._setup_task(num_tasks=1)
        self._write_stale_logs(task, 1)
        usage_file = task.get_process_logging_path(1)
        real_stat = os.stat

        def _failing_stat(path, *args, **kwargs):
            if path == usage_file:
                raise OSError(116, "Stale file handle")
            return real_stat(path, *args, **kwargs)

        with mock.patch("os.stat", _failing_stat):
            self.assertIsNone(task.running(1))
            engine = FixedStateEngine({1: gs.STATE_RUNNING})
            self.assertEqual(self._scan_warnings(lambda: task.state(engine, 1, force=True)), [])
            engine = FixedStateEngine({1: gs.STATE_UNKNOWN})
            self.assertEqual(task.state(engine, 1, force=True), gs.STATE_INTERRUPTED_NOT_RESUMABLE)

    def test_running_heartbeat_directory_is_absent(self):
        task = self._setup_task(num_tasks=1)
        os.mkdir(task.get_process_logging_path(1))
        self.assertIsNone(task.running(1))

    def test_unknown_checks_heartbeat_each_scan(self):
        task = self._setup_task(num_tasks=1)
        engine = FixedStateEngine({1: gs.STATE_UNKNOWN})
        t0 = time.time()
        self._write_heartbeat(task, 1, mtime=t0)
        with mock.patch.object(sisyphus.task, "time") as clock:
            clock.time.return_value = t0
            self.assertEqual(task.state(engine, 1, force=True), gs.STATE_RUNNING)
            clock.time.return_value = t0 + self._heartbeat_max_age() + 1
            self.assertEqual(task.state(engine, 1, force=True), gs.STATE_INTERRUPTED_NOT_RESUMABLE)

    def test_running_marker_after_heartbeat_check(self):
        task = self._setup_task(num_tasks=2)
        engine = FixedStateEngine({1: gs.STATE_RUNNING, 2: gs.STATE_RUNNING})
        for task_id in (1, 2):
            self._write_heartbeat(task, task_id, mtime=time.time())
            self.assertEqual(task.state(engine, task_id, force=True), gs.STATE_RUNNING)
        self._write_finished_marker(task, 1, age=0)
        self._write(task.path(gs.STATE_ERROR, 2), age=0)
        self.assertEqual(task.state(engine, 1, force=True), gs.STATE_FINISHED)
        self.assertEqual(task.state(engine, 2, force=True), gs.STATE_ERROR)


if __name__ == "__main__":
    unittest.main()
