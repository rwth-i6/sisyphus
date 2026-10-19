import os
import tempfile
import unittest
from ast import literal_eval
from unittest import mock

import sisyphus.global_settings as gs
from sisyphus import Job, Task
from sisyphus.engine import DeferSubmission, EngineBase
from sisyphus.simple_linux_utility_for_resource_management_engine import SimpleLinuxUtilityForResourceManagementEngine
from sisyphus.tools import execute_in_dir


class SubmitTestJob(Job):
    def __init__(self, resumable):
        self.resumable = resumable

    def run(self):
        pass

    def tasks(self):
        yield Task("run", resume="run" if self.resumable else None)


class RecordingEngine(EngineBase):
    def __init__(self):
        self.submitted_rqmts = []

    def get_default_rqmt(self, task):
        return {"cpu": 1, "mem": 1, "time": 1}

    def task_state(self, task, task_id):
        return gs.STATE_UNKNOWN

    def reset_cache(self):
        pass

    def submit_call(self, call, logpath, rqmt, name, task_name, task_ids):
        self.submitted_rqmts.append(rqmt)
        return "recording", []


class EngineSubmitTest(unittest.TestCase):
    def test_submit_passes_resumable(self):
        with tempfile.TemporaryDirectory() as tmp_dir, execute_in_dir(tmp_dir), mock.patch.object(
            gs, "BASE_DIR", tmp_dir
        ):
            for resumable in [True, False]:
                job = SubmitTestJob(resumable=resumable)
                job._sis_setup_directory()
                (task,) = job._sis_tasks()
                engine = RecordingEngine()
                engine.submit(task)
                (rqmt,) = engine.submitted_rqmts
                self.assertIs(rqmt["sis_resumable"], resumable)


class ArraySubmitTestJob(Job):
    def __init__(self, test_name):
        self.test_name = test_name

    def run(self, arg):
        pass

    def tasks(self):
        yield Task("run", args=[1, 2, 3])


class DeferringEngine(RecordingEngine):
    def __init__(self, partial_result):
        super().__init__()
        self.partial_result = partial_result

    def submit_call(self, call, logpath, rqmt, name, task_name, task_ids):
        super().submit_call(call, logpath, rqmt, name, task_name, task_ids)
        raise DeferSubmission("submit limit", retry_at=123.0, partial_result=self.partial_result)


class EngineSubmitDeferredTest(unittest.TestCase):
    def _submit(self, engine):
        with tempfile.TemporaryDirectory() as tmp_dir, execute_in_dir(tmp_dir), mock.patch.object(
            gs, "BASE_DIR", tmp_dir
        ):
            job = ArraySubmitTestJob(test_name=self.id())
            job._sis_setup_directory()
            (task,) = job._sis_tasks()
            retry_at = engine.submit(task)
            submit_log = task.path(gs.ENGINE_SUBMIT)
            if not os.path.exists(submit_log):
                return retry_at, []
            with open(submit_log) as f:
                return retry_at, [literal_eval(line) for line in f]

    def test_deferred_no_history(self):
        retry_at, history = self._submit(DeferringEngine(partial_result=None))
        self.assertEqual(retry_at, 123.0)
        self.assertEqual(history, [])

    def test_partially_deferred_history_of_submitted(self):
        retry_at, history = self._submit(DeferringEngine(partial_result=("deferring", [([1, 2], "42")])))
        self.assertEqual(retry_at, 123.0)
        ((task_ids, submit_info),) = history
        self.assertEqual(task_ids, [1, 2])
        self.assertEqual(submit_info["engine_info"], [([1, 2], "42")])
        self.assertEqual(submit_info["engine_name"], "deferring")

    def test_not_deferred(self):
        retry_at, history = self._submit(RecordingEngine())
        self.assertIsNone(retry_at)
        ((task_ids, _),) = history
        self.assertEqual(task_ids, [1, 2, 3])


class SlurmOptionsTest(unittest.TestCase):
    def _options(self, **rqmt):
        engine = SimpleLinuxUtilityForResourceManagementEngine(default_rqmt={})
        return engine.options(dict({"cpu": 1, "mem": 1, "time": 1}, **rqmt))

    def test_not_resumable_no_requeue(self):
        self.assertIn("--no-requeue", self._options(sis_resumable=False))

    def test_resumable_cluster_default(self):
        self.assertNotIn("--no-requeue", self._options(sis_resumable=True))
        self.assertNotIn("--no-requeue", self._options())

    def test_sbatch_args_after_no_requeue(self):
        options = self._options(sis_resumable=False, sbatch_args=["--requeue"])
        self.assertLess(options.index("--no-requeue"), options.index("--requeue"))


if __name__ == "__main__":
    unittest.main()
