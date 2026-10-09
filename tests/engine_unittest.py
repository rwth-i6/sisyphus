import tempfile
import unittest
from unittest import mock

import sisyphus.global_settings as gs
from sisyphus import Job, Task
from sisyphus.engine import EngineBase
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
