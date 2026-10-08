import os
import tempfile
import unittest
from unittest import mock

from sisyphus import localengine


class LETest(unittest.TestCase):
    def test_f(self):
        le = localengine.LocalEngine()
        le.start_engine()
        # TODO actuall testing...
        le.stop_engine()

    def test_recover_task_with_additional_worker_options(self):
        call = ["python", "sis", "worker", "work/Job.abc", "run", "1"]
        with tempfile.TemporaryDirectory() as tmp_dir:
            usage_file = os.path.join(tmp_dir, "usage.run.1")
            with open(usage_file, "w") as f:
                f.write(str({"pid": 12345, "requested_resources": {"cpu": 1, "gpu": 0}}))
            task = mock.Mock()
            task.get_process_logging_path.return_value = usage_file
            task.path.return_value = os.path.join(tmp_dir, "engine")
            task.get_worker_call.return_value = call
            task.task_name.return_value = "Job.abc.run"
            task.name.return_value = "run"
            process = mock.Mock()
            process.cmdline.return_value = ["/usr/bin/python3"] + call[1:] + ["--resume_job", "yes"]
            process.environ.return_value = {}
            le = localengine.LocalEngine()
            le.start_engine()
            try:
                with mock.patch.object(localengine.psutil, "Process", return_value=process), mock.patch.object(
                    localengine.logging, "warning"
                ) as warning:
                    self.assertTrue(le.try_to_recover_task(task, 1))
                warning.assert_not_called()
            finally:
                le.stop_engine()


if __name__ == "__main__":
    unittest.main()
