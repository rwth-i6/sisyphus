import os
import subprocess
import unittest
from unittest import mock

from sisyphus import simple_linux_utility_for_resource_management_engine as slurm_engine


class SlurmEngineTest(unittest.TestCase):
    def _is_job_being_preempted(self, *results):
        engine = slurm_engine.SimpleLinuxUtilityForResourceManagementEngine(default_rqmt={})
        procs = [subprocess.CompletedProcess(args=[], returncode=rc, stdout=out, stderr="") for rc, out in results]
        with mock.patch.dict(os.environ, {"SLURM_JOB_ID": "123"}), mock.patch.object(
            slurm_engine.subprocess, "run", side_effect=procs
        ) as run, mock.patch.object(slurm_engine.time, "sleep"):
            preempted = engine.is_job_being_preempted()
        self.assertEqual(run.call_args[0][0], ["squeue", "-h", "-j", "123", "-O", "PreemptTime"])
        return preempted

    def test_not_preempted(self):
        self.assertFalse(self._is_job_being_preempted((0, "N/A                 \n")))

    def test_preempted(self):
        self.assertTrue(self._is_job_being_preempted((0, "2026-10-08T16:00:00 \n")))

    def test_squeue_fails_then_preempted(self):
        self.assertTrue(self._is_job_being_preempted((1, ""), (0, "2026-10-08T16:00:00 \n")))

    def test_squeue_always_fails(self):
        self.assertFalse(self._is_job_being_preempted((1, ""), (1, ""), (1, "")))


if __name__ == "__main__":
    unittest.main()
