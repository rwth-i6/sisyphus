import os
import subprocess
import unittest
from unittest import mock

from sisyphus import simple_linux_utility_for_resource_management_engine as slurm_engine
from sisyphus.engine import DeferSubmission


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


def _sbatch_ok(job_id):
    return subprocess.CompletedProcess(args=[], returncode=0, stdout=b"Submitted batch job %i\n" % job_id, stderr=b"")


def _sbatch_error(msg):
    stderr = b"sbatch: error: %s\nsbatch: error: Batch job submission failed: ...\n" % msg.encode()
    return subprocess.CompletedProcess(args=[], returncode=1, stdout=b"", stderr=stderr)


class SlurmSubmitLimitTest(unittest.TestCase):
    def setUp(self):
        self.engine = slurm_engine.SimpleLinuxUtilityForResourceManagementEngine(default_rqmt={})
        self.now = 1000.0
        self.sbatch_results = []
        self.sbatch_calls = 0
        patches = [
            mock.patch.object(slurm_engine.time, "monotonic", side_effect=lambda: self.now),
            mock.patch.object(slurm_engine.subprocess, "run", side_effect=self._run),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def _run(self, *_args, **_kwargs):
        self.sbatch_calls += 1
        return self.sbatch_results.pop(0)

    def _submit(self, task_ids):
        rqmt = {"cpu": 1, "mem": 1, "time": 1}
        return self.engine.submit_call(["sis", "worker"], "log", rqmt, "Job.abc.run", "run", task_ids)

    def test_qos_limit_deferred_with_cooldown(self):
        self.sbatch_results = [_sbatch_error("QOSMaxSubmitJobPerUserLimit")]
        with self.assertRaises(DeferSubmission) as ctx:
            self._submit([1, 2, 3])
        self.assertIn("QOSMaxSubmitJobPerUserLimit", str(ctx.exception))
        self.assertEqual(ctx.exception.retry_at, 1060.0)
        self.assertIsNone(ctx.exception.partial_result)
        self.now = 1059.0
        with self.assertRaises(DeferSubmission) as ctx:
            self._submit([1, 2, 3])
        self.assertIn("QOSMaxSubmitJobPerUserLimit", str(ctx.exception))
        self.assertEqual(ctx.exception.retry_at, 1060.0)
        self.assertEqual(self.sbatch_calls, 1)
        self.now = 1060.0
        self.sbatch_results = [_sbatch_ok(123)]
        self.assertEqual(self._submit([1, 2, 3]), ("slurm", [([1, 2, 3], "123")]))
        self.assertEqual(self.sbatch_calls, 2)

    def test_all_submit_limit_reasons_deferred(self):
        for reason in slurm_engine._SUBMIT_LIMIT_REASONS:
            self.engine = slurm_engine.SimpleLinuxUtilityForResourceManagementEngine(default_rqmt={})
            self.sbatch_results = [_sbatch_error(reason)]
            with self.assertRaises(DeferSubmission) as ctx:
                self._submit([1])
            self.assertIn(reason, str(ctx.exception))

    def test_other_rejection_not_deferred(self):
        for msg in [
            "QOSMaxWallDurationPerJobLimit",
            "Invalid account or account/partition combination specified",
            "Requested node configuration is not available",
        ]:
            self.sbatch_results = [_sbatch_error(msg)]
            self.assertEqual(self._submit([1]), ("slurm", [([1], None)]))

    def test_partial_submission_keeps_job_id(self):
        self.sbatch_results = [_sbatch_ok(123), _sbatch_error("QOSMaxSubmitJobPerUserLimit")]
        with self.assertRaises(DeferSubmission) as ctx:
            self._submit([1, 2, 5])
        self.assertEqual(ctx.exception.partial_result, ("slurm", [([1, 2], "123")]))
        self.assertEqual(self.sbatch_calls, 2)


if __name__ == "__main__":
    unittest.main()
