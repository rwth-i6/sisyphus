import os
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

import sisyphus.global_settings as gs
from sisyphus.engine import EngineBase
from sisyphus.simple_linux_utility_for_resource_management_engine import SimpleLinuxUtilityForResourceManagementEngine
from sisyphus.worker import PreemptionWatcher

# writes "started", then "SIGTERM" when it gets SIGTERM
_CHILD = """
import signal, sys, time
def handler(signum, frame):
    with open(sys.argv[1], "a") as f:
        f.write("SIGTERM\\n")
    sys.exit(1)
signal.signal(signal.SIGTERM, handler)
with open(sys.argv[1], "a") as f:
    f.write("started\\n")
time.sleep(60)
"""


class PreemptedEngine(EngineBase):
    def __init__(self, preempted):
        self.preempted = preempted

    def is_job_being_preempted(self):
        return self.preempted

    def is_signaled_on_preemption(self, process):
        return process.name() == "srun"


class PreemptionWatcherTest(unittest.TestCase):
    def _run(self, *, preempted):
        with tempfile.TemporaryDirectory() as tmp_dir, mock.patch.object(
            gs, "active_engine", PreemptedEngine(preempted)
        ):
            # a process named srun, as Slurm signals its step itself
            srun = os.path.join(tmp_dir, "srun")
            os.symlink(sys.executable, srun)
            outs = {name: os.path.join(tmp_dir, name + ".out") for name in ["child", "grandchild", "srun"]}
            procs = [
                subprocess.Popen([sys.executable, "-c", _CHILD, outs["child"]]),
                # e.g. a shell running the actual command
                subprocess.Popen(["sh", "-c", '"$0" -c "$1" "$2"; true', sys.executable, _CHILD, outs["grandchild"]]),
                subprocess.Popen([srun, "-c", _CHILD, outs["srun"]]),
            ]
            try:
                while not all(os.path.exists(out) for out in outs.values()):
                    time.sleep(0.05)
                watcher = PreemptionWatcher(0.1)
                watcher.start()
                time.sleep(1)
                watcher.stop()
                watcher.join()
                time.sleep(0.5)
                return {name: open(out).read().split() for name, out in outs.items()}
            finally:
                for proc in procs:
                    proc.kill()
                    proc.wait()

    def test_preempted_forwards_sigterm(self):
        self.assertEqual(
            self._run(preempted=True),
            {"child": ["started", "SIGTERM"], "grandchild": ["started", "SIGTERM"], "srun": ["started"]},
        )

    def test_not_preempted(self):
        self.assertEqual(
            self._run(preempted=False), {"child": ["started"], "grandchild": ["started"], "srun": ["started"]}
        )


class SlurmEngineSignaledOnPreemptionTest(unittest.TestCase):
    def test_srun(self):
        engine = SimpleLinuxUtilityForResourceManagementEngine(default_rqmt={})
        for name, signaled in [("srun", True), ("python3", False), ("torchrun", False)]:
            process = mock.Mock()
            process.name.return_value = name
            self.assertEqual(engine.is_signaled_on_preemption(process), signaled)


if __name__ == "__main__":
    unittest.main()
