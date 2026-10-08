import hashlib
import os
import pickle
import shutil
import sys
import tempfile
import unittest
from unittest import mock

from sisyphus import Job, Task
from sisyphus.job_path import Path
from sisyphus.tools import execute_in_dir
from sisyphus.hash import sis_hash_helper

# Use old hash function to avoid updating precomputed hashes
import sisyphus.global_settings as gs

gs.SIS_HASH = lambda x: hashlib.md5(sis_hash_helper(x)).hexdigest()

# TODO replace fixed job hashes and compare if things changed
sys.path.append(gs.TEST_DIR)


class JobTest(unittest.TestCase):
    def test_connect_path(self):
        from recipe.task.test import Test

        job = Test(text="input_text.gz")

        self.assertEqual(job.text, "input_text.gz")
        self.assertEqual(
            str(job.out), os.path.abspath("work/task/test/Test.f744898e46ca9452ff1889edc988d045/output/out_text.gz")
        )

        job = Test(text=job.out)
        self.assertEqual(
            str(job.text), os.path.abspath("work/task/test/Test.f744898e46ca9452ff1889edc988d045/output/out_text.gz")
        )

        job = Test(text=job.out)
        job = Test(text=job.out)
        self.assertEqual(
            str(job.text), os.path.abspath("work/task/test/Test.a4ce523976aa98f9fca9d9956bbfdffa/output/out_text.gz")
        )
        self.assertEqual(
            str(job.out), os.path.abspath("work/task/test/Test.a14422432288985538db5a4be40a44aa/output/out_text.gz")
        )

    def test_sis_hash(self):
        from recipe.task.test import Test

        # regular hash
        job = Test(text="input_text.gz")
        self.assertEqual(job._sis_id(), "task/test/Test.f744898e46ca9452ff1889edc988d045")

        # test versioning
        Test.__sis_version__ = 1
        job = Test(text="input_text.gz")
        self.assertEqual(job._sis_id(), "task/test/Test.4efda2530a66c6d8973f0991996ad9a7")

        # test versioning
        Test.__sis_version__ = 2
        job = Test(text="input_text.gz")
        self.assertEqual(job._sis_id(), "task/test/Test.c7638c71725cf3e7188db454d4443614")
        Test.__sis_version__ = None

        Test.sis_hash_exclude = {"text": "input_text.gz"}
        job = Test(text="input_text.gz")
        self.assertEqual(job._sis_id(), "task/test/Test.f744898e46ca9452ff1889edc988d045")

        Test.sis_hash_exclude = {"text": "input_text2.gz"}
        job = Test(text="input_text2.gz")
        self.assertEqual(job._sis_id(), "task/test/Test.36cb8075860f276698a63cbec193f025")
        job = Test(text="input_text.gz")
        self.assertEqual(job._sis_id(), "task/test/Test.f744898e46ca9452ff1889edc988d045")
        Test.sis_hash_exclude = {}

    def test_run(self):
        with execute_in_dir(gs.TEST_DIR):
            from recipe.task.test import Test

            job = Test(text=Path("input_text.gz"))
            job._sis_setup_directory()
            shutil.rmtree(gs.WORK_DIR)


class ResumableTestJob(Job):
    def __init__(self, resumable):
        self.resumable = resumable

    def run(self):
        pass

    def tasks(self):
        yield Task("run", resumable=self.resumable)


class TaskResumableTest(unittest.TestCase):
    def test_default_not_resumable(self):
        self.assertFalse(Task("run").resumeable())

    def test_resumable(self):
        self.assertTrue(Task("run", resumable=True).resumeable())

    def test_resume_is_deprecated(self):
        with self.assertWarns(DeprecationWarning):
            task = Task("run", resume="run")
        self.assertTrue(task.resumeable())

    def test_resume_and_resumable(self):
        with self.assertRaises(AssertionError):
            Task("run", resume="run", resumable=True)

    def test_unpickle_task_with_resume(self):
        for resume, resumable in [("run", True), (None, False)]:
            task = Task("run")
            del task._resumable
            task._resume = resume
            self.assertEqual(pickle.loads(pickle.dumps(task)).resumeable(), resumable)

    def test_interrupted_state(self):
        with tempfile.TemporaryDirectory() as tmp_dir, execute_in_dir(tmp_dir), mock.patch.object(
            gs, "BASE_DIR", tmp_dir
        ), mock.patch.object(gs, "WAIT_PERIOD_JOB_FS_SYNC", 0), mock.patch.object(
            gs, "PLOGGING_UPDATE_FILE_PERIOD", 0
        ), mock.patch.object(gs, "WAIT_PERIOD_JOB_CLEANUP", 0):
            for resumable, state in [
                (True, gs.STATE_INTERRUPTED_RESUMABLE),
                (False, gs.STATE_INTERRUPTED_NOT_RESUMABLE),
            ]:
                job = ResumableTestJob(resumable=resumable)
                job._sis_setup_directory()
                (task,) = job._sis_tasks()
                # started before, not finished, not running anymore
                with open(task.path(gs.JOB_LOG, 1), "w"):
                    pass
                os.utime(task.path(gs.JOB_LOG, 1), (0, 0))
                self.assertEqual(task._get_state_helper(None, 1), state)


if __name__ == "__main__":
    unittest.main()
