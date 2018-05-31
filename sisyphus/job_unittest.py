import unittest
import os
import sys
import hashlib

from sisyphus.job_path import Path
from sisyphus.tools import execute_in_dir
from sisyphus.hash import sis_hash_helper

# Use old hash function to avoid updating precomputed hashes
import sisyphus.global_settings as gs
gs.SIS_HASH = lambda x: hashlib.md5(sis_hash_helper(x)).hexdigest()

TEST_DIR = 'test'
sys.path.append(TEST_DIR)

# TODO replace fixed job hashes and compare if things changed


class JobTest(unittest.TestCase):

    def test_connect_path(self):
        from recipe.task.test import Test

        job = Test(text="input_text.gz")

        self.assertEqual(job.text, "input_text.gz")
        self.assertEqual(str(job.out),
                         os.path.abspath("work/task/test/Test.f744898e46ca9452ff1889edc988d045/output/out_text.gz"))

        job = Test(text=job.out)
        self.assertEqual(str(job.text),
                         os.path.abspath("work/task/test/Test.f744898e46ca9452ff1889edc988d045/output/out_text.gz"))

        job = Test(text=job.out)
        job = Test(text=job.out)
        self.assertEqual(str(job.text),
                         os.path.abspath("work/task/test/Test.a4ce523976aa98f9fca9d9956bbfdffa/output/out_text.gz"))
        self.assertEqual(str(job.out),
                         os.path.abspath("work/task/test/Test.a14422432288985538db5a4be40a44aa/output/out_text.gz"))

    def test_sis_hash(self):
        from recipe.task.test import Test

        # regular hash
        job = Test(text="input_text.gz")
        self.assertEqual(job._sis_id(), 'task/test/Test.f744898e46ca9452ff1889edc988d045')

        # test versioning
        Test.sis_version = 1
        job = Test(text="input_text.gz")
        self.assertEqual(job._sis_id(), 'task/test/Test.4efda2530a66c6d8973f0991996ad9a7')

        # test versioning
        Test.sis_version = 2
        job = Test(text="input_text.gz")
        self.assertEqual(job._sis_id(), 'task/test/Test.c7638c71725cf3e7188db454d4443614')
        Test.sis_version = None

        Test.sis_hash_exclude = {'text': 'input_text.gz'}
        job = Test(text="input_text.gz")
        self.assertEqual(job._sis_id(), 'task/test/Test.43a8c463b5aa0403c1306552bf7dad10')

        Test.sis_hash_exclude = {'text': 'input_text2.gz'}
        job = Test(text="input_text2.gz")
        self.assertEqual(job._sis_id(), 'task/test/Test.43a8c463b5aa0403c1306552bf7dad10')
        job = Test(text="input_text.gz")
        self.assertEqual(job._sis_id(), 'task/test/Test.f744898e46ca9452ff1889edc988d045')
        Test.sis_hash_exclude = {}

    def test_run(self):
        with execute_in_dir(TEST_DIR):
            from recipe.task.test import Test
            job = Test(text=Path("input_text.gz"))
            job._sis_setup_directory()

if __name__ == '__main__':
    unittest.main()
