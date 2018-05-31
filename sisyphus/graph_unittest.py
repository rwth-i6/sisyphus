import sys
import unittest

from sisyphus.job_path import Path
from sisyphus.graph import SISGraph

import hashlib
from sisyphus.hash import sis_hash_helper
import sisyphus.global_settings as gs
# Use old hash function to avoid updating precomputed hashes
gs.SIS_HASH = lambda x: hashlib.md5(sis_hash_helper(x)).hexdigest()

TEST_DIR = 'test'
sys.path.append(TEST_DIR)


def get_example_graph():
    from recipe.task import test
    job1 = test.Test(text=Path("input_text1.gz"))
    job2 = test.Test(text=Path("input_text2.gz"))
    job_merge1 = test.MergeInputs([job1.out, job2.out])
    job_merge2 = test.MergeInputs([job2.out, job1.out])
    job_merge3 = test.MergeInputs([job1.out, job2.out, job_merge1.out, job_merge2.out])
    return SISGraph(output={'test': job_merge3.out})


class GraphTest(unittest.TestCase):

    def test_jobs(self):
        graph = get_example_graph()
        jobs = list(graph.jobs())
        self.assertEqual(
            sorted([job._sis_id() for job in jobs]),
            sorted(['task/test/MergeInputs.5441df2af53fd62b8e0b15d619b0e51c',
                    'task/test/MergeInputs.699d06d9fcfb871889cc2d3cd6623a6c',
                    'task/test/MergeInputs.fff2af28cc087c94d5c44357e142f574',
                    'task/test/Test.7a6735aa36750fd8818bcae092713493',
                    'task/test/Test.7b7ec5efb6cfa2a74d6995521e086fc8']))
        self.assertEqual(len(jobs), 5)
        self.assertEqual(
            graph.job_by_id('task/test/MergeInputs.699d06d9fcfb871889cc2d3cd6623a6c')._sis_id(),
            'task/test/MergeInputs.699d06d9fcfb871889cc2d3cd6623a6c')

    def test_sort(self):
        graph = get_example_graph()
        jobs = list(i._sis_id() for i in graph.jobs_sorted())
        self.assertEqual(jobs,
                         ['task/test/Test.7a6735aa36750fd8818bcae092713493',
                          'task/test/Test.7b7ec5efb6cfa2a74d6995521e086fc8',
                          'task/test/MergeInputs.5441df2af53fd62b8e0b15d619b0e51c',
                          'task/test/MergeInputs.fff2af28cc087c94d5c44357e142f574',
                          'task/test/MergeInputs.699d06d9fcfb871889cc2d3cd6623a6c'])

if __name__ == '__main__':
    unittest.main()
