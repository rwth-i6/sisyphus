import sys
import unittest
import os

from sisyphus import engine
from sisyphus.job_path import Path
from sisyphus.graph import SISGraph
from sisyphus import http_server

TEST_DIR = 'test'
sys.path.append(TEST_DIR)


def test_jobs():
    import recipe.task.test as test_recipe
    job1 = test_recipe.Test(text=Path("input_text1.gz"))
    job2 = test_recipe.Test(text=Path("input_text2.gz"))
    job_merge1 = test_recipe.MergeInputs([job1.out, job2.out])
    job_merge2 = test_recipe.MergeInputs([job2.out, job1.out])
    job_merge3 = test_recipe.MergeInputs([job1.out, job2.out, job_merge1.out, job_merge2.out])
    graph = SISGraph(output={'test': job_merge3.out})
    job_engine = engine.Engine()

    server = http_server.start(sis_graph=graph, sis_engine=job_engine, debug=True, port=5001)
    # TODO actuall testing...

# test_jobs()
