from recipe import parallel
from recipe import pipeline

from sisyphus import *

RelPath = tk.Path


@tk.block(cache=True)
def init(input_file):
    input_file = tk.input_path(input_file)

    spliter = parallel.LineSpliter(input_file)
    spliter.set_rqmt("run", rqmt={"cpu": 3, "mem": 2})
    return parallel.Parallel(spliter.out, pipeline.pipeline)


def starter(path, tags, output):
    input_data = RelPath(path, tags=tags)
    tk.register_output(output, init(input_data).out, export_graph=True)
