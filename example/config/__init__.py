from sisyphus import *
from recipe import parallel
from recipe import pipeline


@tk.block(cache=True)
def init(input_file):
    input_file = tk.input_path(input_file)

    spliter = parallel.LineSpliter(input_file)
    spliter.set_rqmt("run", rqmt={"cpu": 1, "mem": 2, "gpu": 1})
    return parallel.Parallel(spliter.out, pipeline.pipeline)


def main():
    input_data = tk.Path("data/5lines.txt", tags={"5lines"})
    tk.register_output("result", init(input_data).out, export_graph=True)


async def async_main():
    input_data = tk.Path("data", tags={"5lines"})
    input_data = input_data.join_right("5lines.txt")
    spliter = parallel.LineSpliter(input_data)
    # Test if requesting gpu works
    spliter.set_rqmt("run", rqmt={"cpu": 1, "mem": 2, "gpu": 1})
    await tk.async_run(spliter.out)

    check_block = tk.sub_block("checker")
    parallel_out = []
    for count, path in enumerate(spliter.out.get()):
        with tk.block("pipeline_%i" % count):
            p = pipeline.pipeline(path, check_block, input_data.tags | {"pipeline_%i" % count})
            parallel_out.append(p)

    await tk.async_run(parallel_out)
    for p in parallel_out:
        print(p.score.get(), p.out)


if __name__ == "__main__":
    input_data = tk.Path("data/5lines.txt", tags={"5lines"})
    output = init(input_data).out
    tk.run(output)
    tk.sh("cp %s myoutput" % output)
