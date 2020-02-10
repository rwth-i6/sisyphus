from sisyphus import *
from recipe import parallel
from recipe import pipeline


@tk.block(cache=True)
def init(input_file):
    input_file = tk.input_path(input_file)

    spliter = parallel.LineSpliter(input_file)
    spliter.set_rqmt('run', rqmt={'cpu': 1, 'mem': 2, 'gpu': 1})
    return parallel.Parallel(spliter.out, pipeline.pipeline)


def main():
    input_data = tk.Path('data/5lines.txt', tags={'5lines'})
    tk.register_output('result', init(input_data).out, export_graph=True)


if __name__ == '__main__':
    input_data = tk.Path('data/5lines.txt', tags={'5lines'})
    output = init(input_data).out
    tk.run(output)
    tk.sh('cp %s myoutput' % output)
