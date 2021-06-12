import os
import gzip
import glob
import sisyphus.hash
import time

from sisyphus import *


class LineSpliter(Job):
    __sis_auto_cleanup__ = False  # disable automatic cleanup for testing

    def __init__(self, text):
        assert not tk.running_in_worker()
        self.text = text
        self.out_dir = self.output_path('out_dir', True)
        self.out = self.output_var('out_path', pickle=True)

    def run(self):
        assert tk.running_in_worker()
        start_time = time.time()
        waste = []
        # just waste some computation time and memory
        while time.time() - start_time < 10:
            waste.append(len(waste))

        if tk.zipped(self.text):
            fopen = gzip.open
        else:
            fopen = open

        # split file into multiple files
        out_path = []
        with fopen(str(self.text)) as f:
            line_nr = 0
            for line_nr, line in enumerate(f, 1):
                filename = os.path.join(str(self.out_dir), 'txt.%i.gz' % line_nr)
                with gzip.open(filename, 'w') as out:
                    print(out, filename, line, line_nr)
                    out.write(line.encode())
                    out_path.append(self.output_path('out_dir/txt.%i.gz' % line_nr, True))
        self.out.set(out_path)

    def tasks(self):
        yield Task('run', rqmt={'cpu': 1, 'gpu': 0})

    @staticmethod
    def hash(text):
        return sisyphus.hash.short_hash(str(text), 18)


class Parallel(Job):
    def __init__(self, split_path, parallel_function):
        self.split_path = split_path
        self.parallel_function = parallel_function
        self.parallel_out = []
        self.out = self.output_path('out')
        split_path.keep_value(50)

    def update(self):
        if len(self.parallel_out) == 0:  # only due this once
            check_block = tk.sub_block('checker')
            for count, path in enumerate(self.split_path.get()):
                with tk.block('pipeline_%i' % count):
                    pipeline = self.parallel_function(path, check_block,
                                                      self.tags | {'pipeline_%i' % count})
                    self.add_input(pipeline.out)
                    self.add_input(pipeline.score)
                    self.parallel_out.append(pipeline)

            # uncomment next line to test if exporting the graph warns from unexported jobs behind sets
            # self.parallel_out = set(self.parallel_out)

    def run(self, pos):
        self.sh('echo > run.{pos}', pos=pos)
        for pipeline in self.parallel_out:
            self.sh('cat {text} | gzip -d', text=pipeline.out)
            self.sh('cat {score} | gzip -df', score=pipeline.score)

    def setup(self, pos):
        self.sh('echo > setup.{pos}', pos=pos)

    def finalize(self):
        self.sh('ls setup.*')
        self.sh('ls run.*')

        max_score = -1
        max_text = None
        for pipeline in self.parallel_out:
            score = int(self.sh('cat {score} | gzip -df', True, score=pipeline.score))
            if score > max_score or max_text is None:
                max_score = score
                max_text = pipeline.out

        self.sh('echo {score} > {out}', score=max_score)
        self.sh('cat {text} | gzip -d >> {out}', text=max_text)

    def tasks(self):
        yield Task(start='setup', resume='setup', rqmt={}, args=range(1, 9), tries=4)
        yield Task(start='run', resume='run', rqmt={}, args=range(1, 9))
        yield Task(start='finalize', resume='finalize')
