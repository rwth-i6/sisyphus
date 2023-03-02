from sisyphus import *
import os
from typing import List


class Pipeline(Job):
    """ This job takes a text file as input and pipes it through all given commands """

    # Everything given to the constructor will be used to compute a hash for this job.
    def __init__(self, text: tk.Path, pipeline: List):
        # You can validating the inputs to spot errors earlier
        assert text, "No text given"
        assert isinstance(text, tk.Path), "Given input"
        assert pipeline

        #
        self.text = text
        self.pipeline = pipeline

        self.out = self.output_path('out.txt')

    # Task should return a list, a generator, or something else Sisyphus can iterator over containing all
    # tasks of this job. In this example the job has only one task calling the `run` function
    def tasks(self):
        # Getting the size of the given input file to estimate how much time we need.
        # tasks() is only called when all inputs are available, we can therefore assume all input files exist.
        size = os.path.getsize(self.text.get_path())

        if size <= 1024 * 1024:
            time = 1
        elif size <= 1024 * 1024 * 1024:
            time = 4
        else:
            time = 8

        return [Task('run',  # The first argument defines which tasks should be executed to for this task
                     'run',  # The second (optional) argument defines which function should be called if the job got
                             # interrupted, e.g. killed do too much memory usage. If no cleanup is needed this
                             # can be the same as the first argument. Best practices is to just write one function
                             # that can handle both cases on pass it in both positions.
                             # If no second argument is given the task will not be restarted automatically.
                     rqmt={'time': time, 'mem': 2, 'cpu': 2},  # Requirements needed to run this task
                     tries=3)]  # 3 tries if pipe result is empty or pipe failed

    # This function will be call when the job is started
    def run(self):
        self.pipe = ' | '.join([str(i) for i in self.pipeline])
        # self.sh will run the given string in a shell. Before executing it the string format function will be called
        # to replace {...} which attributes from this job.
        self.sh('cat {text} | {pipe} > {out}')
        # assume that we do not want empty pipe results
        assert not (os.stat(str(self.out)).st_size == 0), "Pipe result was empty"


# Jobs are regular python classes, meaning you can just subclass an existing class to reuse it's code
class Head(Pipeline):
    def __init__(self, text, length, check_output_length=True):
        # tk.Delayed takes any object and converts it into a Delayed object which allows us to define operations
        # which will only be computed at runtime. Here we want to delay formatting since the value of length
        # isn't known before length is computed.
        super().__init__(text, [tk.Delayed('head -n {}').format(length)])
        self.length = length
        self.check_output_length = check_output_length

    def run(self):
        super.run()
        if self.check_output_length:
            output_length = int(self.sh('cat {out} | wc -l', capture_output=True))
            assert self.length.get() == output_length, "Created output file is to short"

    # This is how the computed hash can be modified, since `check_output_length` does not change the output
    # of this job we can exclude it from the hash computation
    @classmethod
    def hash(cls, parsed_args):
        args = parsed_args.copy()
        del args['check_output_length']
        return super().hash(args)


# Here is a Job with multiple Variables as output
class WordCount(Job):
    def __init__(self, text):
        self.text = text
        self.character = self.output_var('char')
        self.lines = self.output_var('lines')
        self.words = self.output_var('words')

    def run(self):
        line = self.sh('wc < {text}', capture_output=True)
        l, w, c = line.split()
        self.lines.set(int(l))
        self.words.set(int(w))
        self.character.set(int(c))

    # Here is an example of task returning a generator
    def tasks(self):
        yield Task('run')
