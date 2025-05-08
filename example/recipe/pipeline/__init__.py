import os
from recipe.pipeline import external

from sisyphus import *

Path = setup_path(__package__)


class Simple(Job):
    """Simple example with an input and an output file"""

    def __init__(self, text):
        self.text = text
        self.out = self.output_path("out.gz")

    def run(self):
        self.sh('echo "Pipe1" > tmp')
        self.sh("cat {text} | gzip -d | cat - tmp | gzip > {out}")

    def tasks(self):
        yield Task("run", rqmt={"cpu": 2})


class Arguments(Job):
    """Parallel execution of multiple arguments"""

    def __init__(self, text):
        self.text = text
        self.out = self.output_path("out.gz")

    def run(self, num, message):
        with tk.mktemp() as self.tmp:
            print(self.tmp)
            self.sh("mkdir {tmp}")
        self.num = num
        self.sh('echo "Pipe2: {message}" > tmp.{num}', message=message)
        if num == 0:
            self.sh("cat tmp.{num} | gzip > {out}")

    def finalize(self, args):
        self.sh("cat {text} | gzip -d > tmp")
        for num, message in args:
            self.sh("cat tmp.{num} >> tmp", num=num)
        self.sh("cat tmp | gzip > {out}")

    def tasks(self):
        args = list(enumerate(["foo", "bar", "code", "abc"], 1))
        yield Task("run", args=args)
        yield Task("finalize", args=[[args]])


class Simple2(Job):
    def __init__(self, text):
        self.text = text
        self.out = self.output_path("out.gz")

    def run(self):
        self.sh('echo "Pipe4" > tmp')
        self.sh("cat {text} | gzip -d | cat - tmp | gzip > {out}")

    def tasks(self):
        yield Task("run")


class FinishedParts(Job):
    def __init__(self, text):
        self.text = text
        self.out1 = self.output_path("out1.gz")
        self.out2 = self.output_path("out2.gz")
        self.out3 = self.output_path("out3.gz")

    def run(self):
        self.sh('echo "FinishedParts1" > tmp')
        self.sh("cat {text} | gzip -d | cat - tmp | gzip > {out1}")
        self.sh("sleep 10")
        self.sh('echo "FinishedParts2" > tmp')
        self.sh("cat {out1} | gzip -d | cat - tmp | gzip > {out2}")
        self.sh("sleep 10")
        self.sh('echo "FinishedParts3" > tmp')
        self.sh("cat {out2} | gzip -d | cat - tmp | gzip > {out3}")

    def tasks(self):
        yield Task("run")

    def path_available(self, path):
        assert isinstance(path, tk.Path)
        assert path.creator == self
        return os.path.isfile(str(path))


class SimplePart1(Simple2):
    pass  # just giving it a new name


class SimplePart2(Simple2):
    pass  # just giving it a new name


class SimplePart3(Simple2):
    pass  # just giving it a new name


class Merger(Job):
    def __init__(self, inputs):
        self.inputs = inputs
        self.out = self.output_path("merger.gz")

    def run(self):
        self.files = " ".join(str(i) for i in self.inputs)
        self.sh("cat {files} | gzip -df | gzip > {out}")

    def tasks(self):
        yield Task("run")


class Piepline:
    def __init__(self, text, score):
        self.text = text
        self.score = score
        self.out = text
        self.score = score


def pipeline(text, check_block, tags):
    pipe1 = Simple(text, sis_tags=tags)
    pipe2 = Arguments(pipe1.out)
    with check_block:
        pipe3 = external.CheckState(pipe2.out)
    parts = FinishedParts(Simple2(pipe2.out).out)
    pipe4 = Merger([SimplePart1(parts.out1).out, SimplePart2(parts.out2).out, SimplePart3(parts.out3).out])
    return Piepline(pipe4.out, pipe3.out)
