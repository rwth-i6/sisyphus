from sisyphus import *


class Add(Job):
    """Simple example with an input and an output file"""

    def __init__(self, a, b):
        self.a = a
        self.b = b
        self.out = self.output_var("result", backup="?")

    def run(self):
        a = self.a
        if isinstance(a, tk.Variable):
            a = a.get()
        b = self.b
        if isinstance(b, tk.Variable):
            b = b.get()
        self.out.set(a + b)

    def tasks(self):
        yield Task("run", mini_task=True)


class Multiply(Job):
    """Simple example with an input and an output file"""

    def __init__(self, a, b):
        self.a = a
        self.b = b
        self.out = self.output_var("result", backup="?")

    def run(self):
        a = self.a
        if isinstance(a, tk.Variable):
            a = a.get()
        b = self.b
        if isinstance(b, tk.Variable):
            b = b.get()
        self.out.set(a * b)

    def tasks(self):
        yield Task("run", mini_task=True)


class Power(Job):
    """Simple example with an input and an output file"""

    def __init__(self, a, b):
        self.a = a
        self.b = b
        self.out = self.output_var("result", backup="?")

    def run(self):
        a = self.a
        if isinstance(a, tk.Variable):
            a = a.get()
        b = self.b
        if isinstance(b, tk.Variable):
            b = b.get()
        self.out.set(a**b)

    def tasks(self):
        yield Task("run", mini_task=True)
