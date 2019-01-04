import os

from sisyphus import *

class Test(Job):

    def __init__(self, text=None):
        self.text = text
        self.out = self.output_path('out_text.gz')

    def run(self, pos):
        """Run the actual job"""
        print(self._sis_kwargs_str)

        print("I'm running!!!")
        print(pos)
        print(os.getcwd())

        print("In:",  str(self.text))
        print("Out:", self.out)
        self.sh("echo SHELL COMMAND")
        print(pos)
        self.sh("echo $$ > test.file.{job}",job=pos)

        self.sh('zcat -f {text}')
        self.sh("pwd")

    def finalize(self):
        self.sh("mv test.file.4 {out}", out=self.out)

    def tasks(self):
        return [Task('run', args=range(3,6), rqmt={'cpu':1}),
                Task('finalize')
                ]

class MergeInputs(Job):

    def __init__(self, texts=[]):
        self.texts = texts
        self.out = self.output_path('out_text.gz')

    def run(self, pos):
        """Run the actual job"""
        print(self._sis_kwargs_str)

        print("I'm running!!!")
        print(pos)
        print(os.getcwd())

        print("In:",  str(self.text))
        print("Out:", self.out)
        self.sh("echo SHELL COMMAND")
        print(pos)
        self.sh("echo $$ > test.file.{job}",job=pos)

        for self.text in self.texts:
            self.sh('zcat -f {text}')
        self.sh("pwd")

    def finalize(self):
        self.sh("mv test.file.4 {out}", out=self.out)

    def tasks(self):
        return [Task('run', args=range(3,6), rqmt={'cpu':1}),
                Task('finalize')
                ]
