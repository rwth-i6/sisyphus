import random
import os
import time

from sisyphus import *
Path = setup_path(__package__)


class CheckState(Job):
    """Example how binaries that detach from the main process and run in the background could be handled"""
    def __init__(self, text, binary=Path('starter.sh')):
        self.text = text
        self.binary = binary
        self.out = self.output_path('score')

    def run(self):
        # create 'random' number
        random.seed(self.job_id() + ' run')
        score = random.randint(0, 100000)
        print(self.binary)
        print(self.text)
        print(self.out)
        self.sh('{binary} {score} {out}', score=score)
        time.sleep(10)
        assert os.path.isfile('pid')
        while True:
            if int(self.sh('ps aux | awk \'{{print $2}}\' | grep -w `cat pid` | wc -l', True, pipefail=False)) == 1:
                time.sleep(5)
            else:
                break

    def tasks(self):
        yield Task('run', mini_task=True)

