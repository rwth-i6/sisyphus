from sisyphus import *
import os
from typing import Iterable, List

# All paths created by RelPath will be relative to the current directory
# RelPath('splitter.py') points therefore at the splitter.py file inside this directory
RelPath = setup_path(__package__)


class ParagraphSplitter(Job):
    def __init__(self, text: tk.Path, splitter: tk.Path = RelPath('splitter.py')):
        assert text
        assert isinstance(text, tk.Path)

        self.text = text
        self.splitter = splitter
        self.out_prefix = 'splitted.'
        self.splitted_dir = self.output_path('out', True)

    # It's unclear how many outputs will be created by this job
    # A way around it is to compute all output paths after this job has finished
    async def outputs(self) -> List[tk.Path]:
        await tk.async_run(self.splitted_dir)
        out = []
        for path in sorted(os.listdir(str(self.splitted_dir))):
            if path.startswith(self.out_prefix):
                out.append(self.output_path('out/' + path))
        return out

    def run(self):
        self.sh('cat {text} | {splitter} {splitted_dir}/{out_prefix}')

    def tasks(self) -> Iterable[Task]:
        yield Task('run', 'run')
