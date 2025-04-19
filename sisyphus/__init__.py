import sys
from .job import Job
from .task import Task
import sisyphus.toolkit as tk
import sisyphus.runtime as rt
from sisyphus.loader import RecipeFinder

# setup_path and gs will be removed in the future since they are both accessible via tk
gs = tk.gs
setup_path = tk.setup_path
Path = tk.Path
__all__ = ["Job", "Task", "Path", "gs", "tk", "setup_path", "rt"]


# Setup recipe importer
if RecipeFinder not in sys.meta_path:
    sys.meta_path.append(RecipeFinder)
