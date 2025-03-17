"""
Provides the sisyphus runtime interface, which can be used to interact with the
currently running job and the scheduler.

The runtime interface functions are only callable from inside the worker context.
"""

from typing import List

from sisyphus.engine import EngineBase
import sisyphus.global_settings as gs
import sisyphus.toolkit as tk


def get_job_node_hostnames() -> List[str]:
    """
    Returns the list of node hostnames the currently running job is executing on.

    Most of the time a job will only execute on a single node.
    Certain scenaria like multi-node multi-GPU training, however, may also use multiple nodes
    to execute a single job.
    """
    return _get_engine().get_job_node_hostnames()


def _get_engine() -> EngineBase:
    """Returns the currently active Engine."""
    assert tk._sis_running_in_worker, (
        "This function can only be called from a running job."
    )
    eng = gs.active_engine
    assert eng is not None
    return eng
