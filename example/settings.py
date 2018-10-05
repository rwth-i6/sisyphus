
"""
You can overwrite any of the settings from sisyphus.global_settings here.
"""

import sys
import socket


def engine():
    from sisyphus.engine import EngineSelector
    from sisyphus.localengine import LocalEngine
    return LocalEngine(cpu=4, gpu=1, mem=6)
    # Example how to use the engine selector, normally the 'long' engine would be a grid engine e.g. SGE
    # noinspection PyUnreachableCode
    return EngineSelector(engines={'short': LocalEngine(cpu=6, gpu=1),
                                   'long': LocalEngine(cpu=8, gpu=1)},
                          default_engine='long')


WAIT_PERIOD_JOB_FS_SYNC = 1  # finishing a job
WAIT_PERIOD_BETWEEN_CHECKS = 1  # checking for finished jobs
WAIT_PERIOD_CACHE = 1  # stopping to wait for actionable jobs to appear

JOB_AUTO_CLEANUP = False
