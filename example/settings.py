import sys
import socket

def engine():
    from sisyphus.engine import EngineSelector
    from sisyphus.localengine import LocalEngine
    return LocalEngine(cpus=4)
    # Example how to use the engine selector, normally the 'long' engine would be a grid enigne e.g. sge
    return EngineSelector(engines={'short': LocalEngine(cpus=4, gpus=0),
                                   'long': LocalEngine(cpus=8, gpus=1)},
                          default_engine='long')


WAIT_PERIOD_JOB_FS_SYNC = 1  # finishing a job
WAIT_PERIOD_BETWEEN_CHECKS = 1  # checking for finished jobs
WAIT_PERIOD_CACHE = 1  # stoping to wait for actionable jobs to appear

JOB_USE_TAGS_IN_PATH = False

JOB_AUTO_CLEANUP = False
START_KERNEL = False
