# Author: Jan-Thorsten Peter <peter@cs.rwth-aachen.de>

import os
import sys
import psutil
import logging
import sisyphus.hash
from sisyphus.global_constants import *


def engine():
    """ Create engine object used to submit jobs. The simplest setup just creates a local
    engine starting all jobs on the local machine e.g.:

        from sisyphus.localengine import LocalEngine
        return LocalEngine(max_cpu=8)

    The usually recommended version is to use a local and a normal grid engine. The EngineSelector
    can be used to schedule tasks on different engines. The main intuition was to have an engine for
    very small jobs that don't required to be scheduled on a large grid engine (e.g. counting lines of file).
    A setup using the EngineSelector would look like this:

        from sisyphus.localengine import LocalEngine
        from sisyphus.engine import EngineSelector
        from sisyphus.son_of_grid_engine import SonOfGridEngine
        return EngineSelector(engines={'short': LocalEngine(cpus=4),
                                       'long': SonOfGridEngine(default_rqmt={'cpu': 1, 'mem': 1,
                                                                             'gpu': 0, 'time': 1})},
                              default_engine='long')

    Note: the engines should only be imported locally inside the function to avoid circular imports

    :return: engine
    """
    cpu_count = psutil.cpu_count()
    logging.info('No custom engine setup, using default engine: LocalEngine(cpus=%i, gpus=0)' % cpu_count)
    from sisyphus.localengine import LocalEngine
    return LocalEngine(cpus=cpu_count, gpus=0)


def update_engine_rqmt(initial_rqmt, last_usage):
    """ Update rqmts after a job got interrupted.

    :param first_rqmt:
    :param last_usage:
    :return:
    """

    # Contains the resources requested for interrupted run
    requested_resources = last_usage.get('requested_resources', {})
    requested_time = requested_resources.get('time', initial_rqmt.get('time', 1))
    requested_memory = requested_resources.get('mem', initial_rqmt.get('mem', 1))

    # How much was actually used
    used_time = last_usage.get('used_time', 0)
    used_memory = last_usage.get('max', {}).get('rss', 0)

    # Did it (nearly) break the limits?
    out_of_memory = last_usage.get('out_of_memory') or requested_memory - used_memory < 0.25
    out_of_time = requested_time - used_time < 0.1

    # Double limits if needed
    if out_of_time:
        requested_time = max(initial_rqmt.get('time', 0), requested_time * 2)

    if out_of_memory:
        requested_memory = max(initial_rqmt.get('mem', 0), requested_memory * 2)

    # create updated rqmt dict
    out = initial_rqmt.copy()
    out.update(requested_resources)
    out['time'] = requested_time
    out['mem'] = requested_memory

    return out


def check_engine_limits(current_rqmt, task):
    """ Check if requested requirements break and hardware limits

    :param current_rqmt:
    :param task:
    :return:
    """
    current_rqmt['time'] = min(168, current_rqmt.get('time', 2))
    if current_rqmt['time'] > 24:
        current_rqmt['mem'] = min(63, current_rqmt['mem'])
    else:
        current_rqmt['mem'] = min(127, current_rqmt['mem'])
    return current_rqmt


def file_caching(path):
    """ This function should be replaced to enable file caching.
    e.g. copy given file to /var/tmp and return new path.
    The default behaviour is to just pass on the given path

    :param path(str): Path to file that should be cached
    :return: path to cached file
    """
    logging.info('No file caching function set, simply keep given path: %s' % path)
    return path

ENABLE_LAST_USAGE = False
JOB_AUTO_CLEANUP = True
JOB_CLEANER_INTERVAL = 60
JOB_CLEANER_WORKER = 5
JOB_CLEANUP_KEEP_WORK = False
JOB_USE_TAGS_IN_PATH = False
START_KERNEL = False
JOB_DEFAULT_KEEP_VALUE = 50
GRAPH_WORKER = 16

MANAGER_SUBMIT_WORKER = 10

# default function to hash objects
SIS_HASH = sisyphus.hash.short_hash

# recipe settings
CONFIG_DIR = "config"
CONFIG_FILE_DEFAULT = "config.py"
CONFIG_FUNCTION_DEFAULT = "%s.main" % CONFIG_DIR

# alias & output settings
ALIAS_DIR = 'alias'
OUTPUT_DIR = 'output'

# if set to a non-empty string aliases and outputs will be placed in a subdir
# this is useful for setups with multiple configs
ALIAS_AND_OUTPUT_SUBDIR = ''

# Show job targets on status screen
SHOW_JOB_TARGETS = True

# Team share settings
TEAM_SHARE_DIR = None  # If set results will be linked to this directory

# how many seconds should be waited before ...
WAIT_PERIOD_JOB_FS_SYNC = 30  # finishing a job
WAIT_PERIOD_BETWEEN_CHECKS = 30  # checking for finished jobs
WAIT_PERIOD_CACHE = 20  # stoping to wait for actionable jobs to appear
WAIT_PERIOD_SSH_TIMEOUT = 15  # retrying ssh connection
WAIT_PERIOD_QSTAT_PARSING = 15  # retrying to parse qstat output
WAIT_PERIOD_HTTP_RETRY_BIND = 10  # retrying to bind to the desired port
WAIT_PERIOD_JOB_CLEANUP = 10  # cleaning up a job
WAIT_PERIOD_MTIME_OF_INPUTS = 60  # wait X seconds long before starting a job to avoid file system sync problems

CLEAR_ERROR = False  # set true to automatically clean jobs in error state
PRINT_ERROR_TASKS = 1
PRINT_ERROR_LINES = 40

SIS_COMMAND = [sys.argv[0]]

# Process control logging
PLOGGING_INTERVAL = 5  # Seconds between memory checks
PLOGGING_QUIET = False  # Suppress process control messages
PLOGGING_MIN_CHANGE = 0.1  # Minimal relative change between log entries
PLOGGING_UPDATE_FILE_PERIOD = 60  # How often the a worker updates it's logging file

FILESYSTEM_CACHE_TIME = 30

USE_VERBOSE_TRACEBACK = True

# Job setup options
AUTO_SET_JOB_INIT_ATTRIBUTES = False
LOG_TRACEBACKS = False


# Job environment
CLEANUP_ENVIRONMENT = True  # only Trump would say no!
DEFAULT_ENVIRONMENT_KEEP = set(['CUDA_VISIBLE_DEVICES', 'HOME', 'PWD', 'SGE_STDERR_PATH', 'SGE_TASK_ID', 'TMP',
                                'TMPDIR', 'USER'])
DEFAULT_ENVIRONMENT_SET = {'LANG': 'en_US.UTF-8',
                           'MKL_NUM_THREADS': 1,
                           'OMP_NUM_THREADS': 1,
                           'PATH': ':'.join(['/rbi/sge/bin', '/rbi/sge/bin/lx-amd64',
                                             '/usr/local/sbin', '/usr/local/bin',
                                             '/usr/sbin', '/usr/bin',
                                             '/sbin', '/bin',
                                             '/usr/games', '/usr/local/games',
                                             '/snap/bin']),
                           'SHELL': '/bin/bash'}

# Visualization
VIS_RELATIVE_MERGE_THRESHOLD = 0.25
VIS_ABSOLUTE_MERGE_THRESHOLD = 5


def update_gloabal_settings_from_text(text, filename):
    # create basically empty globals
    globals_ = {
        '__builtins__': globals()['__builtins__'],
        '__file__': filename,
        '__name__': filename,
        '__package__': None,
        '__doc__': None,
    }
    globals_keys = list(globals_.keys())

    # it might be useful to change the default environment by modifying the existing set/dict
    # thus we need to add it to the globals, but not to globals_keys
    globals_['DEFAULT_ENVIRONMENT_KEEP'] = DEFAULT_ENVIRONMENT_KEEP
    globals_['DEFAULT_ENVIRONMENT_SET'] = DEFAULT_ENVIRONMENT_SET

    # compile is needed for a nice trace back
    exec(compile(text, filename, "exec"), globals_)

    for k, v in globals_.items():
        if k not in globals_keys:
            globals()[k] = v

    if AUTO_SET_JOB_INIT_ATTRIBUTES:
        import logging
        logging.warning('AUTO_SET_JOB_INIT_ATTRIBUTES is deprecated, please set the attributes manually '
                        'you might want to use self.set_attrs(locals())')


def update_gloabal_settings_from_file(filename):
    # skip if settings file doesn't exist
    globals()['GLOBAL_SETTINGS_FILE'] = filename

    content = ''
    try:
        with open(filename, encoding='utf-8') as f:
            content = f.read()
    except IOError as e:
        if e.errno != 2:
            raise e

    globals()['GLOBAL_SETTINGS_FILE_CONTENT'] = content
    update_gloabal_settings_from_text(content, filename)


def update_gloabal_settings_from_list(settings_list):
    # skip if no options are given is empty
    content = '\n'.join(settings_list)
    globals()['GLOBAL_SETTINGS_COMMANDLINE'] = content
    if settings_list:
        update_gloabal_settings_from_text(content, 'COMMANDLINE_SETTINGS')


def cached_engine(cache=[]):
    """ Returns a cached version, for internal usage """
    if not cache:
        cache.append(engine())
    return cache[0]
