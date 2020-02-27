
"""
These settings can be overwritten via a ``settings.py`` file in the current directory, when ``sis`` is run.
"""

# Author: Jan-Thorsten Peter <peter@cs.rwth-aachen.de>

import sys
import logging
import sisyphus.hash
# noinspection PyUnresolvedReferences
from sisyphus.global_constants import *
# noinspection PyUnresolvedReferences
import os


def engine():
    """
    Create engine object used to submit jobs. The simplest setup just creates a local
    engine starting all jobs on the local machine e.g.::

        from sisyphus.localengine import LocalEngine
        return LocalEngine(cpu=8)

    The usually recommended version is to use a local and a normal grid engine. The EngineSelector
    can be used to schedule tasks on different engines. The main intuition was to have an engine for
    very small jobs that don't required to be scheduled on a large grid engine (e.g. counting lines of file).
    A setup using the EngineSelector would look like this::

        from sisyphus.localengine import LocalEngine
        from sisyphus.engine import EngineSelector
        from sisyphus.son_of_grid_engine import SonOfGridEngine
        return EngineSelector(engines={'short': LocalEngine(cpu=4),
                                       'long': SonOfGridEngine(default_rqmt={'cpu': 1, 'mem': 1,
                                                                             'gpu': 0, 'time': 1})},
                              default_engine='long')

    Note: the engines should only be imported locally inside the function to avoid circular imports

    :return: engine (EngineBase)
    """
    import psutil
    cpu_count = psutil.cpu_count()

    if ENGINE_NOT_SETUP_WARNING:
        logging.info('No custom engine setup, using default engine: LocalEngine(cpu=%i, gpu=0)' % cpu_count)

    from sisyphus.localengine import LocalEngine
    return LocalEngine(cpu=cpu_count, gpu=0)


def update_engine_rqmt(initial_rqmt, last_usage):
    """ Update requirements after a job got interrupted.

    :param dict[str] initial_rqmt: requirements that are requested first
    :param dict[str] last_usage: information about the last usage by the task
    :return: updated requirements
    :rtype: dict[str]
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


# noinspection PyUnusedLocal
def check_engine_limits(current_rqmt, task):
    """ Check if requested requirements break and hardware limits and reduce them.
    By default ignored, a possible check for limits could look like this::

        current_rqmt['time'] = min(168, current_rqmt.get('time', 2))
        if current_rqmt['time'] > 24:
            current_rqmt['mem'] = min(63, current_rqmt['mem'])
        else:
            current_rqmt['mem'] = min(127, current_rqmt['mem'])
        return current_rqmt

    :param dict[str] current_rqmt: requirements currently requested
    :param sisyphus.task.Task task: task that is handled
    :return: requirements updated to engine limits
    :rtype: dict[str]
    """
    return current_rqmt


def file_caching(path):
    """ This function should be replaced to enable file caching.
    e.g. copy given file to /var/tmp and return new path.
    The default behaviour is to just pass on the given path

    :param str path: Path to file that should be cached
    :return: path to cached file
    :rtype: str
    """
    logging.info('No file caching function set, simply keep given path: %s' % path)
    return path


# Experimental settings
# Log when a job output was used the last time, currently not active maintained
ENABLE_LAST_USAGE = False
# Add tags attached to job to work path, currently not active maintained
JOB_USE_TAGS_IN_PATH = False
# Start kernel to connect remotely, currently not active maintained
START_KERNEL = False
# Link all computed outputs to this directory for easy sharing in a team
TEAM_SHARE_DIR = None  # If set results will be linked to this directory

#: Automatically clean up job directory after job has finished
JOB_AUTO_CLEANUP = True
#: How often to check for finished jobs in seconds
JOB_CLEANER_INTERVAL = 60
#: How many threads should be cleaning in parallel
JOB_CLEANER_WORKER = 5
#: If the job internal work directory should be keeped re deleted during clean up
JOB_CLEANUP_KEEP_WORK = False
#: Default value for job used by tk.cleaner to determine if a job should be removed or not
JOB_DEFAULT_KEEP_VALUE = 50
#: How many threads should update the graph in parallel, useful if the filesystem has a high latency
GRAPH_WORKER = 16

#: How many threads are used to setup the job directory and submit jobs
MANAGER_SUBMIT_WORKER = 10

#: How many locks can be used by all jobs (one lock per job). If there are more jobs than locks, locks are reused
#: This could lead to a slowdown, but the number of locks per process is limited
JOB_MAX_NUMBER_OF_LOCKS = 100

#: How often sisyphus will try to resubmit a task to the engine before returning a RETRY_ERROR
MAX_SUBMIT_RETRIES = 3

#: Default function to hash jobs and objects
SIS_HASH = sisyphus.hash.short_hash

#: Path to the config directory, not including the directory name 'config'
CONFIG_PATH = '.'

#: Path to the recipe directory, not including the directory name 'recipe'
RECIPE_PATH = '.'

#: The work directory
WORK_DIR = 'work'

# Name default config file if no config directory is found
CONFIG_FILE_DEFAULT = "config.py"

#: Name of default fuction to call in config directory
CONFIG_FUNCTION_DEFAULT = "%s.main" % CONFIG_PREFIX

#: Name alias directory
ALIAS_DIR = 'alias'
#: Name output directory
OUTPUT_DIR = 'output'

#: If set to a non-empty string aliases and outputs will be placed in a subdir.
#: This is useful for setups with multiple configs
ALIAS_AND_OUTPUT_SUBDIR = ''

#: Show job targets on status screen, can significantly slow down startup time if many outputs are used
SHOW_JOB_TARGETS = True

#: How many seconds should be waited before assuming a job is finished after the finished file is written
#: to allow network file system to sync up
WAIT_PERIOD_JOB_FS_SYNC = 30
#: How often should the manager check for finished jobs
WAIT_PERIOD_BETWEEN_CHECKS = 30
#: Safety period to wait for actionable jobs to change status before running action
WAIT_PERIOD_CACHE = 20
#: How many seconds should be waited before retrying a ssh connection
WAIT_PERIOD_SSH_TIMEOUT = 15
#: How many seconds should be waited before retrying to parse a failed qstat output
WAIT_PERIOD_QSTAT_PARSING = 15
#: How many seconds should be waited before retrying to bind to the desired port
WAIT_PERIOD_HTTP_RETRY_BIND = 10
#: How many seconds should be waited before cleaning up a finished job
WAIT_PERIOD_JOB_CLEANUP = 10
#: How many seconds should all inputs be available before starting a job to avoid file system synchronization problems
WAIT_PERIOD_MTIME_OF_INPUTS = 60

#: set true to automatically clean jobs in error state and retry
CLEAR_ERROR = False

#: Print error messages of a job in the manager status field
PRINT_ERROR = True
#: Print detailed log of that many jobs in error state
PRINT_ERROR_TASKS = 1
#: Print that many last lines of error state log file
PRINT_ERROR_LINES = 40

#: Which command should be called to start sisyphus, can be used to replace the python binary
SIS_COMMAND = [sys.executable, sys.argv[0]]
# if this first argument is -m it's missing the module name
if sys.argv[0] == '-m':
    SIS_COMMAND += ['sisyphus']

# Parameter to log used resources by each task
#: Seconds between checks how much memory and cpu a process is using
PLOGGING_INTERVAL = 5
#: Suppress messages about process resources usage
PLOGGING_QUIET = False
#: Minimal relative change between log entries of used resources
PLOGGING_MIN_CHANGE = 0.1
#: In which interval the process used resources file should be updated
PLOGGING_UPDATE_FILE_PERIOD = 60

#: How long the virtual file system should cache process states
FILESYSTEM_CACHE_TIME = 30

#: Use ipython traceback
USE_VERBOSE_TRACEBACK = True
#: The verbose traceback type. "ipython" or "better_exchook"
VERBOSE_TRACEBACK_TYPE = "ipython"

#: Install signal handlers for debugging
USE_SIGNAL_HANDLERS = False

# Job setup options
#: Automatically set all input given to __init__  as attributes of the created job.
#: Disabled by default since it tends to confuse new users reading the code.
AUTO_SET_JOB_INIT_ATTRIBUTES = False

# Job environment
#: Remove all environment variables to ensure the same environment between different users
CLEANUP_ENVIRONMENT = True  # only Trump would say no!
#: Keep these environment variables if CLEANUP_ENVIRONMENT is set
DEFAULT_ENVIRONMENT_KEEP = {'CUDA_VISIBLE_DEVICES', 'HOME', 'PWD', 'SGE_STDERR_PATH', 'SGE_TASK_ID', 'TMP', 'TMPDIR',
                            'USER'}
#: Set these environment variables if CLEANUP_ENVIRONMENT is set
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
#: Directory used by tk.mktemp
TMP_PREFIX = os.path.join(os.environ.get('TMPDIR', '/tmp'), 'sis_')

# Visualization
#: For http visualization, list job input as common input if it is share between more then X*(total jobs) jobs
VIS_RELATIVE_MERGE_THRESHOLD = 0.25
#: For http visualization, list job input as common input if it is share between more then X jobs
VIS_ABSOLUTE_MERGE_THRESHOLD = 5
#: For http visualization, time out to create visual representation
VIS_TIMEOUT = 5
#: For http visualization, maximum number of nodes to show per view
VIS_MAX_NODES_PER_VIEW = 500

SHOW_VIS_NAME_IN_MANAGER = True

# Add stacktrace information with specified depth, 0 for deactivation, None or -1 for full stack
JOB_ADD_STACKTRACE_WITH_DEPTH = 0

# Is enabled if tk.run is called
SKIP_IS_FINISHED_TIMEOUT = False


# Internal functions
def update_global_settings_from_text(text, filename):
    """
    :param text:
    :param str filename:
    :return: nothing
    """
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


def update_global_settings_from_file(filename):
    """
    :param str filename:
    :return: nothing
    """
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
    update_global_settings_from_text(content, filename)


GLOBAL_SETTINGS_COMMANDLINE = []
ENVIRONMENT_SETTINGS = {}
ENVIRONMENT_SETTINGS_PREFIX = 'SIS_'


def update_global_settings_from_env():
    """
    :return: nothing
    """
    from ast import literal_eval
    for k, v in os.environ.items():
        if k.startswith(ENVIRONMENT_SETTINGS_PREFIX):
            ENVIRONMENT_SETTINGS[k] = v
            k = k[len(ENVIRONMENT_SETTINGS_PREFIX):]
            # Try to eval parameter, if not possible use as string
            try:
                v = literal_eval(v)
            except Exception:
                pass
            globals()[k] = v


# Parameter used for debugging or profiling
MEMORY_PROFILE_LOG = None

USE_UI = True

# Set to False to disable Warning of unset engine
ENGINE_NOT_SETUP_WARNING = True

update_global_settings_from_env()
update_global_settings_from_file(GLOBAL_SETTINGS_FILE_DEFAULT)
