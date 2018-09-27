import os

# Job settings
JOB_OUTPUT = 'output'
JOB_INPUT = 'input'
JOB_LOG = 'log'
JOB_LOG_ENGINE = 'engine'
JOB_SAVE = 'job.save'
JOB_WORK_DIR = 'work'
JOB_FINISHED_MARKER = 'finished'
JOB_LAST_USER = 'last_user'
JOB_FINISHED_ARCHIVE = 'finished.tar.gz'
JOB_INFO = 'info'

# engine path
ENGINE_LOG = 'log'
ENGINE_SUBMIT = 'submit_log'

# Process control logging
PLOGGING_FILE = 'usage'

# base directories
BASE_DIR = os.getcwd()
RECIPE_DIR = "recipe"
WORK_DIR = 'work'

# settings file
GLOBAL_SETTINGS_FILE_DEFAULT = "settings.py"

# File states
STATE_INPUT_PATH = 'input_path'
STATE_INPUT_MISSING = 'input_missing'

# job states
STATE_WAITING = 'waiting'  # Job is waiting for other jobs to finish
STATE_RUNNABLE = 'runnable'  # Job can be started
STATE_INTERRUPTED = 'interrupted'  # task was started, but couldn't finish
STATE_ERROR = 'error'  # Job/task return a non zero return value
STATE_FINISHED = 'finished'  # job/task finished successful

# Job states returnable by the engine
STATE_QUEUE = 'queue'  # Task is waiting in queue
STATE_QUEUE_ERROR = 'queue_error'  # Some thing went wrong in the queue
STATE_RUNNING = 'running'  # Task is currently running
STATE_UNKNOWN = 'unknown'  # Task is not know known by the engine
STATE_RETRY_ERROR = 'retry_error'  # to many failed retries

# commandline parameter
CMD_WORKER = 'worker'  # command to call the worker from commandline
