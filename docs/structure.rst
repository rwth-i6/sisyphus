.. _sec-structure:

=========
Structure
=========

A Sisyphus experiment folder consists mainly of 4 things:
 * the ``config`` folder, containing graph definition code
 * the ``recipe`` folder, containing Job definition and pipeline code
 * the ``settings.py`` file, defines sisyphus parameters and the used engine
 * the ``work`` folder, contains the actual data in form of structured "Job" folders

When running Sisyphus, two additional folders will be added and filled automatically:
 * the ``alias`` folder, containing human readable symlinks to Job folders
 * the ``output`` folder, containing symlinks to output files from jobs


recipe folder
-------------

The recipe folder contains python files which describe what commands are executed in which order and how they are linked together.
A typical file starts with this line::

  from sisyphus import *


which setup the sisyphus environment by importing/creating:
 - Job, this is the base class for all Jobs created in a sisyphus setup. A job takes some parameter and files as input and creates some other files as output. It represents a node in our workflow graph.
 - Task, these are the subelements of a Job. Each job runs one or more Tasks to create it's actual outputs.
 - Path, used to reference to files directly. Path object are also created as outputs of Jobs. They can be seen as edges in the workflow graph.
 - tk, short for toolkit. Contains commands to communicate with sisyphus

A workflow in a python file is now created by connecting these jobs together via a path object.
This is usually done by a function which serves as template.
Outputs of the workflow graph a registered at sisyphus via the ```tk.register_output('name', path)``` function.
These files will be linked to the output folder after the responsible job to create this file finished.

config folder
-------------
The config folder contains the description which experiments should be run. e.g.::

  from sisyphus import *

  from recipe import tools
  head = tools.Head(Path('input/file')).out
  tk.register_output('head_of_input_file', head)

This imports the module tools from the recipe folder and runs the job Head with a given input file and registers the result as output.

work folder
-----------

The work folder stores all files created during the experiment.
This folder should point to a directory with a lot available space.
The whole folder could be deleted after an experiment is done since everything can be recomputed, assuming your experiments are deterministic.

settings.py
-----------
Contains all settings that should be changed for the whole setup globally.
Usually a description of the work engine that should be used. You can probably just copy the last one you used.
A detailed overview of all settings can be found :ref:`here <sec-settings-api>`.
Example::

    def engine():
        """ Create engine object used to submit jobs. The simplest setup just creates a local
        engine starting all jobs on the local machine e.g.:

            from sisyphus.localengine import LocalEngine
            return LocalEngine(max_cpu=8)

        The usually recommended version is to use a local and a normal grid engine. The EngineSelector
        can be used to schedule tasks on different engines. The main intuition was to have an engine for
        very small jobs that don't required to be scheduled on a large grid engine (e.g. counting lines of file).

        Note: the engines should only be imported locally inside the function to avoid circular imports

        :return: engine
        """
        # Exmple of local engine:
        from sisyphus.localengine import LocalEngine
        return LocalEngine(cpu=4)

        # Example how to use the engine selector, normally the 'long' engine would be a grid enigne e.g. sge
        from sisyphus.engine import EngineSelector
        from sisyphus.son_of_grid_engine import SonOfGridEngine
        return EngineSelector(engines={'short': LocalEngine(cpu=8),
                                   'long': SonOfGridEngine(default_rqmt={'cpu' : 1, 'mem' : '1G', 'gpu' : 0, 'time' : 1, })},
                          default_engine='long')

    # Wait so long before marking a job as finished to allow network
    # filesystems so synchronize, should be reduced if only the local engine and filesystem is used.
    WAIT_PERIOD_JOB_FS_SYNC = 30

    # How ofter Sisyphus checking for finished jobs
    WAIT_PERIOD_BETWEEN_CHECKS = 30

    # Disable automatic job directory clean up
    JOB_AUTO_CLEANUP = False
