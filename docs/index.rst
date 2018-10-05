.. Sisyphus documentation master file, created by
   sphinx-quickstart on Tue Feb 27 17:34:49 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Sisyphus's documentation!
====================================

.. toctree::
   :maxdepth: 2


Motivation
==========
The motivation for Sisyphus was to have framework to easily rerun experiments. It should make is simple to come back to an old experiment and see exactly what commands were executed to get the final result.
Sisyphus makes it easy to have a organized way how to share a workflow, e.g. how to setup a complete translation system from start to end or just use parts of it.
This is done by creating a graph which connects outputs of jobs (calculations on some input files given some parameter) with other jobs. The connections between these jobs are either files or simple python objects.

Installation
============
Sisyphus requires a Python 3.5 installation with the following additional libraries:
   - sudo pip3 install psutil
   - sudo pip3 install ipython

  Optional if web interface should be used:
   - sudo pip3 install flask

  Optional to compile documentation:
   - sudo pip3 install Sphinx

  Optional if virtual file system should be used:
   - sudo pip3 install fusepy
   - sudo addgroup $USER fuse  # depending on your system

QuickStart
==========
To run sisyphus you need to setup an experiment folder that contains all needed files (See :ref:`sec-structure`).
An example directory is given in the example folder. To start this toy setup run::

    ../sis manager

you will get something similar to::

    [2018-06-15 16:31:50,488] INFO: Add target result to jobs (used for more informativ output, disable with SHOW_JOB_TARGETS=False)
    [2018-06-15 16:31:50,796] INFO: runnable: Job< workdir: work/parallel/LineSpliter.AVSubx1baWqKyMx35c> <target: result>
    [2018-06-15 16:31:50,796] INFO: runnable(1) waiting(1)
    Print verbose overview (v), start manager (y), or exit (n)?

Start the computation by pressing `y`. You can stop the manager again at any time by pressing CTRL-C.
Sisyphus will show you which processes are currently running. For more information about the processes either check the web interface. It can be started with the http option::

    ../sis manager --http 8080

This will start a local web server at the given port. Visit it by going to http://localhost:8080
Once the final output is computed it will appear in the output folder. In this given example just some random text file.

.. _sec-structure:

Structure
=========
A Sisyphus experiment folder consists mainly of 5 things:
 - the config.py file or config folder
 - the settings.py file
 - the recipes folder
 - the work folder
 - and the output folder (create automatically)

The recipe folder
-----------------
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
The work folder stores all files created during the experiment. This folder should point to a directory with a lot available space. The whole folder could be deleted after an experiment is done since everything can be recomputed, assuming your experiments are deterministic.

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



Job
---
Jobs are the most import objects to understand a sisyphus setup. A job defines a operation which creates a well defines output given the same inputs. The outputs of a job are normally the input to other jobs or defines as output of this sisyphus setup. Sisyphus will automatically figure out which jobs need to be run in which order to created all requested outputs.
If two jobs with the exact same inputs are created sisyphus assumes they are equal since they should produces the same output by definition. They will be grouped together and only run once, this is useful to reduce the number of calculations dramatically.
Each job gets it's own clean work directory to work with and a output directory to place it's finished calculations.
A simple job looks like this::

  class CountVocab(Job):
  
      def __init__(self, text): # takes text as input parameter, all inputs for this job need to be listed in the __init__ function
          self.text = text
          self.out = self.output_path('counts.gz') # the output file of this job
  
      def run(self): # this function will be run by the task, see below
          # the actual bash command, everything placed in {name} will be replaced by property with the same name of this
          # object, e.g. self.name
          self.sh("zcat -f {text} | tr ' ' '\n' | awk 'NF' | sort | uniq -c | sort -g | gzip > {out}")
  
      def tasks(self): # function that will be called to request all tasks from this job, expects a iterable
          # request to run the function 'run', with requirements of 2GB memory and 2 hours of time.
          yield Task('run', rqmt={'mem': 2, 'time': 2})


Task
----
A task defines which functions of a job should be run with which argument and which resources should be requested.
A job can have multiple tasks. All tasks are executed after another. A possible setup with multiple tasks is a setup task, a worker task which is run on multiple computers and a finalize task to collect the results of all worker tasks.

sis command
-----------
Sisyphus is started by running the `sis` command in it's folder.
The main mode of this tool is `sis manager` or short `sis m` it will parses the config directive and will submit the required job to the cluster.
The manager will periodically check which jobs have finished and submits all jobs that became runnable to the cluster as long as it is running.
If you stop the manager (using `Ctrl-C`) no further jobs are submitted, but jobs submitted to the cluster will continue.


FAQ
===

Dryrun a Job without engine
---------------------------
For debugging it is often annoying to submit a job to the engine just to see it crashing shortly after.
If the job directory is already created just run the command that would be executed by Sisyphus manual::

    sis worker work/path/to/job name_of_method

If this is not the case you want to switch into console mode and setup the job::

    sis console

In the console you need to find the job an pass it to tk.run_job to automatically set it up and run it::

    # find and save job:
    In [1]: tk.sis_graph.find('LineSp', mode='job')
    Out[1]: [Job< workdir: work/parallel/LineSpliter.AVSubx1baWqKyMx35c>]
    
    In [2]: j = tk.sis_graph.find('LineSp', mode='job')[0]
    
    # setup and run job:
    In [3]: tk.run_job(j, 'run', 1)
    # If only the job is given it will run the first task:
    In [3]: tk.run_job(j)

Using a relative Path in recipe folder
--------------------------------------

It is nice to have small scripts directly next to the recipe calls. This can be easily achieved by initializing a relative path like this::

  RelPath = tk.setup_path(__package__)

All a `Path` created using `RelPath` will be relative to the current recipe file. 

Remove finished jobs and its descendants
----------------------------------------

You can remove a job with all jobs depending on it from the Sisyphus console using the
:py:meth:`sisyphus.toolkit.remove_job_and_descendants` method.
This is useful if a job definition changed and everything depending on it should be rerun::

    # find and save job:
    In [1]: tk.sis_graph.find('LineSp', mode='job')
    Out[1]: [Job< workdir: work/parallel/LineSpliter.AVSubx1baWqKyMx35c>]

    In [2]: jobs = tk.sis_graph.find('LineSp', mode='job')

    # delete these jobs with all jobs depending on them
    In [3]: tk.remove_job_and_descendants(jobs)

API
===

Toolkit
-------
.. automodule:: sisyphus.toolkit
  :members:

Graph
-----
.. autoclass:: sisyphus.graph.SISGraph
  :members:

Job
---
.. autoclass:: sisyphus.job.Job
  :members:

Task
----
.. autoclass:: sisyphus.task.Task

.. _sec-settings-api:

Settings
--------
.. automodule:: sisyphus.global_settings
  :members:

Engines
-------
.. automodule:: sisyphus.engine
  :members:
.. automodule:: sisyphus.localengine
  :members:
.. automodule:: sisyphus.son_of_grid_engine
  :members:
.. automodule:: sisyphus.load_sharing_facility_engine
  :members:

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

