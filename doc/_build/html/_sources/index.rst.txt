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
A typical file starts with the two lines::

  from sisyphus import *
  Path = tk.setup_path(__package__)

which setup the sisyphus environment by importing/creating:
 - Job, this is the base class for all Jobs created in a sisyphus setup. A job takes some parameter and files as input and creates some other files as output. It represents a node in our workflow graph.
 - Task, these are the subelements of a Job. Each job runs one or more Tasks to create it's actual outputs.
 - tk, short for toolkit. Contains commands to communicate with sisyphus
 - Path, used to reference to files relative to the path of this python file. This is used to link to scripts that are stored next to the recipe files. Path object are also created as outputs of Jobs. They can be interpreted as edges in the workflow graph.

A workflow in a python file is now created by connecting these jobs together via a path object.
This is usually done by a function which serves as template.
Outputs of the workflow graph a registered at sisyphus via the ```tk.register_output('name', path)``` function. 
These files will be linked to the output/ folder after the responsible job to create this file finished.

config.py or config folder
--------------------------
The config.py file contains the description which experiments should be run. e.g.::

  from sisyphus import *

  from recipe import tools
  head = tools.Head(Path('input/file')).out 
  tk.register_output('head_of_input_file', head)

this imports the module tools from the recipe folder and runs the job Head with a given input file and registers the result as output.

work folder
-----------
The work folder stores all files created during the experiment. This folder should point to a directory with a lot available space. The whole folder could be deleted after an experiment is done since everything can be recomputed, assuming your experiments are deterministic.

settings.py
-----------
Contains all settings that should be changed for the whole setup globally. Usually a description of the work engine that should be used. You can probably just copy the last one you used.


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

sis.sh
------
The /u/smt/tools/sisyphus/2015-07-28/sis.sh script is a simple bash script wrapper to ensure that the correct python version is used.
The main mode of this tool is `sis.sh manager` it will parses the config.py file and will submit them to the cluster if started with the `-r` flag (for run). The manager will periodically check which jobs have finished and submits all runnable jobs to the cluster as long as it is running. If you stop the manager (using Ctrl-C) now further jobs are submitted, but jobs submitted to the cluster will continue.


FAQ
===

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

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

