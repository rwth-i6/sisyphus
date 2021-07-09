==========
Motivation
==========

The motivation for Sisyphus was to have framework to easily rerun experiments.
It should make is simple to come back to an old experiment and see exactly what commands were executed to get the final result.
Sisyphus makes it easy to have a organized way how to share a workflow, e.g. how to setup a complete translation system from start to end or just use parts of it.
This is done by creating a graph which connects outputs of jobs (calculations on some input files given some parameter) with other jobs.
The connections between these jobs are either files or simple python objects.

========
Concepts
========

Sisyphus uses an `directed acyclic graph<https://en.wikipedia.org/wiki/Directed_acyclic_graph>`_ to define an experiment workflow.
The "nodes" of the graph are "Jobs" that are executed on a local machine or in a cluster environments and the edges are physical "files"
that are passed between jobs.
This means that all edges leading into the node are the input files to a job, and the outgoing edges are files that the job
did produce which are passed on to the following jobs.
In addition to the edges, a job can get "constant" inputs, which are usually fixed parameters that further specify what a certain
job should do with the input files.
Each job (instance, not template) in the graph is unique,
which means that if you try to create a job with the same input files (incoming edges) and the same static parameters as an existing job,
no new job will be created, but the existing one is re-used.
Sisyphus uses a hash function over the inputs and static parameters to generate the unique identifier for each job.

It is important to remember that all edges are (physical!) files, without any exception.
In case that e.g. python variables are passed from one job to another, they are stored as file and then open again in the following job.

A job is divided into tasks, as in order to produce specific output file there can be multiple steps involved.
Given that all inputs to a job are ready, Sisyphus will try to execute the tasks of a job in the order they are specified,
by submitting each task to a "job-scheduling-engine", which can be e.g. SGE, slurm or just the local machine.
Specifying multiple tasks is for example needed if you want to call an external software, and you need to do preparations.
Then there can be a task ``create_config`` which is executed locally and prepares the needed files,
and a task ``run`` which calls the software, and is submitted to e.g. a multi-GPU machine when the software needs a lot of resources.

The Sisyphus binary contains three modes: **manager**, **worker** and **console**.
The manager is responsible for creating the internal graph, creating the job folders and submitting tasks to the selected job-scheduling engine.
It also shows the current status of all running, crashed or waiting jobs and tasks.
The worker is called to execute a task on the target cluster machine, and calls the task function of a job class.
The console opens an IPython session and is used to directly interact with the Sisyphus API.


Components Overview
-------------------

 - **Graph:** The Sisyphus graph consists of Job instances as nodes, and is constructed from the outputs towards the inputs (directed, acyclic graph).
 - **Job (class/instance):** Jobs definitions (a Python class inheriting from `sisyphus.Job`) are the templates for graph nodes, and are defined by their name and their input parameters. Each job is intended to define some operation on the inputs, and store the results as “output” files. A job instance is uniquely defined by its hash. The hash of a job is defined by its name, and the combined hash of the inputs.
 - **Job (physical):** When a Job with a certain set of inputs is added to the graph, and all the inputs are “available” (e.g. for Paths the file got created and its creator Job is finished), the Job is “physically” created as a folder under the work folder.
 - **Task:** A task is a python function of a Job that is defining the actual functionality of the job. This code will only run if the Job is executed, and is usually submitted to a cluster system such as SGE or Slurm. Small tasks (e.g. creating config files) are usually set to run on the same machine as the sisyphus manager.
 - **Path:** Paths are the “edges” of the graph, and define a physical file that is passed from one Job to another. The hash of a Path is defined by its “creator” (= Job) and the file path. For Paths created by jobs, the file path is defined as relative to the job root. Please note that folders can also be a valid path, not only files.
 - **Variable:** Variables are a variant of Paths where the file content is representing a python variable (e.g. int/float), and behave in the same way. The content has to be read/set via get and set functions.

Manager
-------

The Sispyhus manager (``sis manager`` or ``sis m`` in short) is the "heart" of Sisyphus, and the part that the user usually interacts with.
When starting the manager it will construct the graph calling a python function in the ``config``folder,
usually a ``main`` function in  ``config/__init`` (see :ref:`sec-structure` for details on the setup structure).
All jobs instances that are created when calling running the config code are stored, and added to the graph
when they are needed to produce a certain output (= a Path object registered as output).
The manager will then display which Jobs are ready to be executed.
If the manager is selected to start running, it will submit all jobs that are ready into the queue of the selected
job-scheduling engine, and display which jobs are in queue, running, interrupted or crashed.
Within a specific time intervall the manager will poll for the status of all jobs, and will continue to
submit/run new jobs as soon as all inputs are existing in the file system.

