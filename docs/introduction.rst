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


Components Overview
-------------------


 - **Graph:** The Sisyphus graph consists of Job instances as nodes, and is constructed from the outputs towards the inputs (directed, acyclic graph).
 - **Job (class/instance):** Jobs definitions are the templates for graph nodes, and are defined by their name and their input parameters. Each job is intended to define some operation on the inputs, and store the results as “output” files. A job instance is uniquely defined by its hash. The hash of a job is defined by its name, and the combined hash of the inputs.
 - **Job (physical):** When a Job with a certain set of inputs is added to the graph, and all the inputs are “available” (e.g. for Paths the file got created and its creator Job is finished), the Job is “physically” created as a folder under the work folder.
 - **Task:** A task is a python function of a Job that is defining the actual functionality of the job. This code will only run if the Job is executed, and is usually submitted to a cluster system such as SGE or Slurm. Small tasks (e.g. creating config files) are usually set to run on the same machine as the sisyphus manager.
 - **Path:** Paths are the “edges” of the graph, and define a physical file that is passed from one Job to another. The hash of a Path is defined by its “creator” (= Job) and the file path. For Paths created by jobs, the file path is defined as relative to the job root. Please note that folders can also be a valid path, not only files.
 - **Variable:** Variables are a variant of Paths where the file content is representing a python variable (e.g. int/float), and behave in the same way. The content has to be read/set via get and set functions.
