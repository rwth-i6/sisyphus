==========
Components
==========

Job
---

Jobs are the most import objects to understand a sisyphus setup.
A job defines a operation which creates a well defines output given the same inputs.
The outputs of a job are normally the input to other jobs or defines as output of this sisyphus setup.
Sisyphus will automatically figure out which jobs need to be run in which order to created all requested outputs.
If two jobs with the exact same inputs are created sisyphus assumes they are equal since they should produces the same output by definition.
They will be grouped together and only run once, this is useful to reduce the number of calculations dramatically.
Each job gets it's own clean work directory to work with and a output directory to place it's finished calculations.
A simple job looks like this::

  class CountVocab(Job):

      def __init__(self, text): # takes text as input parameter, all inputs for this job need to be listed in the __init__ function
          self.text = text
          self.out = self.output_path('counts.gz') # the output file of this job

      def tasks(self): # function that will be called to request all tasks from this job, expects a iterable
          # request to run the function 'run', with requirements of 2GB memory and 2 hours of time.
          yield Task('run', rqmt={'mem': 2, 'time': 2})

      def run(self): # this function will be run by the task, see below
          # the actual bash command, everything placed in {name} will be replaced by property with the same name of this
          # object, e.g. self.name
          self.sh("zcat -f {text} | tr ' ' '\n' | awk 'NF' | sort | uniq -c | sort -g | gzip > {out}")



Task
----

A task defines which functions of a job should be run with which argument and which resources should be requested.
A job can have multiple tasks.
All tasks are executed after another.
A possible setup with multiple tasks is a setup task, a worker task which is run on multiple computers and a finalize task to collect the results of all worker tasks.


Path
----

The `Path` object is the most common type of "edge" that is passed between Jobs.
A Path object usually contains:

 - The **creator**, which is the Job that created the Path, and can be considered the "origin" of the edge.
 - The relative file location, meaning the relative path from the folder of the creator to the file.
   In case no creator is give, this can be an absolute path, which means that this Path object is an "input" edge into
   the Sisyphus graph.

Note that you should NEVER try to access the content of a Path object outside of a task function in the "manager" thread for any other reason than debugging, meaning to display the current content.
This will always lead to inconsistent behavior within the Sisyphus graph.


Variable
--------

A variable is similar to a Path, with the difference is that it acts as an interface to directly read or write python
variables into the file via ``Variable.get`` and ``Variable.set``.
Same as for the Path object, ``.get`` and ``.set`` should never be used outside of task functions.



