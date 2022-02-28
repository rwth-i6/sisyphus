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


recipe folders
--------------

The recipe folder contains python packages and modules containing job definitions and pipeline code.
Currently Sisyphus allows the recipe folder to be used in two different ways:

 #. The recipe folder as a single python package: This means that all imports start with ``from recipe.``,
    and the full name of each job will be based on the package structure **without** the recipe prefix.
 #. The recipe folder as location for different recipe packages: This means that there are individual
    recipe packages that are located in the ``recipe`` folder, and the imports start with the package name,
    e.g. ``from i6_core.`` (see the `i6_core recipes <https://github.com/rwth-i6/i6_core>`_)

Note that for new setups, variant #2 should always be preferred.
If you are using PyCharm to manage a setup, it is also important to mark the recipe folder `as source <https://github.com/rwth-i6/i6_core/wiki/Sisyphus-PyCharm-Setup>`_.

In addition to the jobs, the recipe folder can also contain helper classes and functions that are used as building
blocks for a pipeline. Depending on the personal preference, also full experiments can be defined here, but the experiments
have to be called via code in the **config** folder. See more on this in the next session.

..
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

The config folder should contain the pipeline calls for the specific experiments/workflows in a hierarchical order.
When creating a new setup it is important to have a ``config/__init__.py`` which contains a ``main()`` function.
This will be the global entry point for the graph thread, and should call **ALL** experiments/workflows without any exception.
If this is not the case, some console commands will lead to incorrect behavior or not work at all.

Besides the ``main()`` function, the config folder can contain further packages/modules/functions that define workflow pipelines in the form
of partial Sisyphus graphs.

**Example:**

The config folder has the following structure:

.. code::

  - config
    - __init__.py
    - experiments1.py
    - experiments2.py

``__init__.py:``

.. code:: python

    from config.experiments1 import run_experiments1
    from config.experiments2 import run_experiments2

    def main():
        run_experiments1()
        run_experiments2()

``experiments1.py:``

.. code:: python

    from sisyphus import tk, Path

    from some_recipe_package.some_jobs import Job1, Job2, Job3

    def run_experiment1():
        # define any input
        input = Path("/path/to/some/input")

        # define a sequence of jobs imported from a recipe package
        job1 = Job1(input, param="some_value")
        job2 = Job2(job1.output, param="another_value")
        job3 = Job3(job2.output, param="yet_another_value")

        # register the output
        tk.register_output("experiments1/an_output_file", job3.output)
        return job1.output, job2.output, job3.output

``experiments2.py:``

.. code:: python

    from another_recipe_package.some_pipelines import pipeline1, pipeline2

    def run_experiment2():
        # define some inputs
        input1 = Path("/path/to/some/input")
        input2 = Path("/path/to/another/input")

        # run a pipeline (consisting of a sequence of jobs like in run_experiment1) on different inputs
        output1 = pipeline1(input1)
        output2 = pipeline1(input2)
        tk.register_output("experiments2/pipeline1/output_file1", output1)
        tk.register_output("experiments2/pipeline1/output_file2", output2)

        # run another pipeline on the same input
        output3 = pipeline2(input1)
        tk.register_output("experiments2/pipeline2/output_file1", output3)

When the pipelines are defined this way, a ``./sis m`` call will create the full graph, and run jobs in order to produce all defined outputs.
Now lets say the graph code is already very large, and you only want to run a sub-graph.
With an hierarchical structure, it is then possible to call the manager with a specific function,
e.g. ``./sis m config.experiments2.run_experiment2`` to only build and run the sub-graph for experiment 2.

It is also possible to define asynchronous workflows which allow halting the calculation of the graph to wait until the requested jobs are finished. This allows to easily make the graph dependent on intermediate results. Given the code example above it could work like this:

.. code:: python

    from config.experiments1 import run_experiments1
    from config.experiments2 import run_experiments2

    async def main():
        job1_output, job2_output, job3_output = run_experiments1()
        await tk.async_run(job2_output)  # The workflow will pause here until the output of job2 is available
        if job2_output.get() < some_other_value:  # Assuming Job2 returns a Variable
            run_experiments2()

The pipeline code in both the ``config`` and ``recipe`` folders can be arbitrary complex and structured freely, but it is
important to keep in mind that sub-graph functions always have to be located within the ``config`` folder.


work folder
-----------

The work folder stores all files created during the experiment in the form a folder per created job.
The directory structure will match the package structure below the ``recipe`` folder.
This folder should point to a directory with a lot available space, and is typically a symlink to a location on
a specific file system that is accessable by all cluster machines.
The whole folder could be deleted after an experiment is done since everything can be recomputed, assuming your experiments are deterministic.

settings.py
-----------

Contains all settings that determine the general behavior of Sisyphus with respect to the specific setup.
A required entry is the ``engine`` function that determines the backend job-scheduling engine.
See :ref:`sec_installation` for examples.
A detailed overview of all settings can be found :ref:`here <sec-settings-api>`.


.. code:: python

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
        return LocalEngine(cpu=4, gpu=0, mem=16)

        # Example how to use the engine selector, normally the 'long' engine would be a grid enigne e.g. sge
        from sisyphus.engine import EngineSelector
        from sisyphus.localengine import LocalEngine
        from sisyphus.son_of_grid_engine import SonOfGridEngine
        return EngineSelector(
            engines={'short': LocalEngine(cpu=4),
                     'long': SonOfGridEngine(
                         default_rqmt={'cpu' : 1, 'mem' : 2, 'gpu' : 0, 'time' : 1},
                         gateway="<gateway-machine-name>")}, # a gateway is only needed if the local machine has no SGE installation
            default_engine='long')

    # Wait so long before marking a job as finished to allow network
    # filesystems so synchronize, should be reduced if only the local engine and filesystem is used.
    WAIT_PERIOD_JOB_FS_SYNC = 30

    # How ofter Sisyphus checking for finished jobs
    WAIT_PERIOD_BETWEEN_CHECKS = 30

    # Disable automatic job directory clean up
    JOB_AUTO_CLEANUP = False
