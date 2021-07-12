.. _sec_installation:

==================
Installing with...
==================

Sisyphus needs to be used with a job-scheduling systems in order to run Tasks of a Job.
Currently supported "Job-Engines" by Sisyphus are:

- local engine: A built-in engine to run tasks on the same machine as the manager, supports only a single GPU
- `Slurm <https://slurm.schedmd.com>`_: A common open source job-scheduling system used for managing cluster systems
- `Son of Grid Engine <https://arc.liv.ac.uk/trac/SGE>`_: Open implementation of the propretriary cluster management system Sun Grid Engine,
  succeeded by Oracle Grid Engine and `Open Grid Engine <http://gridscheduler.sourceforge.net>`_.

Independent of the used scheduler engine, Sisyphus requires a Python 3.5 installation with the following additional libraries:

- ``pip3 install psutil``
- ``pip3 install ipython``

Optional if web interface should be used:

- ``pip3 install flask``

Optional to compile documentation:

- ``pip3 install sphinx``
- ``pip3 install sphinx_rtd_theme``
- ``pip3 install sphinxcontrib-mermaid``

Optional if virtual file system should be used:

- ``pip3 install fusepy``
- ``sudo addgroup $USER fuse  # depending on your system``


===============
...Local Engine
===============

The local engine is a built-in job-scheduler engine that can be used to submit jobs to the local machine,
meaning the machine that the Sisyphus manager is executed from.
The local engine can be setup by adding the following code into the ``settings.py``:

.. code:: python

    def engine():
        from sisyphus.localengine import LocalEngine
        return LocalEngine(cpu=4, gpu=0, mem=16)

Currently, the task requirements will always default to 1 cpu, 1Gb of memory and 1h of execution time unless not specified otherwise.
While more than one gpu can be specified, this is currently not supported and may lead to conflicts.

========
...Slurm
========


The usage of a Slurm backend can be enabled by inserting the following code into the ``settings.py``:

.. code:: python

    def engine():
        from sisyphus.engine import EngineSelector
        from sisyphus.localengine import LocalEngine
        from sisyphus.simple_linux_utility_for_resource_management_engine import SimpleLinuxUtilityForResourceManagementEngine
        return EngineSelector(
            engines={'short': LocalEngine(cpu=4),
                     'long': SimpleLinuxUtilityForResourceManagementEngine(default_rqmt={'cpu': 1, 'mem': 4, 'time': 1})},
            default_engine='long')

If slurm should be used on a local machine to manage the resources, e.g. on a multi-gpu machine in a cloud service (AWS, GCP etc...),
have a look at this `quick-installation script <slurm_examples/bootstrap_slurm_ubuntu1804.sh>`_ for a simple slurm setup.

=====================
...Son of Grid Engine
=====================

The usage of a SGE backend can be enabled by inserting the following code into the ``settings.py``:

.. code:: python

    def engine():
        from sisyphus.engine import EngineSelector
        from sisyphus.localengine import LocalEngine
        from sisyphus.son_of_grid_engine import SonOfGridEngine
        return EngineSelector(
            engines={'short': LocalEngine(cpu=4),
                     'long': SonOfGridEngine(
                         default_rqmt={'cpu' : 1, 'mem' : 2, 'gpu' : 0, 'time' : 1},
                         gateway="<gateway-machine-name>")}, # a gateway is only needed if the local machine has no SGE installation
            default_engine='long')

Some setups may use the ``mini_task`` flag to mark jobs that should be run locally instead of being submitted to a cluster system.
Using ``mini_task`` requires a ``"short"`` engine to be defined.
Additional SGE parameters can be passed to Tasks by adding a ``qsub_args`` string entry to the ``rqmt`` dict.
