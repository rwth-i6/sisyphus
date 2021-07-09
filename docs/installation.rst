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
   - sudo pip3 install psutil
   - sudo pip3 install ipython

  Optional if web interface should be used:
   - sudo pip3 install flask

  Optional to compile documentation:
   - sudo pip3 install sphinx
   - sudo pip3 install sphinx_rtd_theme
   - sudo pip3 install sphinxcontrib-mermaid

  Optional if virtual file system should be used:
   - sudo pip3 install fusepy
   - sudo addgroup $USER fuse  # depending on your system


===============
...Local Engine
===============

This section is under construction!

========
...Slurm
========

This section is under construction!

=====================
...Son of Grid Engine
=====================

This section is under construction!
