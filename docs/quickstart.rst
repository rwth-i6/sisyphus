==========
Quickstart
==========

``sis`` command
---------------

Sisyphus is started by running the `sis` command in it's folder.
The main mode of this tool is `sis manager` or short `sis m` it will parses the config directive and will submit the required job to the cluster.
The manager will periodically check which jobs have finished and submits all jobs that became runnable to the cluster as long as it is running.
If you stop the manager (using `Ctrl-C`) no further jobs are submitted, but jobs submitted to the cluster will continue.


Cheatsheet
----------

.. list-table::
 :widths: 50 50
 :header-rows: 1

 * - What to do
   - Command
 * - load the default graph and run the manager
   - ``./sis m -r``
