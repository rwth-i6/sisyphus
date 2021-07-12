==========
Quickstart
==========

``sis`` command
---------------

Sisyphus is started by running the ``sis`` command in it's folder.
The main mode of this tool is ``sis manager`` or short ``sis m`` it will parses the config directive and will submit the required job to the cluster.
The manager will periodically check which jobs have finished and submits all jobs that became runnable to the cluster as long as it is running.
If you stop the manager (using ``Ctrl-C``) no further jobs are submitted, but jobs submitted to the cluster will continue.


Cheatsheet
----------


.. list-table:: Manager commands
 :header-rows: 1

 * - What to do
   - Command
 * - load the default graph and run the manager
   - ``./sis m -r``
 * - load the default graph and prompt for options
   - ``./sis m``
 * - load a sub-graph function
   - ``./sis m config.sub_module.sub_function``

.. list-table:: Handling jobs and tasks
 :header-rows: 1

 * - What to do
   - Command
 * - run a (already created) task directly*
   - ``./sis worker <path/to/job.hash> <task_name>``
 * - create a non-existing job manually
   - #. ``./sis c``
     #. | ``jobs = tk.find_jobs("<query>")``
        | this will list all found jobs
     #. ``j = jobs[<desired_index>]``
     #. ``tk.setup_job_directory(j)``
 * - restart a crashed job (by deleting the job)
   - ``rm -r <path/to/job.hash>``
 * - restart a crashed task (keeping the job)
   - #. ``rm <path/to/job.hash>/error.<task_name>.*``
     #. ``rm <path/to/job.hash>/log.<task_name>.*``
 * - restart an interrupted task
   - ``rm <path/to/job.hash>/log.<task_name>.*``


`*(WARNING: this might run the job with different ENVIRONMENT variables)`

.. list-table:: Quick console commands
 :header-rows: 1


 * - What to do
   - Command
 * - | import jobs from another work directory
     | (symlink or copy)
   - #. ``./sis c``
     #. ``tk.import_work_directory(<path/to/other/work>)``
 * - | delete jobs that are no longer part of
     | the graph (WARNING: make sure your default
     | graph contains all subgraphs)
   - #. ``./sis c``
     #. ``tk.cleaner.cleanup_unused()``


