===
FAQ
===

Dryrun a Job without engine
---------------------------
For debugging it is often annoying to submit a job to the engine just to see it crashing shortly after.
If the job directory is already created just run the command that would be executed by Sisyphus manual::

    sis worker <work/path/to/job> <task_name>

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
