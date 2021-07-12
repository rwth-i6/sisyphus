====
Demo
====

To run sisyphus you need to setup an experiment folder that contains all needed files (See :ref:`sec-structure`).

An example directory is given in the `example folder <https://github.com/rwth-i6/sisyphus/tree/master/example>`_.
It runs the workflow presented by the diagramm below

.. mermaid::

  graph TD;
      start[Start]:::startclass -- data/5lines.txt --> Splitter
      Splitter -- out_path --> Parallel
      Parallel -- out_path: path, check_block --> Simple
      Parallel -- out --> result[Finish]:::resultclass
      classDef resultclass fill:#f66;
      classDef startclass fill:#63e06d;
      PipeLine -- merger.gz, score --> Parallel

      subgraph pipeline
      Merger -- merger.gz  --> PipeLine
      FinishedParts -. out1.gz --> SimplePart1 -.-> Merger
      FinishedParts -. out2.gz  --> SimplePart2 -.-> Merger
      FinishedParts -. out3.gz  --> SimplePart3 -.-> Merger
      Arguments -- out.gz --> FinishedParts
      Simple -- out.gz --> Arguments
      Arguments -- out.gz  --> CheckState
      CheckState -- score --> PipeLine
    end

To start this toy setup run::

    ../sis manager

you will get something similar to::

    [2018-06-15 16:31:50,488] INFO: Add target result to jobs (used for more informativ output, disable with SHOW_JOB_TARGETS=False)
    [2018-06-15 16:31:50,796] INFO: runnable: Job< workdir: work/parallel/LineSpliter.AVSubx1baWqKyMx35c> <target: result>
    [2018-06-15 16:31:50,796] INFO: runnable(1) waiting(1)
    Print verbose overview (v), start manager (y), or exit (n)?

Start the computation by pressing `y`. You can stop the manager again at any time by pressing CTRL-C.
Sisyphus will show you which processes are currently running. For more information about the processes either check the web interface. It can be started with the http option::

    ../sis manager --http 8080

This will start a local web server at the given port. Visit it by going to http://localhost:8080
Once the final output is computed it will appear in the output folder. In this given example just some random text file.
