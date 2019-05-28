import sisyphus
import sisyphus.engine
import sisyphus.manager
import sisyphus.toolkit as tk
from importlib import reload
from sisyphus.loader import load_config_file

# Setup logging
import logging
import sisyphus.logging_format

__author__ = "Jan-Thorsten Peter, Eugen Beck"
__email__ = "peter@cs.rwth-aachen.de"

logging.root.setLevel(20)
sisyphus.logging_format.add_coloring_to_logging()

job_engine = sisyphus.engine.Engine()

target_output = {}
sis_graph = sisyphus.graph.SISGraph(target_output)


manager = sisyphus.manager.Manager(sis_graph=sis_graph,
                                   job_engine=job_engine,
                                   callbacks={},
                                   link_outputs=False,
                                   clear_errors_once=False,
                                   start_computations=True,
                                   auto_print_stat_overview=True)

manager.stop_if_done = False

__all__ = ['tk', 'job_engine', 'sis_graph', 'manager', 'sisyphus', 'sis_graph', 'reload', 'load_config_file']
