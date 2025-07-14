# Author: Jan-Thorsten Peter <peter@cs.rwth-aachen.de>

"""This module contains helper methods used in the console or in a Job. Use tk.<name of function>? for more help.

Useful examples::

    # Find job or path:
    j = tk.sis_graph.find('LineSplitter')
    # Find only job:
    j = tk.sis_graph.find('LineSplitter', mode='job')
    # Find only path:
    j = tk.sis_graph.find('LineSplitter', mode='path')

    # Rerun tasks depending on a given file/job:
    tk.remove_job_and_descendants(tk.sis_graph.find('hitchhiker'))

    # Setup job:
    tk.setup_job_directory(j)
    # run job:
    tk.run_job(j)

    # Reload start up config:
    tk.reload_config(config_files)
    # Reload config file:
    tk.reload_config('path/to/config/file/or/directory')
    # Reload all recipe files
    reload_recipes()
    # Load job from job directory:
    tk.load_job('path/to/job/direcotry')

    # Import jobs from other work directory
    tk.import_work_directory(['path/to/other/work'], mode='copy')

    # Print short job summary
    tk.job_info(j)

    # List functions to cleanup work directory:
    tk.cleaner?
"""

import glob
import gzip
import logging
import os
import pickle
import shutil
import tarfile
import tempfile
from typing import Union, Any, List, Optional
import subprocess
import importlib

from sisyphus.tools import sh, extract_paths
import sisyphus.block
from sisyphus.block import block, sub_block, set_root_block
from sisyphus.async_workflow import async_run, async_gather, async_context
from sisyphus.delayed_ops import Delayed
from sisyphus import cleaner

from sisyphus.job_path import AbstractPath, Path, Variable
from sisyphus.job import Job
from sisyphus import graph
import sisyphus.global_settings as gs

from sisyphus.loader import config_manager


class BlockedWorkflow(Exception):
    pass


# Functions mainly useful in Job definitions
def zipped(filename: Union[Path, str]) -> bool:
    """Check if given file is zipped

    :param filename (Path/str): File to be checked
    :return (bool): True if input file is zipped"""
    with open(str(filename), "rb") as f:
        return f.read(2) == b"\x1f\x8b"


class mktemp:
    """Object to be used by the with statement.
    creates temporary file that will be delete at exit. Can be used like this::

        with mktemp() as temp:
            #do stuff with temp
            f = open(temp, 'w')
            f.write('foo')

            f = open(temp, 'r')
            foo = f.read()
        # temp file is deleted
    """

    def __enter__(self):
        self.temp_path = tempfile.mktemp(prefix=gs.TMP_PREFIX)
        return self.temp_path

    def __exit__(self, type, value, traceback):
        if len(glob.glob(self.temp_path)):
            if os.path.isdir(self.temp_path):
                shutil.rmtree(self.temp_path)
            else:
                os.unlink(self.temp_path)


def input_path(path: Union[Path, str]) -> Path:
    """Ensures a given input is a Path. Strings are automatically converted into Path objects

    :param path: path that should be checked
    :return: Path object
    """
    if isinstance(path, str):
        return Path(path)
    else:
        assert isinstance(path, Path)
        return path


# TODO remove?
def is_path(path):
    return isinstance(path, Path)


# TODO remove?
def uncached_path(path):
    """
    :param Path|str path:
    :rtype: str
    """
    return path.get_path() if is_path(path) else str(path)


# TODO remove?
def bundle_to_str(bundle):
    """Convert bundle of objects into a space separated list"""
    if isinstance(bundle, set):
        bundle = sorted(bundle)

    return " ".join(str(i) for i in bundle)


sis_graph = graph.graph


# graph macros
def find_job(pattern):
    jobs = sis_graph.find(pattern, mode="job")
    if len(jobs) == 0:
        print("No job found")
        return []
    else:
        print("Jobs found:")
        for i, job in enumerate(jobs):
            print("%i: %s" % (i, str(job)))
        return jobs


def find_path(pattern):
    return sis_graph.find(pattern, mode="path")


def register_output(name, value, export_graph=False):
    """
    :param str name:
    :param Path value:
    :param bool export_graph:
    """
    if not isinstance(value, AbstractPath):
        import pathlib

        if isinstance(value, pathlib.Path):
            raise ValueError(
                "pathlib.Path objects are not supported, use sisyphus.Path instead. "
                "Did your editor's auto-import choose the right Path class? "
                "You can convert it using `sisyphus.Path(pathlib_path)`."
            )
        raise ValueError(
            "Only sisyphus.Path or sisyphus.Variable objects are supported as output, "
            f"but {name} is of type {type(value)}.\n"
            f"{value}"
        )
    sis_graph.add_target(graph.OutputPath(name, value))
    if export_graph:
        dump(value, os.path.join(gs.OUTPUT_DIR, gs.ALIAS_AND_OUTPUT_SUBDIR, ".%s.sis" % name))


def register_callback(f, *args, **kwargs):
    assert callable(f)
    sis_graph.add_target(graph.OutputCall(f, args, kwargs))


def register_report(name, values, template=None, required=None, update_frequency=300):
    report = graph.OutputReport(
        output_path=name,
        report_values=values,
        report_template=template,
        required=required,
        update_frequency=update_frequency,
    )
    sis_graph.add_target(report)
    return report


class Object:
    """Simple helper class to create Objects without adding code"""

    pass


class RelPath:
    """
    Creates an object that will create a Path object relative to the current module if called
    """

    def __init__(self, origin, hash_overwrite=None):
        self.origin = origin
        self.hash_overwrite = hash_overwrite

    def __call__(self, path: str, *args, **kwargs) -> Path:
        if self.hash_overwrite and "hash_overwrite" not in kwargs and len(args) < 3:
            kwargs["hash_overwrite"] = os.path.join(self.hash_overwrite, path)
        if not os.path.isabs(path):
            path = os.path.join(self.origin, path)
            path = os.path.relpath(path)
        return Path(path, *args, **kwargs)


def setup_path(package: str) -> RelPath:
    """
    Should be called like ```rel_path = setup_path(__package__)``` which setups RelPath to create Path objects
    relative to the current module.

    :param str package:
    :rtype: RelPath
    """
    assert package, (
        "setup_path is used to make all path relative to the current package directory, "
        "it only works inside of directories and not if the config file is passed directly"
    )

    hash_overwrite = package.replace(".", "/")
    module = importlib.import_module(package)
    assert getattr(module, "__file__", None), "setup_path failed for %s. Does %s/__init__.py exist?" % (
        package,
        hash_overwrite,
    )
    path = os.path.dirname(module.__file__)
    path = os.path.relpath(path)

    return RelPath(path, hash_overwrite=hash_overwrite)


def dump(obj: Any, filename: str):
    """Dumps object using pickle in zipped file, creates directory if needed

    :param obj: object to pickle
    :param str filename: path to pickled file
    """
    outfile_dir = os.path.dirname(filename)
    if not os.path.isdir(outfile_dir):
        os.makedirs(outfile_dir)
    with gzip.open(filename, "wb") as f:
        pickle.dump(obj, f)


def load_file(path: str) -> Any:
    """Load object from pickled file, works with zipped and unzipped files

    :param str path: path to pickled file
    :return: unpickled object
    """
    fopen = gzip.open(path, "rb") if zipped(path) else open(path, "rb")
    with fopen as f:
        return pickle.load(f)


_sis_running_in_worker = False


def running_in_worker():
    """Returns rue if this code is run inside the worker"""
    return _sis_running_in_worker


# Helper functions mainly used in the console
def load_job(path: str) -> Job:
    """Load job from job directory even if it is already cleaned up

    :param path(str): path to job directory
    :return (Job):
    """

    def load_tar(filename):
        with tarfile.open(filename) as tar:
            with tar.extractfile(gs.JOB_SAVE) as f:
                return pickle.loads(gzip.decompress(f.read()))

    if os.path.isfile(path):
        if path.endswith(gs.JOB_FINISHED_ARCHIVE):
            graph = load_tar(path)
        else:
            graph = load_file(path)
    else:
        tmp_path = os.path.join(path, gs.JOB_SAVE)
        if os.path.isfile(tmp_path):
            graph = load_file(tmp_path)
        else:
            tmp_path = os.path.join(path, gs.JOB_FINISHED_ARCHIVE)
            assert os.path.isfile(tmp_path), "Could not find job path or file: %s" % path
            graph = load_tar(tmp_path)
    return graph


def setup_job_directory(job: Job):
    """Setup the work directory of the given job.

    :param Job|Path job: Job which needs work directory
    """

    original_input = job
    if is_path(job):
        job = job.creator
    from sisyphus.job import Job

    if isinstance(job, Job):
        if job._sis_runnable():
            job._sis_setup_directory()
            logging.info("Done setting up: %s" % job)
        else:
            missing_inputs = "\n".join(str(i) for i in job._sis_inputs if not i.available())
            logging.error("Job has still missing inputs: %s" % missing_inputs)
    else:
        logging.error("Not a job: %s" % original_input)
        print(type(job))


def run_job(job: Job, task_name: str = None, task_id: int = 1, force_resume: bool = False):
    """
    Run job directly in console window.

    :param Job job: Job with tasks to run
    :param str task_name: which task should run, default: The first listed task
    :param int task_id: which task_id should be used, default: 1
    :param bool force_resume: Force resume of job in error state
    """
    assert isinstance(job, Job), "%s is not a Job" % job

    if not job._sis_setup():
        logging.info("Job directory missing, set it up: %s" % job)
        setup_job_directory(job)

    task = None
    if task_name is None:
        task = job._sis_tasks()[0]
    else:
        for t in job._sis_tasks():
            if t._start == task_name:
                task = t
                break
        assert task is not None, "'%s' is not a valid task name (Valid names: %s)" % (
            task_name,
            [t._start for t in job._sis_tasks()],
        )

    try:
        call = task.get_worker_call(task_id)
        if force_resume:
            call.append("--force_resume")
        import subprocess

        process = subprocess.Popen(call)
        try:
            process.wait()
        except KeyboardInterrupt as e:
            process.terminate()
            process.wait()
            raise e
    except Exception as e:
        import traceback

        logging.error("Job failed %s" % e)
        traceback.print_exc()


def remove_job_and_descendants(
    jobs: Union[str, AbstractPath, Job, List[Union[str, AbstractPath, Job]]], mode: str = "remove"
) -> bool:
    """
    Remove all jobs that depend on the given jobs/paths.

    :param List[Job|Path] jobs: They and all jobs depended on them should be removed
    :param string mode: run mode (remove, move, dryrun)
    """
    assert mode in ["remove", "move", "dryrun"]
    sis_graph.update_nodes()

    delete_list = []
    not_setup_list = []

    if isinstance(jobs, (str, AbstractPath, Job)):
        jobs = [jobs]

    for source in jobs:
        # Make sure source is a string matching the _sis_contains_required_inputs pattern
        if isinstance(source, AbstractPath):
            source = str(source)
        elif isinstance(source, Job):
            source = os.path.join(gs.BASE_DIR, source._sis_path())

        assert isinstance(source, str), "Source is not string, Path, or Job it is: %s" % type(source)
        print("Check for %s" % source)

        def add_if_dependened(job):
            # check for new inputs
            job._sis_runnable()
            if job._sis_contains_required_inputs({source}, include_job_path=True):
                if os.path.isdir(job._sis_path()):
                    delete_list.append(job)
                else:
                    not_setup_list.append(job)
                return True
            else:
                return False

        sis_graph.for_all_nodes(add_if_dependened, bottom_up=False)

    if not delete_list:
        if not not_setup_list:
            print("No job depending on input found")
        else:
            print("No job depending on input is setup, these are the depending jobs:")
        for job in not_setup_list:
            print(job._sis_path())
        return

    delete_list = sorted(list(set(delete_list)))

    print("Deleting the following directories:")
    for job in delete_list:
        path = job._sis_path()
        if os.path.isdir(path):
            print(path)
    if mode != "dryrun":
        input_var = input("Start deleting? (y/N): ")
        if input_var == "y":
            for job in delete_list:
                if mode == "move":
                    job._sis_move()
                else:
                    job._sis_delete()
            sis_graph.update_nodes()
        else:
            print("Abort")


def import_work_directory(directories: Union[str, List[str]], mode="dryrun", use_alias=False):
    """
    Link or copy finished jobs from other work directories.

    :param str directories: Path to other work directories
    :param str mode: How to import job directories. Options: (copy, symlink, hardlink, dryrun)
    """

    if isinstance(directories, str):
        directories = [directories]

    def import_directory(job):
        # check for new inputs
        job._sis_runnable()
        # import work directory if job is not already setup
        if not job._sis_setup():
            job._sis_import_from_dirs(directories, mode=mode, use_alias=use_alias)
        return True

    number_of_jobs = 0
    # run once before to unsure inputs are updated at least once
    sis_graph.for_all_nodes(import_directory, bottom_up=True)
    # run until no new jobs are added. This could be solved more efficient, but this is works...
    while number_of_jobs != len(sis_graph.jobs()):
        number_of_jobs = len(sis_graph.jobs())
        sis_graph.for_all_nodes(import_directory, bottom_up=True)


def cached_engine(cache=[]):
    """Returns a cached version, for internal usage"""
    if not cache:
        # used persistent default argument as cache
        e = gs.engine()
        cache.append(e)
        return e
    return cache[0]


def start_manager(job_engine=None, start_computations=False):
    """Shortcut to start Manager

    :param job_engine: Use this job engine, init own job engine if set to None
    :param start_computations: Submit jobs directly
    :return: Manager
    """
    if job_engine is None:
        job_engine = cached_engine()
    import sisyphus.manager

    return sisyphus.manager.Manager(
        sis_graph=sis_graph,
        job_engine=job_engine,
        link_outputs=False,
        clear_errors_once=False,
        start_computations=start_computations,
        auto_print_stat_overview=False,
    )


def job_info(job: Job):
    """Prints information about given job to stdout

    :param job(Job):
    """
    from sisyphus import tools

    print("Job id: %s" % job._sis_id())
    print("Arguments:")
    for k, v in job._sis_kwargs.items():
        print("  %s : %s" % (k, str(v)))

    print("Inputs:")
    for name, value in job.__dict__.items():
        if not name.startswith("_sis_"):
            paths = tools.extract_paths(value)
            for path in paths:
                if path.creator is not job:
                    if path.creator is None:
                        print("  %s : %s" % (name, path.path))
                    else:
                        print("  %s : %s %s" % (name, path.creator._sis_id(), path.path))

    print("Outputs:")
    for name, value in job.__dict__.items():
        if not name.startswith("_sis_"):
            paths = tools.extract_paths(value)
            for path in paths:
                if path.creator is job:
                    print("  %s : %s" % (name, path.path))

    print("Job dir: %s" % os.path.abspath(job._sis_path()))
    print("Work dir: %s" % job._sis_path(gs.WORK_DIR))


def show_jobs_in_webserver(port: int, jobs: List[Job]):
    """Start web server on given port which displays given loaded jobs"""
    from sisyphus import http_server

    web_graph = graph.SISGraph()
    for job in jobs:
        if job._sis_outputs:
            output = next(iter(job._sis_outputs.values()))
            web_graph.add_target(graph.OutputPath(str(job), output))
        else:
            print(f"Skipping {job} since this job has no output")
    engine = cached_engine()
    engine.start_engine()
    http_server.start(sis_graph=web_graph, sis_engine=engine, port=port, thread=False)


def print_graph(targets=None, required_inputs=None):
    visited = {}
    # create dictionary with available paths
    required_inputs_str = set()
    if required_inputs:
        for i in required_inputs:
            if isinstance(i, AbstractPath):
                required_inputs_str.add(i.get_path())
            elif isinstance(i, Job):
                required_inputs_str.add(os.path.join(gs.BASE_DIR, i._sis_path()))
            elif isinstance(i, str):
                required_inputs_str.add(str(i))
            else:
                assert False

    if isinstance(targets, (AbstractPath, Job)):
        targets = [targets]

    if not targets:
        targets = set()
        for t in sis_graph.targets:
            targets.update(t.required)
        targets = list(targets)
        targets.sort()

    for target in targets:
        if isinstance(target, AbstractPath):
            creator = target.creator
            path = target
            if creator is None:
                # This path is a input path of the graph
                continue
        elif isinstance(target, Job):
            creator = target
            path = None
        else:
            assert False, "Target is neither Job nor Path it's : %s %s" % (type(target), target)
        if creator._sis_contains_required_inputs(required_inputs_str, include_job_path=True):
            if path:
                print("%s:" % path)
            else:
                print("%s" % creator._sis_path())
            path.creator._sis_print_tree(visited, required_inputs=required_inputs_str)
            print()
        else:
            print("%s: path" % (path))
            print()


def export_graph(output_file: Optional[str] = None):
    """
    Needs more testing

    :param output_file:
    :return:
    """
    import sys

    sis_graph.update_nodes()

    out = open(output_file, "w") if output_file else sys.stdout

    for path, job in sis_graph.path_to_all_nodes():
        out.write("%s %s\n" % (job._sis_id(), repr(path)))


def migrate_graph(input_file=None, work_source=None, mode="dryrun"):
    """
    migrate the graph from the provided graph file to the current graph

    :param str input_file: path to the graph file
    :param str|None work_source: path to the work folder, if None use the local work folder
    :param str mode: dryrun, link, copy, move, move_and_link, hardlink_or_copy, hardlink_or_link, the default is dryrun
    :return:
    """
    sis_graph.update_nodes()

    if not work_source:
        work_source = gs.WORK_DIR

    import sys
    from ast import literal_eval

    in_stream = open(input_file) if input_file else sys.stdin
    for line in in_stream:
        job_id, path = line.split(" ", 1)
        path = literal_eval(path)
        job = sis_graph.get_job_from_path(path)
        if job:
            job._sis_migrate_directory(os.path.join(work_source, job_id), mode=mode)
        else:
            logging.warning("Could not find: %s" % path)


#  ### Graph modify and compare functions
def compare_graph(obj1, obj2, traceback=None, visited=None):
    """Compares two objects and shows traceback to first found difference

    :param obj1 (Job/Path): Object1 to compare
    :param obj2 (Job/Path): Object2 which is compared to Object1
    :param traceback: Used for recursion, leave blank
    :param visited: Used for recursion, leave blank
    :return: traceback
    """

    visited = set() if visited is None else visited

    traceback = [] if traceback is None else traceback
    traceback.append((obj1, obj2))

    sis_hash = gs.SIS_HASH(obj1)
    skip = sis_hash in visited
    if not skip:
        visited.add(gs.SIS_HASH(obj1))

    if skip:
        pass
    elif type(obj1) is not type(obj2):
        yield traceback + [(type(obj1), type(obj2))]
    elif isinstance(obj1, Job):
        if obj1._sis_id() != obj2._sis_id():
            yield from compare_graph(obj1._sis_kwargs, obj2._sis_kwargs, traceback[:], visited)
    elif isinstance(obj1, AbstractPath):
        if obj1.path != obj2.path:
            yield traceback + [(obj1.path, obj2.path)]
        else:
            yield from compare_graph(obj1.creator, obj2.creator, traceback[:], visited)
    elif isinstance(obj1, (list, tuple, set)):
        if len(obj1) != len(obj2):
            yield traceback + [len(obj1), len(obj2)]
        else:
            if isinstance(obj1, set):
                obj1 = sorted(list(obj1))
                obj2 = sorted(list(obj2))
            for a, b in zip(obj1, obj2):
                yield from compare_graph(a, b, traceback[:], visited)
    elif isinstance(obj1, dict):
        for k, v1 in obj1.items():
            try:
                v2 = obj2[k]
            except KeyError:
                yield traceback + [(k, None)]
            else:
                yield from compare_graph(v1, v2, traceback[:], visited)

        for k, v2 in obj2.items():
            if k not in obj1:
                yield traceback + [(None, k)]
    elif hasattr(obj1, "__dict__"):
        yield from compare_graph(obj1.__dict__, obj2.__dict__, traceback[:], visited)
    elif hasattr(obj1, "__slots__"):
        for k in obj1.__slots__:
            if hasattr(obj1, k):
                if hasattr(obj2, k):
                    v1 = getattr(obj1, k)
                    v2 = getattr(obj2, k)
                    yield from compare_graph(v1, v2, traceback[:], visited)
                else:
                    yield traceback + [(k, None)]
            else:
                if hasattr(obj2, k):
                    yield traceback + [(None, k)]
    else:
        if obj1 != obj2:
            yield traceback[:]


def replace_graph_objects(current, mapping=None, replace_function=None):
    """This function takes a given graph and creates a new graph where every object listed in mapping is replaced.

    current: current graph
    mapping: [(old_object, new_object), ....]
    replace_function: how an object will be replace, defaults using the mapping

    returns: New graph
    """

    def replace_function_mapping(obj):
        for old, new in mapping:
            if obj == old:
                return new
        return obj

    if replace_function is None:
        assert mapping is not None
        replace_function = replace_function_mapping

    return _replace_graph_objects_helper(current, replace_function)


def _replace_graph_objects_helper(current, replace_function=None, visited=None):
    visited = {} if visited is None else visited
    sis_hash = gs.SIS_HASH(current)
    try:
        return visited[sis_hash]
    except KeyError:
        pass

    replace = replace_function(current)
    if replace != current:
        visited[sis_hash] = replace
        return replace

    if isinstance(current, Job):
        kwargs = _replace_graph_objects_helper(current._sis_kwargs, replace_function, visited)
        next = type(current)(**kwargs)
    elif isinstance(current, AbstractPath):
        creator = _replace_graph_objects_helper(current.creator, replace_function, visited)
        # TODO tage care of other attributes
        next = type(current)(current.path, creator)
    elif isinstance(current, (list, tuple, set)):
        next = type(current)(_replace_graph_objects_helper(i, replace_function, visited) for i in current)
    elif isinstance(current, dict):
        next = type(current)(
            (k, _replace_graph_objects_helper(v, replace_function, visited)) for k, v in current.items()
        )
    elif hasattr(current, "__dict__"):
        # TODO may add usage of get an set state
        dict_ = _replace_graph_objects_helper(current.__dict__, replace_function, visited)
        if dict_ == current.__dict__:
            next = current
        else:
            next = type(current).__new__(type(current))
            next.__dict__ = dict_
    elif hasattr(current, "__slots__"):
        diff = False
        dict_ = {}
        for k in current.__slots__:
            if hasattr(current, k):
                v = getattr(current, k)
                new = _replace_graph_objects_helper(v, replace_function, visited)
                diff = diff or v != new
        if diff:
            next = current
        else:
            next = type(current).__new__(type(current))
            for k, v in dict_:
                setattr(next, k, v)
    else:
        next = current
    visited[sis_hash] = next
    return next


# Reload functions
def _reload_prefix(prefix):
    import sys
    import importlib

    for name, module in sys.modules.items():
        if name.startswith(prefix):
            importlib.reload(module)


def reload_recipes():
    """Reload all recipes"""
    _reload_prefix(gs.RECIPE_PREFIX)


# Removed since it could cause problems with async config files
# Best practice would be to restart sisyphus
# def reload_config(config_files: List[str] = []):
#     """ Reset state, reload old config files, and load given config_files
#
#     :param config_files([str, ...]):
#     """
#     # Reset current state
#     import sisyphus.job
#     from sisyphus.loader import config_manager
#     sisyphus.job.created_jobs = {}
#     global sis_graph
#     sis_graph = graph.SISGraph()
#
#     _reload_prefix(gs.CONFIG_PREFIX)
#
#     # Load new config
#     config_manager.load_configs(config_files)


def reload_module(module):
    """Shortcut to reload module, keep sis_graph if toolkit is reloaded

    :param module: Module to reload
    :return:
    """
    import importlib

    if module.__file__ == __file__:
        # Reloading this module, save and restore sis_graph
        tmp = sis_graph
        importlib.reload(module)
        module.sis_graph = tmp
    else:
        importlib.reload(module)


def setup_script_mode():
    """Use this function if you start sisyphus from an recipe file, it will:

    #. setup logging level and prompt

    #. disable the wait periods

    #. disable unwanted warning

    You can run recipes directly by running something similar to this::

        export SIS_RECIPE_PATH=/PATH/TO/RECIPE/DIR
        # If sisyphus is not installed in your python path
        export PYTHONPATH=/PATH/TO/SISYPHUS:$PYTHONPATH
        # If you want to change the work directory:
        export SIS_WORK_DIR=/PATH/TO/WORK/DIR
        python3 $SIS_RECIPE_PATH/recipe/path_to_file script parameters

    An example for the recipe::

        import os
        import argparse
        from sisyphus import *
        from recipe.eval import bleu

        if __name__ == '__main__':
            tk.setup_script_mode()

            parser = argparse.ArgumentParser(description='Evaluate hypothesis')
            parser.add_argument('--hyp', help='hypothesis', required=True)
            parser.add_argument('--ref', help='reference', required=True)

            args = parser.parse_args()
            hyp = os.path.realpath(args.hyp)
            ref = os.path.realpath(args.ref)

            score = bleu(hyp, ref)

            tk.run(score, quiet=True)
            print(score.out)
    """
    # Setup logging
    import logging
    from sisyphus.logging_format import add_coloring_to_logging

    logging.basicConfig(format="[%(asctime)s] %(levelname)s: %(message)s", level=20)
    add_coloring_to_logging()

    for param in [
        "WAIT_PERIOD_JOB_FS_SYNC",  # Work around to avoid running into time out...
        "WAIT_PERIOD_JOB_CLEANUP",  # same here
        "WAIT_PERIOD_MTIME_OF_INPUTS",  # Speed up by not waiting for slow filesystem
        "ENGINE_NOT_SETUP_WARNING",  # Disable unwanted warning
    ]:
        setattr(gs, param, 0)
        gs.ENVIRONMENT_SETTINGS["SIS_%s" % param] = "0"


def run(obj: Any, quiet: bool = False):
    """
    Run and setup all jobs that are contained inside object and all jobs that are necessary.

    :param obj:
    :param quiet: Do not forward job output do stdout
    :return:
    """

    def run_helper(job):
        """
        Helper function which takes a job and runs it task until it's finished

        :param job: Job to run
        :return:
        """
        assert job._sis_runnable()
        if not job._sis_finished():
            logging.info("Run Job: %s" % job)
            job._sis_setup_directory()
            for task in job._sis_tasks():
                for task_id in task.task_ids():
                    if not task.finished(task_id):
                        if len(job._sis_tasks()) > 1 or len(task.task_ids()) > 1:
                            logging.info("Run Task: %s %s %s" % (job, task.name(), task_id))
                        log_file = task.path(gs.JOB_LOG, task_id)
                        env = os.environ.copy()
                        env.update(gs.ENVIRONMENT_SETTINGS)

                        call = " ".join(task.get_worker_call(task_id))
                        if quiet:
                            call += " --redirect_output"
                        else:
                            call += " 2>&1 > %s" % log_file
                        subprocess.check_call(call, shell=True, env=env)
                        assert task.finished(task_id), "Failed to run task %s %s %s" % (job, task.name(), task_id)

    # Create fresh graph and add object as report since a report can handle all kinds of objects.
    temp_graph = graph.SISGraph()
    temp_graph.add_target(
        graph.OutputReport(
            output_path="tmp", report_values=obj, report_template=None, required=None, update_frequency=0
        )
    )

    # Update SIS_COMMAND
    import sys

    gs.SIS_COMMAND = [sys.executable, "-m", "sisyphus"]
    gs.SKIP_IS_FINISHED_TIMEOUT = True

    def get_jobs():
        """Helper function to get all relevant jobs"""
        filter_list = (
            gs.STATE_WAITING,
            gs.STATE_RUNNABLE,
            gs.STATE_INTERRUPTED_RESUMABLE,
            gs.STATE_INTERRUPTED_NOT_RESUMABLE,
            gs.STATE_ERROR,
        )
        return {k: v for k, v in temp_graph.get_jobs_by_status(skip_finished=True).items() if k in filter_list}

    jobs = get_jobs()
    # Iterate over all runnable jobs until it's done
    while jobs:
        # Collect all jobs that can be run
        todo_list = jobs.get(gs.STATE_RUNNABLE, set())
        todo_list.update(jobs.get(gs.STATE_INTERRUPTED_RESUMABLE, set()))

        # Stop loop if no jobs can be run
        if not todo_list:
            logging.error("Can not finish computation of %s some jobs are blocking" % obj)
            for k, v in temp_graph.get_jobs_by_status(skip_finished=True).items():
                if k != gs.STATE_INPUT_PATH:
                    logging.error("Jobs in state %s are: %s" % (k, v))
            raise BlockedWorkflow("Can not finish computation of %s some jobs are blocking" % obj)

        # Actually run the jobs
        for job in todo_list:
            run_helper(job)
        jobs = get_jobs()

    gs.SKIP_IS_FINISHED_TIMEOUT = False


def submit_next_task(job: Job, setup_directory=True):
    if not job._sis_runnable():
        logging.warning("Job is not runnable")
        return
    if setup_directory:
        job._sis_setup_directory()
    cached_engine().start_engine()
    task = job._sis_next_task()
    if task is None:
        logging.warning("No task to run")
    else:
        cached_engine().submit(task)
