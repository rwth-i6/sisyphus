# Author: Jan-Thorsten Peter <jtpeter@apptek.com>

"""This module contains helper functions to clean up the work directory to be used in the console.
Use tk.<name of function>? for more help.

Useful examples::

    # List all directories used in loaded graph:
    tk.cleaner.list_all_graph_directories()

    j = tk.sis_graph.find('LineSplitter')
    # Find only job:
    j = tk.sis_graph.find('LineSplitter', mode='job')
    # Find only path:
"""

import logging
import os
import shutil
import sys
import tempfile

from typing import Dict, List, Optional, Set, Union

from sisyphus import graph
from sisyphus.job import Job
from sisyphus.job_path import Path
import sisyphus.global_settings as gs


# Status codes
DIR_IN_GRAPH = -4
JOB_NOT_FINSIHED = -3
JOB_STILL_NEEDED = -2
JOB_WITHOUT_KEEP_VALUE = -1


def extract_keep_values_from_graph() -> Dict[str, int]:
    """Go through loaded graph and create dict with all jobs and keep values

    :return:
    """
    # create a dictionary with all paths in the current graph
    active_paths = {}
    # and a set containing all jobs which should not be deleted yet since they are needed to compute
    # the output of unfinished jobs or belong to the output. Recheck targets until no new targets are added
    needed = set()
    last_targets = None

    sis_graph = graph.graph
    current_targets = sis_graph.targets.copy()
    while last_targets != current_targets:
        for target in sis_graph.targets.copy():
            for path in target.required:
                active_paths[os.path.abspath(os.path.join(path.get_path()))] = path
                if path.creator is not None:
                    needed.update(path.creator._sis_get_needed_jobs({}))
                    active_paths.update(path.creator._sis_get_all_inputs())
        last_targets = current_targets
        current_targets = sis_graph.targets.copy()

    needed = {job._sis_path() for job in needed}

    # create directory with all jobs and partial paths to these jobs
    job_dirs = {}
    for k, v in active_paths.items():
        if hasattr(v, "creator") and v.creator:
            job = v.creator
            path = job._sis_path()
            if path in needed:
                status = JOB_STILL_NEEDED
            elif not job._sis_finished():
                status = JOB_NOT_FINSIHED
            else:
                status = job.keep_value()
                if status is None:
                    status = JOB_WITHOUT_KEEP_VALUE
            job_dirs[path] = status
            path_parts = os.path.split(path)[0]
            while path_parts:
                if path_parts not in job_dirs:
                    job_dirs[path_parts] = DIR_IN_GRAPH
                path_parts = os.path.split(path_parts)[0]
    return job_dirs


def find_too_low_keep_value(
    job_dirs: Union[str, Dict[Union[str, Path], int]],
    min_keep_value: int,
    filter_removed_jobs: Optional[List[Union[str, Path]]] = None,
) -> Set[Union[Path, str]]:
    """Check all given job if they can be removed and have a keep value lower min_keep_value.

    :param job_dirs: dict with all keep values, can be created with extract_keep_values_from_graph
    :param min_keep_value: minimal keep value
    :param filter_removed_jobs: Only Jobs matching the substring will be deleted

    :return:
    """
    if isinstance(job_dirs, str):
        job_dirs = load_used_paths(job_dirs)

    to_remove = set()
    for path, keep_value in job_dirs.items():
        if keep_value in (DIR_IN_GRAPH, JOB_NOT_FINSIHED, JOB_STILL_NEEDED):
            continue
        if keep_value == JOB_WITHOUT_KEEP_VALUE:
            keep_value = gs.JOB_DEFAULT_KEEP_VALUE
        if keep_value < min_keep_value and (filter_removed_jobs is None or any(x in path for x in filter_removed_jobs)):
            to_remove.add(path)
    return to_remove


def list_all_graph_directories() -> Dict[str, int]:
    """Create dict containing all filesystem directories used by jobs inside the loaded graph

    :return: dict
    """
    job_dirs = {}
    for job in graph.graph.jobs():
        path = job._sis_path()
        job_dirs[path] = JOB_STILL_NEEDED
        path_parts = os.path.split(path)[0]
        while path_parts:
            job_dirs[path_parts] = DIR_IN_GRAPH
            path_parts = os.path.split(path_parts)[0]
    return job_dirs


def save_used_paths(outfile: Union[str, Path] = None, job_dirs: Dict[Union[str, Path], int] = None):
    """Write dict of directories in the graph to file

    :param outfile: Filename of output file, if not given write to stdout
    :param job_dirs: Job dirs that will be written to file, if not given it will be extracted from current graph
    :return:
    """
    out = open(outfile, "a") if outfile else sys.stdout
    if job_dirs is None:
        job_dirs = list_all_graph_directories()
    for path, status in job_dirs.items():
        out.write("%s %i\n" % (path, status))
    if out != sys.stdout:
        out.close()


def load_used_paths(infile: Union[str, Path]) -> Dict[str, int]:
    """Load list save with save_used_paths

    :param infile: Filename to load from
    :return: remove_list
    """
    job_dirs = {}
    with open(infile) as f:
        for line in f:
            path, status = line.split()
            job_dirs[path] = int(status)
    return job_dirs


def save_remove_list(to_remove: List[Union[str, Path]], outfile: Union[str, Path]):
    """Write list of files that should be removed to file
    :param to_remove: List of directories
    :param outfile: Filename of output file
    :return:
    """
    with open(outfile, "w") as f:
        for i in sorted(to_remove):
            f.write(i + "\n")


def load_remove_list(infile: Union[str, Path]) -> List[str]:
    """Load list save with save_remove_list

    :param infile: Filename to load from
    :return: remove_list
    """
    out = []
    with open(infile) as f:
        for i in f:
            out.append(i.strip())
    return out


def search_for_unused(
    job_dirs: Union[str, Dict[Union[str, Path], int]],
    current: str = gs.WORK_DIR,
    verbose: bool = True,
    filter_unused: Optional[List[str]] = None,
) -> Set[str]:
    """Check work directory and list all subdirectories which do not belong to the given list of directories.

    :param job_dirs: dict with all used directories, can be created with list_all_graph_directories.
    :param current: current work directory
    :param verbose: make it verbose
    :param filter_unused: Only Jobs matching the substring will be deleted

    :return: List with all unused directories
    """

    if isinstance(job_dirs, str):
        job_dirs = load_used_paths(job_dirs)

    unused = set()  # going to hold all directories not needed anymore

    all_dirs = os.listdir(current)
    if verbose:
        logging.info("Directories in %s: %i" % (current, len(all_dirs)))
    for short_path in all_dirs:
        path = os.path.join(current, short_path)
        status = job_dirs.get(path)

        if status is None and (filter_unused is None or any(x in path for x in filter_unused)):
            unused.add(path)
        elif status == DIR_IN_GRAPH:
            # directory has sub directories used by current graph
            found = search_for_unused(job_dirs, path, verbose, filter_unused=filter_unused)
            unused.update(found)
            if verbose:
                logging.info("found %s unused directories in %s (total so far: %s)" % (len(found), path, len(unused)))
        else:
            # if nothing else matches it's a job of this graph so let's keep it
            pass
    return unused


def remove_directories(
    dirs: Union[str, Dict[Union[str, Path], int], Set],
    message: str,
    move_postfix: str = ".cleanup",
    mode: str = "remove",
    force: bool = False,
    filter_printed: Optional[List[str]] = None,
):
    """list all directories that will be deleted and add a security check"""
    if isinstance(dirs, str):
        dirs = load_remove_list(dirs)
    tmp = list(dirs)
    tmp.sort(key=lambda x: str(x))

    logging.info(message)
    logging.info("Number of affected directories: %i" % len(dirs))
    if len(dirs) == 0:
        return

    input_var = "UNSET"
    while input_var.lower() not in ("n", "y", ""):
        input_var = input("Calculate size of affected directories? (Y/n): ")
    if input_var.lower() == "n":
        input_var = "UNSET"
        while input_var.lower() not in ("n", "y", ""):
            input_var = input("List affected directories? (Y/n): ")
        if input_var.lower() != "n":
            logging.info("Affected directories:")
            for i in tmp:
                if os.path.exists(i + "/info") and gs.CLEANER_PRINT_ALIAS:
                    with open(i + "/info") as f:
                        lines = f.readlines()
                        if lines[-1].strip().startswith("ALIAS"):
                            s = lines[-1].strip()
                            s.replace("ALIAS:", "ALIAS AT CREATION:")
                        else:
                            s = ""
                else:
                    s = ""
                if filter_printed is None or any(x in i for x in filter_printed):
                    logging.info(i + "  " + s)

    else:
        with tempfile.NamedTemporaryFile(mode="w") as tmp_file:
            for directory in dirs:
                tmp_file.write(directory + "\x00")
            tmp_file.flush()
            command = "du -sch --files0-from=%s" % tmp_file.name
            p = os.popen(command)
            print(p.read())
            p.close()

    input_var = "UNSET"
    if mode == "dryrun":
        input_var = "n"
    elif force:
        input_var = "y"

    while input_var.lower() not in ("n", "y"):
        message = "Move directories?" if mode == "move" else "Delete directories?"
        input_var = input("%s (y/n): " % message)

    if input_var.lower() == "y":
        for num, k in enumerate(dirs, 1):
            if mode == "move":
                logging.info("move: %s" % k)
                # todo: k.{postfix} is may already used
                shutil.move(k, k + "." + move_postfix)
            elif mode == "remove":
                logging.info("Delete: (%d/%d) %s" % (num, len(dirs), k))
                if os.path.islink(k):
                    os.unlink(k)
                else:
                    try:
                        shutil.rmtree(k)
                    except OSError as error:
                        print(error)
            else:
                assert False
    else:
        logging.error("Abort")


def cleanup_jobs():
    """Go through all jobs in the current graph. If they are finished it remove its work directory and compress
    the log files"""
    for job in graph.graph.jobs():
        if job is not True and job._sis_cleanable():
            # clean job directory if possible
            logging.info("cleanup: %s" % job._sis_path())
            job._sis_cleanup()


def cleanup_keep_value(
    min_keep_value: int,
    load_from: str = "",
    mode: str = "remove",
    filter_removed_jobs: Optional[List[str]] = None,
    filter_printed: Optional[List[str]] = None,
):
    """Go through all jobs in the current graph to remove all jobs with a lower keep value that the given minimum

    :param min_keep_value: Remove jobs with lower keep value than this
    :param load_from: File name to load list with used directories
    :param mode: Cleanup mode ('remove', 'move', or 'dryrun')
    :param filter_removed_jobs: Only Jobs matching the substring will be deleted
    :param filter_printed: Defines what substrings should be printed when listing affected directories

    """
    if min_keep_value <= 0:
        logging.error("Keep value must be larger than 0")
    if load_from:
        job_dirs = load_used_paths(load_from)
    else:
        job_dirs = extract_keep_values_from_graph()

    to_remove = find_too_low_keep_value(job_dirs, min_keep_value, filter_removed_jobs=filter_removed_jobs)
    remove_directories(
        to_remove,
        "Remove jobs with lower keep value than min",
        move_postfix=".cleanup",
        mode=mode,
        force=False,
        filter_printed=filter_printed,
    )


def cleanup_unused(
    load_from: str = "",
    job_dirs: List[Job] = None,
    mode: str = "remove",
    filter_unused: Optional[List[str]] = None,
    filter_printed: Optional[List[str]] = None,
):
    """Check work directory and remove all subdirectories which do not belong to the given list of directories.
    If no input is given it removes everything that is not in the current graph

    :param load_from: File name to load list with used directories
    :param job_dirs: Already loaded list of used directories
    :param mode: Cleanup mode ('remove', 'move', or 'dryrun')
    :param filter_unused: Only Jobs matching the substring will be deleted
    :param filter_printed: Defines what substrings should be printed when listing affected directories
    :return:
    """
    if job_dirs:
        pass
    elif load_from:
        job_dirs = load_used_paths(load_from)
    else:
        job_dirs = list_all_graph_directories()
    to_remove = search_for_unused(job_dirs, verbose=True, filter_unused=filter_unused)
    remove_directories(
        to_remove,
        "Not used in graph",
        mode=mode,
        force=False,
        filter_printed=filter_printed,
    )
