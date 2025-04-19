from sisyphus.tools import cache_result, extract_paths
import sisyphus.global_settings as gs
from sisyphus.job import Job
from sisyphus.job_path import AbstractPath
from sisyphus.block import Block
import sisyphus.tools as tools
import sisyphus.hash

import atexit
from inspect import isclass
import logging
import collections
import os
import time
import pprint
import threading
from typing import DefaultDict, Optional, List
from multiprocessing.pool import ThreadPool
from datetime import datetime


class Node(object):
    __slots__ = ["job", "sis_id", "inputs", "outputs"]

    def __init__(self, sis_id):
        self.job = None
        self.sis_id = sis_id
        self.inputs = set()
        self.outputs = set()


class OutputTarget:
    def __init__(self, name, inputs):
        """
        :param str name:
        :param inputs:
        """
        self._required = extract_paths(inputs)
        self.required_full_list = sorted(list(self._required))
        self.name = name

    def update_requirements(self, write_output=True, force=False):
        self._required = {out for out in self._required if not out.available()}

    @property
    def required(self):
        return self._required

    def is_done(self):
        if self._required is None:
            return False
        else:
            return len(self._required) == 0

    def run_when_done(self, write_output=None):
        pass

    def __fs_like__(self):
        if len(self.required_full_list) == 1:
            return self.required_full_list[0]
        else:
            return self.required_full_list

    def __eq__(self, other):
        return type(self) is type(other) and self.__dict__ == other.__dict__

    def __hash__(self):
        return sisyphus.hash.int_hash(self)


class OutputPath(OutputTarget):
    def __init__(self, output_path, sis_path):
        assert isinstance(sis_path, AbstractPath)
        self._output_path = output_path
        self._sis_path = sis_path
        super().__init__(output_path, sis_path)

    def run_when_done(self, write_output=None):
        """Checks if output is computed, if yes create output link"""
        assert self._sis_path.available()
        if write_output:
            creator = self._sis_path.creator
            if creator is None or len(creator._sis_alias_prefixes) == 0:
                prefixes = [""]
            else:
                prefixes = list(creator._sis_alias_prefixes)
            # Link output file
            for prefix in prefixes:
                outfile_name = os.path.join(gs.OUTPUT_DIR, prefix, self._output_path)
                outfile_dir = os.path.dirname(outfile_name)

                # Remove file if it exists, if not or if it is an directory an OSError is raised
                try:
                    os.unlink(outfile_dir)
                    logging.warning("Removed file from output directory: %s" % outfile_dir)
                except OSError:
                    pass

                # Create directory if it does not exist yet
                try:
                    try:
                        os.makedirs(outfile_dir)
                    except FileExistsError:
                        pass

                    # Check if current link is correct
                    prev_link = None
                    new_link = os.path.realpath(self._sis_path.get_path())
                    if os.path.islink(outfile_name):
                        prev_link = os.path.realpath(outfile_name)
                        if prev_link != new_link:
                            os.unlink(outfile_name)
                        else:
                            return

                    # Set new link if needed
                    os.symlink(new_link, outfile_name)
                    logging.info("Finished output: %s" % outfile_name)

                    if gs.FINISHED_LOG:
                        if "/" in gs.FINISHED_LOG and not os.path.exists(os.path.dirname(gs.FINISHED_LOG)):
                            os.makedirs(os.path.dirname(gs.FINISHED_LOG))
                        with open(gs.FINISHED_LOG, "a") as f:
                            now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            if prev_link and prev_link != new_link:
                                common_path = os.path.commonpath([prev_link, new_link])
                                clen = (len(common_path) + 1) if common_path != "/" else 0
                                f.write(
                                    f"{now_s}: Updated output: {outfile_name}:"
                                    f" {'.../' if clen else ''}{prev_link[clen:]} -> {new_link[clen:]}\n"
                                )
                            f.write(f"{now_s}: Finished output: {outfile_name} (-> {self._sis_path.rel_path()})\n")

                except OSError as e:
                    logging.warning("Failed to updated output %s. Exception: %s" % (outfile_name, e))


class OutputCall(OutputTarget):
    def __init__(self, f, argv, kwargs, required=None):
        self._function_call = (f, argv, kwargs)
        if required is False:
            # This call finishes never
            required = None
        elif required is None:
            # Extract requirements automatically
            required = (argv, kwargs)
        else:
            # Use given requirements
            required = required

        name = "callback_%s_%i_%s_%s" % (f.__name__, id(f), gs.SIS_HASH(argv), gs.SIS_HASH(kwargs))
        self._already_called = False
        super().__init__(name, required)

    def run_when_done(self, write_output=None):
        """Runs given function if output is available"""
        assert all(out.available() for out in self._required)
        if not self._already_called:
            f, args, kwargs = self._function_call
            f(*args, **kwargs)
            self._already_called = True


class OutputReport(OutputTarget):
    def __init__(self, output_path, report_values, report_template=None, required=None, update_frequency=300):
        super().__init__(output_path, required if required is not None else report_values)
        self._report_template = report_template
        self._report_values = report_values
        self._output_path = output_path
        self._update_frequency = update_frequency
        self._last_update = -update_frequency
        self._last_values = None

    def update_values(self, report_values):
        if report_values is not None:
            self._report_values = report_values
        self._required = extract_paths(self._report_values)
        self.required_full_list = sorted(list(self._required))

    def update_requirements(self, write_output=True, force=False):
        """Update current report if enough time as passed since last update"""
        if not force and time.time() - self._last_update < self._update_frequency:
            return
        else:
            self._last_update = time.time()

        if write_output:
            self.write_report()

    def write_report(self):
        # Allow for anonymous reports
        if self._output_path is None:
            return
        outfile_name = os.path.join(gs.OUTPUT_DIR, self._output_path)
        outfile_dir = os.path.dirname(outfile_name)
        try:
            if not os.path.isdir(outfile_dir):
                os.makedirs(outfile_dir)

            # Remove link to avoid overwriting other files
            if os.path.islink(outfile_name):
                os.unlink(outfile_name)

            # Actually write report
            with open(outfile_name, "w") as f:
                if self._report_template:
                    f.write(self._report_template.format(**self._report_values))
                elif callable(self._report_values):
                    f.write(str(self._report_values()))
                else:
                    f.write(pprint.pformat(self._report_values, width=140) + "\n")
        except IOError as e:
            logging.warning("Error while updating %s:  %s" % (outfile_name, str(e)))

    def run_when_done(self, write_output=None):
        if write_output:
            self.write_report()

    def __fs_like__(self):
        if callable(self._report_values):
            return super().__fs_like__()
        else:
            return {
                "template": self._report_template,
                "values": self._report_values,
                "frequency": self._update_frequency,
            }


class SISGraph(object):
    """This graph contains all targets that needs to be calculated and through there dependencies all required jobs.
    These jobs can be searched and modified using the provided functions. Most interesting functions are::

        # Lists all jobs
        jobs()
        # Find jobs by matching substring
        find(pattern)
        # Execute function for all nodes
        for_all_nodes(f)
        # Dictionaries with jobs sorted by current status:
        get_jobs_by_status()
    """

    def __init__(self):
        self._targets = set()  # type: set[OutputTarget]
        self._active_targets = []  # type: list[OutputTarget]
        self._pool = None
        self.used_output_path = set()

    @property
    def pool(self):
        if self._pool is None:
            self._pool = ThreadPool(gs.GRAPH_WORKER)
            atexit.register(self._pool.close)
        return self._pool

    @property
    def targets(self):
        return self._targets

    @property
    def active_targets(self):
        return self._active_targets

    def remove_from_active_targets(self, target):
        self._active_targets = [out for out in self._active_targets if out != target]

    @property
    def targets_dict(self):
        """
        :return: dict name -> target
        :rtype: dict[str,OutputTarget]
        """
        return {t.name: t for t in self._targets}

    @property
    def output(self):
        """Deprecated: used for backwards comparability, only supports path outputs"""
        out = {}

        for t in self._targets:
            if len(t.required_full_list) == 1:
                out[t.name] = t.required_full_list[0]
            else:
                for pos, path in enumerate(t.required_full_list):
                    out["%s_%02i" % (t.name, pos)] = path
        return out

    def add_target(self, target):
        """
        :param OutputTarget target:
        """

        # Avoid adding the same target multiple times
        if target in self._targets:
            return

        self._targets.add(target)

        # check if output path is already used
        try:
            path = target._output_path
            creator = self._sis_path.creator
            if creator is None or len(creator._sis_alias_prefixes) == 0:
                prefixes = [""]
            else:
                prefixes = list(creator._sis_alias_prefixes)
            for prefix in prefixes:
                prefixed_path = prefix + path
                if prefixed_path in self.used_output_path:
                    logging.warning("Output path is used more than once: %s" % path)
                self.used_output_path.add(prefixed_path)
        except AttributeError:
            pass

        if not target.is_done():
            self._active_targets.append(target)

    def update_nodes(self):
        """Update all nodes to get the most current dependency graph"""
        start = time.time()

        def update_nodes(job):
            job._sis_runnable()
            return True

        self.for_all_nodes(update_nodes)
        logging.debug("All graph nodes updated (time needed: %.2f)" % (time.time() - start))

    @cache_result(gs.FILESYSTEM_CACHE_TIME)
    def id_to_job_dict(self):
        return {job._sis_id(): job for job in self.jobs()}

    def __contains__(self, item):
        assert isinstance(item, Job)
        return item._sis_id() in self.id_to_job_dict()

    @cache_result(gs.FILESYSTEM_CACHE_TIME)
    def job_directory_structure(self):
        d = {}
        for job in self.jobs():
            current = d
            path = job._sis_id().split("/")
            for step in path[:-1]:
                if step not in current:
                    current[step] = {}
                current = current[step]
            current[path[-1]] = job
        return d

    def job_by_id(self, sis_id):
        return self.id_to_job_dict().get(sis_id)

    def jobs(self) -> List[Job]:
        """
        :return ([Job, ...]): List with all jobs in grpah
        """
        job_list = []

        def f(job):
            job_list.append(job)
            return True

        self.for_all_nodes(f)
        return job_list

    def find(self, pattern, mode="all"):
        """Returns a list with all jobs and paths that partly match the pattern

        :param pattern(str): Pattern to match
        :param mode(str): Select if jobs, paths or both should be returned. Possible values: all, path, job
        :return ([Job/Path, ...]): List with all matching jobs/paths
        """
        out = set()
        for j in self.jobs():
            if mode in ("all", "job"):
                vis_name = j.get_vis_name()
                aliases = j._sis_aliases if j._sis_aliases is not None else set()
                if (
                    pattern in j._sis_path()
                    or (vis_name is not None and pattern in vis_name)
                    or any(pattern in a for a in aliases)
                ):
                    out.add(j)
            if mode in ("all", "path"):
                for p in j._sis_inputs:
                    if pattern in str(p):
                        out.add(p)
        return list(out)

    def jobs_sorted(self):
        """Yields jobs in a order so that for each jop all jobs
        it depends on are already finished

        :return (generator Node): jobs sorted by dependency
        """
        id_to_job = {}

        def get_job(sis_id):
            if sis_id not in id_to_job:
                id_to_job[sis_id] = Node(sis_id)
            return id_to_job[sis_id]

        stack = []
        for job in self.jobs():
            node = get_job(job._sis_id())
            node.job = job
            for i in job._sis_inputs:
                if i.creator:
                    node.inputs.add(i.creator._sis_id())
                    get_job(i.creator._sis_id()).outputs.add(job._sis_id())
            id_to_job[node.sis_id] = node
            if not node.inputs:
                stack.append(node)
        stack.sort(key=lambda n: n.sis_id)

        def recursive_depth(node):
            yield node.job
            for sis_id in sorted(list(node.outputs)):
                next_node = id_to_job[sis_id]
                next_node.inputs.remove(node.sis_id)
                if not next_node.inputs:
                    for i in recursive_depth(next_node):
                        yield i

        for node in stack:
            for i in recursive_depth(node):
                yield i

    def get_jobs_by_status(
        self, nodes: Optional[List] = None, engine: Optional = None, skip_finished: bool = False
    ) -> DefaultDict[str, List[Job]]:
        """Return all jobs needed to finish output in dictionary with current status as key

        :param nodes: all nodes that will be checked, defaults to all output nodes in graph
        :param sisyphus.engine.EngineBase engine: Use status job status of engine, ignore engine status if set to None
               (default: None)
        :param bool skip_finished: Stop checking subtrees of finished nodes to save time
        :return ({status1\\: [Job, ...], status2\\: ...}): Dictionary with all jobs sorted by current state
        :rtype: dict[str,list[Job]]
        """

        states = collections.defaultdict(set)
        lock = threading.Lock()

        def get_unfinished_jobs(job):
            """
            Returns a list with all non finished jobs.

            :param Job job:
            :rtype: bool
            """
            # job not visited in this run, need to calculate dependencies
            if skip_finished and job._sis_finished():
                # stop on finished
                return False

            new_state = None
            if job._sis_runnable():
                if job._sis_setup():
                    if job._sis_is_set_to_hold():
                        new_state = gs.STATE_HOLD
                    elif job._sis_finished():
                        new_state = gs.STATE_FINISHED
                    else:
                        # check state of tasks
                        for task in job._sis_tasks():
                            if not task.finished():
                                new_state = task.state(engine)
                                break
                        # Job finished since previous check
                        if new_state is None:
                            # Stop here
                            if skip_finished:
                                return False
                            else:
                                new_state = gs.STATE_FINISHED
                else:
                    if job._sis_is_set_to_hold():
                        new_state = gs.STATE_HOLD
                    else:
                        new_state = gs.STATE_RUNNABLE
            else:
                new_state = gs.STATE_WAITING

            # List input paths
            for i in job._sis_inputs:
                if i.creator is None:
                    path_state = gs.STATE_INPUT_PATH if i.available() else gs.STATE_INPUT_MISSING
                    with lock:
                        states[path_state].add(i.get_path())
            assert new_state is not None
            with lock:
                states[new_state].add(job)
            return True

        self.for_all_nodes(get_unfinished_jobs, nodes=nodes)
        return states

    def for_all_nodes(self, f, nodes=None, bottom_up=False, *, pool: Optional[ThreadPool] = None):
        """
        Run function f for each node and ancestor for `nodes` from top down,
        stop expanding tree branch if functions returns False. Does not stop on None to allow functions with no
        return value to run for every node.

        :param (Job)->bool f: function will be executed for all nodes
        :param nodes: all nodes that will be checked, defaults to all output nodes in graph
        :param bool bottom_up: start with deepest nodes first, ignore return value of f
        :param pool: use custom thread pool
        :return: set with all visited nodes
        """

        # fill nodes with all output nodes if none are given
        if nodes is None:
            nodes = []
            for target in self._targets:
                for path in target.required_full_list:
                    if path.creator:
                        nodes.append(path.creator)

        if gs.GRAPH_WORKER == 1:
            visited_set = set()
            visited_list = []
            queue = list(reversed(nodes))
            while queue:
                job = queue.pop(-1)
                if id(job) in visited_set:
                    continue
                visited_set.add(id(job))
                job._sis_runnable()

                if bottom_up:
                    # execute in reverse order at the end
                    visited_list.append(job)
                else:
                    res = f(job)
                    # Stop if function has a not None but false return value
                    if res is not None and not res:
                        continue

                for path in job._sis_inputs:
                    if path.creator:
                        if id(path.creator) not in visited_set:
                            queue.append(path.creator)

            if bottom_up:
                for job in reversed(visited_list):
                    f(job)

            return visited_set

        visited = {}
        finished = 0

        pool_lock = threading.Lock()
        finished_lock = threading.Lock()
        stopped_event = threading.Event()
        if not pool:
            pool = self.pool

        # recursive function to run through tree
        def runner(job):
            """
            :param Job job:
            """
            sis_id = job._sis_id()
            with pool_lock:
                if stopped_event.is_set():
                    return
                if sis_id not in visited:
                    visited[sis_id] = pool.apply_async(
                        tools.default_handle_exception_interrupt_main_thread(runner_helper), (job,)
                    )

        def runner_helper(job):
            """
            :param Job job:
            """
            if stopped_event.is_set():
                return
            # make sure all inputs are updated
            job._sis_runnable()
            nonlocal finished

            if bottom_up:
                for path in job._sis_inputs:
                    if path.creator:
                        runner(path.creator)
                f(job)
            else:
                res = f(job)
                # Stop if function has a not None but false return value
                if res is None or res:
                    for path in job._sis_inputs:
                        if path.creator:
                            runner(path.creator)
            with finished_lock:
                finished += 1

        try:
            for node in nodes:
                runner(node)

            # Check if all jobs are finished
            while len(visited) != finished:
                time.sleep(0.1)
        except BaseException:
            with pool_lock:
                stopped_event.set()
            raise

        # Check again and create output set
        out = set()
        for k, v in visited.items():
            v.get()
            out.add(k)
        return out

    def path_to_all_nodes(self):
        visited = {}
        check_later = {}

        # recursive function to run through tree
        def runner(obj, path, only_check):
            if id(obj) in visited:
                return
            else:
                visited[id(obj)] = obj

            if not isclass(obj):
                try:
                    sis_id = obj._sis_id()
                    if sis_id in visited:
                        return
                    else:
                        visited[sis_id] = obj
                        if only_check:
                            logging.warning(
                                "Could not export %s since it's only reachable " "via sets. %s" % (obj, only_check)
                            )
                        else:
                            yield path, obj
                except AttributeError:
                    pass

            if isinstance(obj, set):
                if len(obj) == 1:
                    for name, value in enumerate(obj):
                        yield from runner(value, path + [name], only_check=only_check)
                elif only_check:
                    # check all values in the given set
                    for name, value in enumerate(obj):
                        yield from runner(value, path + [name], only_check=only_check)
                else:
                    # we can not handle this case since a set can be sorted different every time
                    # check later if we have any jobs we could not map in the end
                    if id(obj) in check_later:
                        check_later[id(obj)][1].append(path)
                    else:
                        check_later[id(obj)] = (obj, [path])
                    return
            elif isinstance(obj, list):
                for name, value in enumerate(obj):
                    yield from runner(value, path + [name], only_check=only_check)
            elif isinstance(obj, dict):
                for name, value in obj.items():
                    assert is_literal(name), "Can not export %s (type: %s) as directory key" % (name, type(name))
                    yield from runner(value, path + [name], only_check=only_check)
            elif isinstance(obj, AbstractPath):
                yield from runner(obj.creator, path + ["creator"], only_check=only_check)
            else:
                try:
                    for name, value in obj.__dict__.items():
                        if not name.startswith("_sis") and not isinstance(obj, Block):
                            yield from runner(value, path + [name], only_check=only_check)
                except AttributeError:
                    pass

        # check all outputs
        for target in self._targets:
            if isinstance(target, OutputPath):
                path = target._output_path
                obj = target._sis_path
                if obj.creator:
                    yield from runner(obj.creator, [path, "creator"], only_check=False)

        # check if there are any jobs that could not be reached due to sets
        for _, (obj, possible_paths) in check_later.items():
            del visited[id(obj)]  # remove to avoid early aborting
            for i in runner(obj, [], only_check=possible_paths):
                pass

    def get_job_from_path(self, path):
        """The reverse function for get_path_to_all_nodes"""

        # extract dict from targets
        current = {}
        for t in self._targets:
            if len(t.required_full_list) == 1:
                current[t.name] = t.required_full_list[0]
            else:
                for pos, required_path in enumerate(t.required_full_list):
                    current["%s_%02i" % (t.name, pos)] = required_path

        for step in path:
            if isinstance(current, dict):
                current = current.get(step)
            elif isinstance(current, (list, tuple)):
                if 0 <= step < len(current):
                    current = current[step]
                else:
                    return None
            elif hasattr(current, "__dict__"):
                current = current.__dict__.get(step)
            else:
                return None
        return current

    def set_job_targets(self, engine=None):
        """Add a target to all jobs (if possible) to have a more informative output"""

        # Reset all caches
        def f(job):
            try:
                job._sis_needed_for_which_targets = set()
            except AttributeError:
                pass

        self.for_all_nodes(f)

        for target in self.targets:
            if isinstance(target, OutputPath):
                name = target._output_path
                out = target._sis_path

                if out.creator is not None:
                    logging.info(
                        "Add target %s to jobs (used for more informativ output, "
                        "disable with SHOW_JOB_TARGETS=False)" % name
                    )

                    def f(job):
                        if gs.SHOW_JOB_TARGETS is True or len(job._sis_needed_for_which_targets) < gs.SHOW_JOB_TARGETS:
                            job._sis_needed_for_which_targets.add(name)
                            return True
                        return False

                    self.for_all_nodes(f=f, nodes=[out.creator])


def is_literal(obj, visited=None):
    # The most likely checks at the beginning
    if isinstance(obj, (str, bytes, int, float, type(None))):
        return True

    # Avoid being stuck in a loop
    if visited is None:
        visited = {id(obj)}
    elif id(obj) in visited:
        return False
    else:
        visited.add(id(obj))

    if isinstance(obj, (list, tuple, set)):
        return all(is_literal(i, visited) for i in obj)
    if isinstance(obj, dict):
        for k, v in obj.items():
            if not is_literal(k, visited) or not is_literal(v, visited):
                return False
    return False


graph = SISGraph()
