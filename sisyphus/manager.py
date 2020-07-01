import asyncio
import logging
import os
import sys
import time
import threading

from multiprocessing.pool import ThreadPool

from sisyphus import toolkit, tools
from sisyphus.loader import load_configs
from sisyphus.block import Block
import sisyphus.global_settings as gs


class JobCleaner(threading.Thread):
    """ Thread to scan all jobs and clean if needed """
    def __init__(self, sis_graph, worker=gs.JOB_CLEANER_WORKER):
        """
        :param sisyphus.graph.SISGraph sis_graph:
        :param int worker: number of workers for the thread pool
        """
        threading.Thread.__init__(self)
        self.daemon = True
        self.sis_graph = sis_graph
        self.worker = worker
        self.thread_pool = ThreadPool(self.worker)
        self.stopped = False

    def run(self):

        def f(job):
            if job._sis_cleanable():
                self.thread_pool.apply_async(tools.default_handle_exception_interrupt_main_thread(job._sis_cleanup))
            return True

        while not self.stopped:
            self.sis_graph.for_all_nodes(f)
            time.sleep(gs.JOB_CLEANER_INTERVAL)

    def close(self):
        self.stopped = True
        self.thread_pool.close()


def manager(args):
    """ Manage which job should run next """

    if args.run:
        if not os.path.isdir(gs.WORK_DIR):
            prompt = '%s does not exist, should I continue? ' \
                     'The directory will be created if needed inplace (y/N)' % gs.WORK_DIR
            if args.ui:
                ret = args.ui.ask_user(prompt)
                logging.info('%s %s' % (prompt, ret))
            else:
                ret = input(prompt)
            if ret.lower() != 'y':
                logging.warning('Abort, create directory or link it to the wished work destination')
                return

    # try to load fuse filesystem
    filesystem = None
    if args.filesystem:
        import sisyphus.filesystem as filesystem

    # Ensure this thread has a event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    start = time.time()
    load_configs(args.config_files)
    load_time = time.time() - start
    if load_time < 5:
        logging.debug("Config loaded (time needed: %.2f)" % load_time)
    else:
        logging.info("Config loaded (time needed: %.2f)" % load_time)

    sis_graph = toolkit.sis_graph
    Block.sis_graph = sis_graph
    job_engine = toolkit.cached_engine()
    job_engine.start_engine()
    job_cleaner = None

    try:
        if args.run:
            create_aliases(sis_graph.jobs())
        else:
            gs.JOB_AUTO_CLEANUP = False

        if gs.JOB_AUTO_CLEANUP:
            job_cleaner = JobCleaner(sis_graph=sis_graph)
            job_cleaner.start()

        # The actual work loop
        if args.http_port is not None:
            logging.debug("Start http server")
            start_http_server(sis_graph=sis_graph,
                              sis_engine=job_engine,
                              port=args.http_port,
                              thread=True)

        manager = Manager(sis_graph=sis_graph,
                          job_engine=job_engine,
                          link_outputs=args.run,
                          clear_errors_once=args.clear_errors_once,
                          clear_interrupts_once=args.clear_interrupts_once,
                          ignore_once=args.ignore_once,
                          start_computations=args.run,
                          job_cleaner=job_cleaner,
                          interative=args.interactive,
                          ui=args.ui)
        if args.ui:
            args.ui.manager = manager
            args.ui.update_menu()
        else:
            manager.unpause()

        kernel_connect_file = None
        if gs.START_KERNEL:
            kernel_connect_file = init_IPython_kernel(user_ns={'manager': manager,
                                                               'job_engine': job_engine,
                                                               'tk': toolkit,
                                                               'sis_graph': sis_graph})

        try:
            if args.filesystem:
                # Start main loop
                logging.debug("Start main loop")
                manager.start()

                # graph updates
                graph_update_thread = threading.Thread(
                    target=tools.default_handle_exception_interrupt_main_thread(sis_graph.update_nodes))
                graph_update_thread.start()

                # Start filesystem
                # run in main thread to allow signal handling of FUSE
                if not os.path.isdir(args.filesystem):
                    os.mkdir(args.filesystem)
                filesystem.start(work_dir=gs.WORK_DIR, sis_graph=sis_graph, mountpoint=args.filesystem)
            else:
                manager.run()
        except KeyboardInterrupt:
            logging.info('Got user interrupt signal stop engine and exit')
            if kernel_connect_file:
                try:
                    os.remove(kernel_connect_file)
                except (IOError, OSError):
                    pass

            # Print traceback in debug mode
            if logging.root.isEnabledFor(logging.DEBUG):
                raise

            sys.exit(1)
    finally:
        if job_cleaner:
            job_cleaner.close()
        job_engine.stop_engine()


# This is used to order the states in a useful way
# Error should be at the bottom since this is the last thing show on screen
state_overview_order = {}

for pos, state in enumerate([gs.STATE_INPUT_PATH,
                             gs.STATE_FINISHED,
                             gs.STATE_WAITING,
                             gs.STATE_QUEUE,
                             gs.STATE_RUNNING,
                             gs.STATE_RUNNABLE,
                             gs.STATE_INTERRUPTED,
                             gs.STATE_UNKNOWN,
                             gs.STATE_QUEUE_ERROR,
                             gs.STATE_RETRY_ERROR,
                             gs.STATE_ERROR,
                             gs.STATE_INPUT_MISSING]):
    # The . is used to ensure unknown states are at the bottom
    # (as long as they start with a normal letter or number)
    state_overview_order[state] = '.%i.%s' % (pos, state)


class Manager(threading.Thread):
    def __init__(self, sis_graph, job_engine,
                 link_outputs=True,
                 clear_errors_once=False,
                 clear_interrupts_once=False,
                 ignore_once=False,
                 start_computations=False,
                 auto_print_stat_overview=True,
                 job_cleaner=None,
                 interative=False,
                 ui=None):
        """
        :param sisyphus.graph.SISGraph sis_graph:
        :param sisyphus.engine.EngineBase job_engine:
        :param bool link_outputs:
        :param bool clear_errors_once:
        :param bool start_computations:
        :param bool auto_print_stat_overview:
        :param JobCleaner|None job_cleaner:
        """
        threading.Thread.__init__(self)
        self.start_computations = start_computations
        self.clear_errors_once = clear_errors_once
        self.clear_interrupts_once = clear_interrupts_once
        self.ignore_once = ignore_once
        self.sis_graph = sis_graph
        self.job_engine = job_engine
        self.link_outputs = link_outputs
        self.auto_print_stat_overview = auto_print_stat_overview
        self.ui = ui
        self.interactive = interative
        self.interactive_always_skip = set()

        self.stop_if_done = True
        self._stop_loop = False

        assert not (self.clear_errors_once and self.ignore_once), \
            "Jobs in error state cannot be both ignored and cleared"

        if gs.SHOW_JOB_TARGETS:
            self.sis_graph.set_job_targets(job_engine)
        self.update_jobs()

        # Disable parallel mode for now, seems buggy
        self.thread_pool = ThreadPool(gs.MANAGER_SUBMIT_WORKER)
        self.job_cleaner = job_cleaner

        # Cached states of jobs
        self.jobs = None
        self.state_overview = None
        self._paused = True

    def stop(self):
        self._stop_loop = True

    def update_jobs(self, skip_finished=True):
        """ Return all jobs needed to finish output """
        self.jobs = jobs = self.sis_graph.get_jobs_by_status(engine=self.job_engine, skip_finished=skip_finished)
        return jobs

    def clear_states(self, state=gs.STATE_ERROR):
        # List errors/ interrupts
        job_cleared = False
        for job in self.jobs[state]:
            logging.warning('Clearing: %s' % job)
            job._sis_move()
            job_cleared = True
        self.update_jobs()
        return job_cleared

    def update_state_overview(self):
        self.state_overview = []
        for state, job_set in self.jobs.items():
            if state != gs.STATE_INPUT_PATH:
                self.state_overview.append("%s(%i)" % (state, len(job_set)))
            for job in job_set:
                logging.debug("%s: %s" % (state, job))

        self.state_overview.sort()
        return self.state_overview

    @staticmethod
    def get_job_info_string(state, job, verbose=False):
        if hasattr(job, '_sis_needed_for_which_targets') and job._sis_needed_for_which_targets:
            if verbose:
                info_string = '%s: %s <target: %s>' % (state,
                                                       job,
                                                       sorted(list(job._sis_needed_for_which_targets)))
            else:
                info_string = '%s: %s <target: %s>' % (state,
                                                       job,
                                                       sorted(list(job._sis_needed_for_which_targets))[0])
        else:
            info_string = '%s: %s' % (state, job)

        if gs.SHOW_VIS_NAME_IN_MANAGER and hasattr(job, "get_vis_name") and job.get_vis_name() is not None:
            info_string += " [%s]" % job.get_vis_name()
        return info_string

    @staticmethod
    def print_job_state(state, job, info_string, verbose=False):
        if hasattr(job, "info") and state == gs.STATE_RUNNING:
            job_manager_info_string = job.info()
            if job_manager_info_string is not None:
                info_string += " {%s} " % job_manager_info_string

        if state in [gs.STATE_INPUT_MISSING,
                     gs.STATE_RETRY_ERROR,
                     gs.STATE_ERROR]:
            logging.error(info_string)
            if state == gs.STATE_ERROR and gs.PRINT_ERROR:
                job._sis_print_error(gs.PRINT_ERROR_TASKS,
                                     gs.PRINT_ERROR_LINES)
        elif state in [gs.STATE_INTERRUPTED, gs.STATE_UNKNOWN]:
            logging.warning(info_string)
        elif state in [gs.STATE_QUEUE,
                       gs.STATE_HOLD,
                       gs.STATE_RUNNING,
                       gs.STATE_RUNNABLE]:
            logging.info(info_string)
        elif verbose:
            logging.info(info_string)
        else:
            logging.debug(info_string)

    def get_job_states(self, all_jobs=False, verbose=False):
        if all_jobs or self.is_paused():
            jobs = self.update_jobs(skip_finished=not all_jobs)
            self.update_state_overview()
        else:
            jobs = self.jobs

        output = []
        for state in sorted(jobs.keys(), key=lambda j: state_overview_order.get(j, j)):
            for job in sorted(list(jobs[state]), key=lambda j: str(j)):
                info_string = self.get_job_info_string(state, job, verbose)
                output.append((state, job, info_string))
        return output

    def print_state_overview(self, verbose=False):
        states = self.get_job_states(all_jobs=verbose, verbose=verbose)
        logging.info('Experiment directory: %s      Call: %s' % (os.path.abspath(os.path.curdir), ' '.join(sys.argv)))
        for state, job, info_string in states:
            self.print_job_state(state, job, info_string, verbose=verbose)
        if self.state_overview:
            logging.info(' '.join(self.state_overview))

    def work_left(self):
        # Check if there is anything that can be done by manager
        if self.stop_if_done and not any([state in self.jobs for state in [gs.STATE_RUNNABLE,
                                                                           gs.STATE_RUNNING,
                                                                           gs.STATE_QUEUE,
                                                                           gs.STATE_UNKNOWN,
                                                                           gs.STATE_INTERRUPTED]]):
            # check again to avoid caching effects
            time.sleep(gs.WAIT_PERIOD_CACHE)
            self.update_jobs()
            if not any([state in self.jobs for state in [gs.STATE_RUNNABLE,
                                                         gs.STATE_RUNNING,
                                                         gs.STATE_QUEUE,
                                                         gs.STATE_UNKNOWN,
                                                         gs.STATE_INTERRUPTED]
                        ]):
                logging.info("There is nothing I can do, good bye!")
                return False
        return True

    def ask_user(self, message, uid):
        if self.interactive:
            if uid in self.interactive_always_skip:
                return False
            answer = self.input('%s (Yes/skip/never)' % message).lower()
            if answer in ('', 'y', 'yes'):
                return True
            elif answer in ('s', 'skip'):
                return False
            elif answer in ('n', 'never'):
                self.interactive_always_skip.add(uid)
                return False
            else:
                logging.warning('Unknown response "%s" skip once' % answer)
                return False
        else:
            return True

    def input(self, prompt):
        if self.ui:
            ret = self.ui.ask_user(prompt)
            logging.info('%s %s' % (prompt, ret))
        else:
            ret = input(prompt)
        return ret

    def setup_holded_jobs(self):
        # Find all jobs in hold state and set them up.
        # _sis_setup_directory can be called multiple times, it will only create the directory once
        self.thread_pool.map(lambda job: job._sis_setup_directory(), self.jobs.get(gs.STATE_HOLD, []))

    def resume_jobs(self):
        # function to resume jobs:
        def f(job):
            task = job._sis_next_task()
            if task is None:
                # job finished in the meantime
                return

            # clean up
            if task.resumeable():
                if job._sis_setup() or not job._sis_setup_since_restart:
                    if self.ask_user("Resetup job directory (%s)?" % job, ('resetup', job)):
                        job._sis_setup_directory(force=True)
                if self.ask_user("Resubmit job (%s)?" % job, ('resubmit', job)):
                    self.job_engine.submit(task)
            else:
                logging.debug('Skip unresumable task')

        self.thread_pool.map(f, self.jobs.get(gs.STATE_INTERRUPTED, []))

    def run_jobs(self):
        """
        Setup directories and submit next job task to queue
        """

        # function to submit jobs to queue, run in parallel
        def f(job):
            # Setup job directory if not already done since restart
            try:
                job._sis_setup_directory()
            except RuntimeError as e:
                logging.error('Failed to setup %s: %s' % (str(job), str(e)))
                return

            # run first runnable task
            task = job._sis_next_task()
            if task is None:
                # job finished in the meantime
                return
            self.job_engine.submit(task)

        if self.interactive:
            for job in self.jobs.get(gs.STATE_RUNNABLE, []):
                if self.ask_user('Submit job (%s)?' % job, ('submit', job)):
                    f(job)
        else:
            self.thread_pool.map(f, self.jobs.get(gs.STATE_RUNNABLE, []))

    def check_output(self, write_output=False, update_all_outputs=False):
        targets = self.sis_graph.targets if update_all_outputs else self.sis_graph.active_targets
        for target in targets:
            target.update_requirements()
            if target.is_done():
                target.run_when_done(write_output=write_output)
                self.sis_graph.remove_from_active_targets(target)

    def continue_manager_loop(self):
        # Stop loop flag is set
        if self._stop_loop:
            logging.info("Manager loop stopped")
            return False

        # Keep running if any config reader is not finished reading
        if toolkit.config_reader_running():
            return True

        # Stop loop if all outputs are computed
        if self.stop_if_done and len(self.sis_graph.active_targets) == 0:
            logging.info("All output calculated")
            return False

        # or nothing is left to do
        return self.work_left()

    def is_paused(self):
        return self._paused

    def pause(self):
        self._paused = True

    def unpause(self):
        self._paused = False

    def startup(self):
        if gs.MEMORY_PROFILE_LOG:
            if tools.tracemalloc:
                self.mem_profile = tools.MemoryProfiler(open(gs.MEMORY_PROFILE_LOG, 'w'))
            else:
                logging.warning('Could not load tracemalloc, continue without memory profiling')
        else:
            self.mem_profile = None

        self.job_engine.reset_cache()
        self.check_output(write_output=False, update_all_outputs=True)
        self.update_jobs()
        self.update_state_overview()

        toolkit.check_for_exceptions()

        # Skip first part if there is nothing todo
        if not (toolkit.config_reader_running() or self.jobs or self.ui):
            answer = self.input('All calculations are done, print verbose overview (v), update outputs and alias (u), '
                                'cancel (c)? ')
            if answer.lower() in ('y', 'v'):
                self.print_state_overview(verbose=True)
            elif answer.lower() in ('u'):
                self.link_outputs = True
                create_aliases(self.sis_graph.jobs())
                self.check_output(write_output=self.link_outputs, update_all_outputs=True)
            return

        self.print_state_overview()

        answer = None
        if gs.STATE_ERROR in self.jobs:
            if self.clear_errors_once:
                self.clear_states(state=gs.STATE_ERROR)
                self.clear_errors_once = False
            elif self.ignore_once:
                pass
            else:
                answer = self.input('Clear jobs in error state? [y/N] ')
                if answer.lower() == 'y':
                    self.clear_states(state=gs.STATE_ERROR)
                    self.clear_errors_once = False
                    self.print_state_overview(verbose=False)
                answer = None
        if (gs.STATE_INTERRUPTED in self.jobs) and self.clear_interrupts_once:
            self.clear_states(state=gs.STATE_INTERRUPTED)
            self.clear_interrupts_once = False

        if self.start_computations:
            answer = 'y'

        while True and not self.ui:
            if answer is None:
                pass
            elif answer.lower() == 'v':
                self.print_state_overview(verbose=True)
            elif answer.lower() == 'y':
                self.link_outputs = True
                create_aliases(self.sis_graph.jobs())
                self.check_output(write_output=self.link_outputs, update_all_outputs=True)
                break
            elif answer.lower() == 'u':
                self.link_outputs = True
                create_aliases(self.sis_graph.jobs())
                self.check_output(write_output=self.link_outputs, update_all_outputs=True)
            elif answer.lower() == 'n':
                self.stop()
                break
            else:
                logging.warning('Unknown command: %s' % answer)
            answer = self.input('Print verbose overview (v), update aliases and outputs (u), '
                                'start manager (y), or exit (n)? ')

        if (not self._stop_loop) and (gs.CLEAR_ERROR or self.clear_errors_once):
            self.clear_states(state=gs.STATE_ERROR)
            self.clear_errors_once = False

    @tools.default_handle_exception_interrupt_main_thread
    def run(self):
        try:
            asyncio.get_event_loop().run_until_complete(self.async_run())
        except KeyboardInterrupt:
            pass

    async def async_run(self):
        self.startup()
        last_state_overview = self.state_overview
        while self.continue_manager_loop():
            # Don't to anything while the manager is paused
            if self.is_paused():
                await asyncio.sleep(1)
                continue
            # check if finished
            logging.debug('Begin of manager loop')

            toolkit.check_for_exceptions()

            if self.mem_profile:
                self.mem_profile.snapshot()
            self.job_engine.reset_cache()
            self.check_output(write_output=self.link_outputs)

            self.update_jobs()

            if (gs.CLEAR_ERROR or self.clear_errors_once):
                self.clear_errors_once = False
                if self.clear_states(state=gs.STATE_ERROR):
                    continue

            if self.auto_print_stat_overview:
                self.update_state_overview()
                if last_state_overview != self.state_overview:
                    if self.ui:
                        self.ui.update_job_view(self.get_job_states())
                        self.ui.update_state_overview(' '.join(sorted(self.state_overview)))
                    else:
                        self.print_state_overview()
                    last_state_overview = self.state_overview

            if gs.STATE_RUNNABLE not in self.jobs:
                # Not thing to do right now, wait for jobs to finish
                # otherwise continue directly
                logging.debug("Wait for %i seconds" % gs.WAIT_PERIOD_BETWEEN_CHECKS)
                await asyncio.sleep(gs.WAIT_PERIOD_BETWEEN_CHECKS)

            self.setup_holded_jobs()
            self.resume_jobs()
            self.run_jobs()

        # Stop config reader
        toolkit.cancel_all_config_reader()

        self.check_output(write_output=self.link_outputs, update_all_outputs=True)

        self.job_engine.stop_engine()
        if self.job_cleaner:
            self.job_cleaner.close()


def create_aliases(jobs):
    # first scan jobs for aliases
    aliases = {}
    alias_dirs = set()
    for job in jobs:
        orig_aliases = job.get_aliases()
        if orig_aliases is None:
            continue

        alias_dirs.update(job._sis_alias_prefixes)
        for prefix in job._sis_alias_prefixes:
            for alias in orig_aliases:
                alias = os.path.join(prefix, alias)
                if alias in aliases:
                    logging.warning('Alias %s is used multiple times:' % alias)
                    logging.warning('First use: %s' % aliases[alias])
                    logging.warning('Additional use: %s' % job.job_id())
                else:
                    aliases[alias] = job.job_id()

    # is there anything to do?
    if len(aliases) <= 0:
        return

    # create alias dir
    for d in alias_dirs:
        d = os.path.join(gs.ALIAS_DIR, d)
        if not os.path.isdir(d):
            os.makedirs(d)

    # create the symlinks
    for alias, target in aliases.items():
        alias = os.path.join(gs.ALIAS_DIR, alias)
        target = os.path.join(gs.WORK_DIR, target)

        if os.path.islink(alias) and os.path.realpath(alias) != os.path.realpath(target):
            os.unlink(alias)
        if not os.path.islink(alias):
            d = os.path.dirname(alias)
            if not os.path.isdir(d):
                os.makedirs(d)
            os.symlink(os.path.realpath(target), alias)


def start_http_server(sis_graph, sis_engine, port, thread=True):
    from sisyphus import http_server
    return http_server.start(sis_graph=sis_graph,
                             sis_engine=sis_engine,
                             port=port,
                             thread=thread)


def init_IPython_kernel(user_ns={}):
    try:
        from ipykernel.kernelapp import IPKernelApp
        import atexit
        import socket

        ip = socket.gethostbyname(socket.gethostname())
        connection_file = "%s/ipython-kernel-%s-%s.json" % (os.path.abspath('.'), ip, os.getpid())

        def cleanup_connection_file():
            try:
                os.remove(connection_file)
            except (IOError, OSError):
                pass
        atexit.register(cleanup_connection_file)
    except Exception as e:
        logging.error("Error while loading IPython kernel, continue without. %s" % e)
        return
    try:
        app = IPKernelApp.instance(user_ns=user_ns)

        # disable signals since they need to run in the main thread
        app.init_signal = lambda: None
        app.initialize(['-f', connection_file])
        app.kernel.pre_handler_hook = lambda: None
        app.kernel.post_handler_hook = lambda: None
        from IPython.core.autocall import ZMQExitAutocall

        class KeepAlive(ZMQExitAutocall):
            def __call__(self):
                super().__call__(True)
        app.shell.exiter = KeepAlive()

        # start thread
        thread = threading.Thread(target=app.start)
        thread.daemon = True
        thread.start()
        return connection_file
    except Exception as e:
        logging.error("Error while starting IPython kernel, continue without. %s" % e)
        return
