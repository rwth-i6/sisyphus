#!/usr/bin/env python3
# encoding: utf-8

"""
Framework to setup complex work flows

"""

import argparse
import sys

from sisyphus.worker import worker
from sisyphus.manager import manager
from sisyphus.helper import console
import sisyphus.global_settings as gs

# Setup logging
import logging
import sisyphus.logging_format

__author__ = "Jan-Thorsten Peter, Eugen Beck"
__email__ = "peter@cs.rwth-aachen.de"


def main():
    """ Parses command line arguments and executes commands """
    # Setup argument parser
    parser = argparse.ArgumentParser(description=gs.SISYPHUS_LOGO,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    subparsers = parser.add_subparsers()

    parser.add_argument('--log_level', dest='log_level', metavar='LOG_LEVEL',
                        type=int, default=20, help='log level, 10 for debug messages, 50 for only critical,'
                                                   ' default: 20, ')

    parser.add_argument('--config', dest='config_files', action='append',
                        default=[], help='config file for setup, defining which jobs to run')

    # Not fully supported at the moment
    # parser.add_argument('--settings', dest='settings_file',
    #                     default=gs.GLOBAL_SETTINGS_FILE_DEFAULT,
    #                     help='settings file, aka how to run the jobs')

    # parser.add_argument('-s', '--setting', dest='commandline_settings', action='append',
    #                     default=[], help='overwrite global settings directly via commandline')

    parser_manager = subparsers.add_parser('manager', aliases=['m'],
                                           conflict_handler='resolve',
                                           help="Load config files and start manager loop")
    parser_manager.set_defaults(func=manager)
    parser_manager.add_argument("-r", dest="run", default=False,
                                action='store_true', help="Run the given task")
    parser_manager.add_argument("-co", dest="clear_errors_once", action="store_true",
                                default=False,
                                help="Move jobs aside that are in an error "
                                     "state when the manager runs the first time")
    parser_manager.add_argument("-cio", dest="clear_interrupts_once", action="store_true",
                                default=False,
                                help="Move jobs aside that are in an interrupt "
                                     "state when the manager runs the first time")
    parser_manager.add_argument("-io", dest="ignore_once", action="store_true",
                                default=False,
                                help="Ignore jobs that are in an error "
                                     "state when the manager runs the first time")
    parser_manager.add_argument("--http", dest="http_port", default=None,
                                type=int, help="Enables http server, takes "
                                               "port as argument")
    parser_manager.add_argument("--fs", "--filesystem", dest="filesystem",
                                default=None,
                                help="Start filesystem in given directory")
    parser_manager.add_argument("-i", "--interactive", dest="interactive",
                                default=False, action="store_true",
                                help="Ask before submitting jobs")
    parser_manager.add_argument("--ui", dest="ui",
                                default=False, action="store_true",
                                help="Start user interface")
    parser_manager.add_argument('argv', metavar='ARGV', type=str,
                                nargs='*',
                                help='an additional way do '
                                     'define config files')

    parser_console = subparsers.add_parser('console',  aliases=['c'],
                                           usage='sis console [-h] [--load LOAD_SIS_GRAPH] [ARGV [ARGV ...]]\n\n'
                                                 'Open console to debug sisyphus graph or job',
                                           help="Start console to interactively work on sis graph. Things like: "
                                                "Rerunning tasks, cleaning up the work directory, and debugging "
                                                "is best done here")
    parser_console.add_argument("--load", dest="load", default=[], action='append',
                                help="load graph and start console")
    parser_console.add_argument("--skip_config", dest="not_load_config", default=False, action='store_true',
                                help="do not load config files before starting the console")
    parser_console.add_argument("-c", dest="commands", default=[], action='append',
                                help="Run commands after loading console")
    parser_console.add_argument('argv', metavar='ARGV', type=str, nargs='*',
                                help='an additional way do define config files')
    parser_console.set_defaults(func=console)

    parser_worker = subparsers.add_parser(gs.CMD_WORKER, help='Start worker to compute job (for internally usage)')
    parser_worker.set_defaults(func=worker)
    parser_worker.add_argument('jobdir', metavar='JOBDIR', type=str,
                               help='Job directory of the executed function')
    parser_worker.add_argument('task_name', metavar='TASK_NAME', type=str,
                               help='Task name')
    parser_worker.add_argument('task_id', metavar='TASK_ID', type=int,
                               nargs='?', default=None,
                               help='Task id, if not set trying to '
                                    'read it from environment variables')
    parser_worker.add_argument('--force_resume', "--fr", default=False, action="store_true",
                               help='force resume of non resumable tasks, good for debugging')
    parser_worker.add_argument('--engine', default='short', help='The engine running the Job')
    parser_worker.add_argument('--redirect_output', default=False, action='store_true',
                               help='Redirect stdout and stderr to logfile')

    # Currently disabled parser, maybe used again in the future
    #
    # Disabled, since it's not used for now
    # parser_unittest = subparsers.add_parser('unittest', help='Run unittest tasks')
    # parser_unittest.set_defaults(func=unittest)
    # parser_unittest.add_argument("--do_not_run", dest="run", default=True, action='store_false',
    #                              help="Do not run the given task")
    # parser_unittest.add_argument('argv', metavar='ARGV', type=str, nargs='*',
    #                              help='an additional way do define config files')

    # Disabled since it's currently not working
    # parser_notebook = subparsers.add_parser('notebook',
    #                                         usage='sis notebook [-h]\n\n'
    #                                               'Open notebook session',
    #                                         help="Start notebook to interactively work on sis graph")
    # parser_notebook.add_argument("--load", dest="load", default=[], action='append',
    #                              help="load graph and start console")
    # parser_notebook.add_argument("--file", "-f", dest="filename", default='default',
    #                              help="load (and create if needed) this notebook file")
    # parser_notebook.set_defaults(func=helper.notebook)

    # Disabled since it's currently not working
    # parser_connect = subparsers.add_parser('connect', usage='sis connect [-h] [connect_file]\n\n'
    #                                                         'Opens a console connected to given kernel',
    #                                        help='Opens a console connected with given kernel')
    # parser_connect.add_argument('argv', metavar='ARGV', type=str, nargs='?',
    #                             default=[], help='connection_file')
    # parser_connect.set_defaults(func=helper.connect)

    args = parser.parse_args()

    if not hasattr(args, 'func'):
        parser.print_help()
        return

    # add argv to config_files if manager or console is called
    if args.func in [manager, console]:
        args.config_files += args.argv

    # Setup logging colors
    sisyphus.logging_format.add_coloring_to_logging()

    # Setup ui loggign to ui or to commandline
    ui = None
    if args.func == manager and args.ui:
        from sisyphus.manager_ui import SisyphusDisplay
        ui = SisyphusDisplay()
        ui.setup()
        logging.basicConfig(#stream=ui.get_logging_pipe(),
                            format='[%(asctime)s] %(levelname)s: %(message)s',
                            level=args.log_level,
                            handlers=[ui.get_log_handler()])
    else:
        logging.basicConfig(format='[%(asctime)s] %(levelname)s: %(message)s', level=args.log_level)

    # Changing settings via commandline is currently not supported
    # Needs to ensure all parameters are passed correctly to worker, ignored since nobody requested it so far
    # update_global_settings_from_file(args.settings_file)
    # update_global_settings_from_list(args.commandline_settings)

    if gs.USE_VERBOSE_TRACEBACK:
        if gs.VERBOSE_TRACEBACK_TYPE == "ipython":
            from IPython.core import ultratb
            sys.excepthook = ultratb.VerboseTB()
        elif gs.VERBOSE_TRACEBACK_TYPE == "better_exchook":
            # noinspection PyPackageRequirements
            import better_exchook
            better_exchook.install()
            better_exchook.replace_traceback_format_tb()
        else:
            raise Exception("invalid VERBOSE_TRACEBACK_TYPE %r" % gs.VERBOSE_TRACEBACK_TYPE)

    if gs.USE_SIGNAL_HANDLERS:
        from sisyphus.tools import maybe_install_signal_handers
        maybe_install_signal_handers()

    if args.func != manager:
        gs.JOB_AUTO_CLEANUP = False

    try:
        if ui:
            import threading
            ui.manager = None
            args.ui = ui
            t = threading.Thread(target=args.func, args=(args,))
            t.start()
            ui.run()
            t.join()
        else:
            args.func(args)
    except BaseException as exc:
        if not isinstance(exc, SystemExit):
            logging.error("Main thread unhandled exception:")
            sys.excepthook(*sys.exc_info())
        import threading
        non_daemon_threads = {
            thread for thread in threading.enumerate()
            if not thread.daemon and thread is not threading.main_thread()}
        if non_daemon_threads:
            logging.warning("Main thread exit. Still running non-daemon threads: %r" % non_daemon_threads)


if __name__ == '__main__':
    main()
