import subprocess
import time
import sisyphus.toolkit as tk
from sisyphus.loader import config_manager


def add_subparsers(parsers):
    parser_shortcuts = parsers.add_parser("shortcut", aliases=["s"], help="Starts console and runs given task")
    sc_subparsers = parser_shortcuts.add_subparsers()

    parser_clean_unused = sc_subparsers.add_parser("clean_unused", help="Remove all files not in the current graph")
    parser_clean_unused.add_argument(
        "--used_path",
        default=None,
        help="File where the found paths are saved, paths will be appended if the " "file already exists",
    )
    parser_clean_unused.add_argument(
        "--load_used_path", default=None, help="Skip finding the used paths and load them from this file instead"
    )
    parser_clean_unused.add_argument(
        "--to_remove",
        default=None,
        help="File where the paths that should be removed are saved. "
        "The file will be overwritten, if it already exists",
    )
    parser_clean_unused.add_argument(
        "--only_find_used_paths", default=False, action="store_true", help="Stop after writing the used path file"
    )
    parser_clean_unused.add_argument(
        "--skip_remove",
        default=False,
        action="store_true",
        help="To not remove found files, stop after writing the to remove file",
    )
    parser_clean_unused.add_argument(
        "--mode",
        default="remove",
        help="'remove' or 'move' defines if unneeded directories should " "be moved or removed",
    )
    parser_clean_unused.add_argument(
        "--move_postfix", default=".cleanup", help="Which postfix should be attached to moved directories"
    )
    parser_clean_unused.add_argument(
        "argv", metavar="ARGV", type=str, nargs="*", help="All config files that will be loaded"
    )
    parser_clean_unused.set_defaults(func=clean_unused)

    parser_clean_jobs = sc_subparsers.add_parser(
        "clean_jobs", help="Remove work directories and compress job log files for all jobs " "inside the graph"
    )
    parser_clean_jobs.add_argument(
        "argv", metavar="ARGV", type=str, nargs="*", help="All config files that will be loaded"
    )
    parser_clean_jobs.set_defaults(func=clean_jobs)

    parser_clean_by_keep_value = sc_subparsers.add_parser(
        "clean_by_keep_value", help="Remove all jobs with a too low keep value"
    )
    parser_clean_by_keep_value.add_argument(
        "--used_path",
        default=None,
        help="File where the found keep_values are saved, paths will be appended " "if the file already exists",
    )
    parser_clean_by_keep_value.add_argument(
        "--load_used_path", default=None, help="Skip finding the keep values and load them from this file instead"
    )
    parser_clean_by_keep_value.add_argument(
        "--to_remove",
        default=None,
        help="File where the paths that should be removed are saved. " "File will be overwritten, if it already exists",
    )
    parser_clean_by_keep_value.add_argument(
        "--only_find_used_paths", default=False, action="store_true", help="Stop after writing the used path file"
    )
    parser_clean_by_keep_value.add_argument(
        "--skip_remove",
        default=False,
        action="store_true",
        help="To not remove found files, stop after writing the to remove file",
    )
    parser_clean_by_keep_value.add_argument(
        "--keep_value",
        default=0,
        type=int,
        help="Keep value, jobs with a smaller value will be removed. "
        "Jobs have a default keep value of %i, "
        "select an higher value to remove all jobs that don't have keep "
        "value set." % tk.gs.JOB_DEFAULT_KEEP_VALUE,
    )
    parser_clean_by_keep_value.add_argument(
        "--mode",
        default="remove",
        help="'remove' or 'move' defines if unneeded directories should " "be moved or removed",
    )
    parser_clean_by_keep_value.add_argument(
        "--move_postfix", default=".cleanup", help="Which postfix should be attached to moved directories"
    )
    parser_clean_by_keep_value.add_argument(
        "argv", metavar="ARGV", type=str, nargs="*", help="All config files that will be loaded"
    )
    parser_clean_by_keep_value.set_defaults(func=clean_by_keep_value)

    parser_import_from_dir = sc_subparsers.add_parser(
        "import_from_dir", help="Import jobs with same hash from given directory"
    )
    parser_import_from_dir.add_argument("--dir", default=[], action="append", help="Directories used as import source")
    parser_import_from_dir.add_argument(
        "--use_alias", default=False, action="store_true", help="Given directories contains alias"
    )
    parser_import_from_dir.add_argument(
        "argv", metavar="ARGV", type=str, nargs="*", help="All config files that will be loaded"
    )
    parser_import_from_dir.set_defaults(func=import_from_dir)

    parser_remove_job_and_descendants = sc_subparsers.add_parser(
        "remove_job_and_descendants", help="Remove all jobs that depend on any path matching the given string"
    )
    parser_remove_job_and_descendants.add_argument(
        "--path", default=[], action="append", help="Try to match string with path"
    )
    parser_remove_job_and_descendants.add_argument(
        "--job", default=[], action="append", help="Try to match string with job"
    )
    parser_remove_job_and_descendants.add_argument(
        "argv", metavar="ARGV", type=str, nargs="*", help="All config files that will be loaded"
    )
    parser_remove_job_and_descendants.set_defaults(func=remove_job_and_descendants)

    parser_show_jobs_in_webserver = sc_subparsers.add_parser(
        "show_jobs_in_webserver", help="Start webserver which shows all given jobs"
    )
    parser_show_jobs_in_webserver.add_argument("--port", required=True, help="Port for webserver to listen on")
    parser_show_jobs_in_webserver.add_argument(
        "--job", default=[], action="append", help="path to job directory, can be given multiple times"
    )
    parser_show_jobs_in_webserver.set_defaults(func=show_job_in_webserver)


def clean_unused(args):
    if args.load_used_path:
        used_paths_file = args.load_used_path
    else:
        if args.used_path:
            used_paths_file = args.used_path
        else:
            used_paths_file = "used_paths.%s.txt" % time.strftime("%Y%m%d%H%M%S")

        for conf_file in args.argv:
            call = ["console", conf_file, "--script", "-c", 'tk.cleaner.save_used_paths("%s")' % used_paths_file]
            call_sis(call)

    if args.only_find_used_paths:
        return

    if args.to_remove:
        to_remove = args.to_remove
    else:
        to_remove = "to_remove.%s.txt" % time.strftime("%Y%m%d%H%M%S")

    call = [
        "console",
        "--skip_config",
        "--script",
        "-c",
        'tk.cleaner.save_remove_list(tk.cleaner.search_for_unused("%s"), "%s")' % (used_paths_file, to_remove),
    ]
    call_sis(call)

    if args.skip_remove:
        return

    call = [
        "console",
        "--skip_config",
        "--script",
        "-c",
        'tk.cleaner.remove_directories("%s", "Unused directories:", move_postfix="%s", mode="%s")'
        % (to_remove, args.move_postfix, args.mode),
    ]
    call_sis(call)


def clean_jobs(args):
    for conf_file in args.argv:
        call = ["console", conf_file, "--script", "-c", "tk.cleaner.cleanup_jobs()"]
        call_sis(call)


def clean_by_keep_value(args):
    if args.load_used_path:
        used_paths_file = args.load_used_path
    else:
        if args.used_path:
            used_paths_file = args.used_path
        else:
            used_paths_file = "used_keep_values.%s.txt" % time.strftime("%Y%m%d%H%M%S")

        for conf_file in args.argv:
            call = [
                "console",
                conf_file,
                "--script",
                "-c",
                'tk.cleaner.save_used_paths("%s", tk.cleaner.extract_keep_values_from_graph())' % used_paths_file,
            ]
            call_sis(call)

    if args.only_find_used_paths:
        return

    if args.to_remove:
        to_remove = args.to_remove
    else:
        to_remove = "to_remove.%s.txt" % time.strftime("%Y%m%d%H%M%S")

    call = [
        "console",
        "--skip_config",
        "--script",
        "-c",
        'tk.cleaner.save_remove_list(tk.cleaner.find_too_low_keep_value("%s", %i), "%s")'
        % (used_paths_file, args.keep_value, to_remove),
    ]
    call_sis(call)

    if args.skip_remove:
        return

    call = [
        "console",
        "--skip_config",
        "--script",
        "-c",
        'tk.cleaner.remove_directories("%s", "Unused directories:", move_postfix="%s", mode="%s")'
        % (to_remove, args.move_postfix, args.mode),
    ]
    call_sis(call)


def import_from_dir(args):
    call = [
        "console",
        "--script",
        "-c",
        'tk.import_work_directory(%s, mode="symlink", use_alias=%s)' % (args.dir, args.use_alias),
    ] + args.argv
    call_sis(call)


def remove_job_and_descendants(args):
    find = " + ".join(
        ["tk.find_path(%s)" % repr(i) for i in args.path] + ["tk.find_job(%s)" % repr(i) for i in args.job]
    )
    call = ["console", "--script", "-c", "tk.remove_job_and_descendants(%s)" % find] + args.argv
    call_sis(call)


def show_job_in_webserver(args):
    call = ["console", "--script"]
    for job in args.job:
        call += ["--load", job]
    call += ["-c", f"tk.show_jobs_in_webserver({args.port}, jobs)"]
    call_sis(call)


def call_sis(call):
    p = subprocess.Popen(tk.gs.SIS_COMMAND + call, start_new_session=True)
    p.wait()
