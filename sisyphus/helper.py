import logging

from sisyphus.loader import config_manager
import sisyphus.toolkit
import sisyphus.manager


def console(args):
    """Start an interactive ipython console"""

    user_ns = {
        "tk": sisyphus.toolkit,
        "config_files": args.config_files,
    }

    if args.load:
        jobs = []
        for job in args.load:
            sisyphus.toolkit.set_root_block(job)
            jobs.append(sisyphus.toolkit.load_job(job))
        user_ns["jobs"] = jobs
        for i, job in enumerate(jobs):
            print("jobs[%i]: %s" % (i, job))
    elif not args.not_load_config:
        config_manager.load_configs(args.config_files)

    if args.script:
        cmd = ";".join(args.commands)
        logging.info("Running: %s" % cmd)
        exec(cmd, user_ns)
        return

    # TODO Update welcome message
    welcome_msg = """
Info: IPCompleter.greedy = True is set to True.
This allows to auto complete lists and dictionaries entries, but may evaluates functions on tab.

Enter tk? for help"""

    import IPython
    from traitlets.config.loader import Config

    c = Config()
    c.InteractiveShell.banner2 = welcome_msg
    c.IPCompleter.greedy = True
    c.InteractiveShellApp.exec_lines = ["%rehashx"] + args.commands
    IPython.start_ipython(config=c, argv=[], user_ns=user_ns)
