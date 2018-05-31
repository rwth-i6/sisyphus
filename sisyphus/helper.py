import logging
import os
import random
import shutil
import socket
import sys
from ast import literal_eval
from glob import glob

from sisyphus.loader import load_configs
import sisyphus.global_settings as gs

import sisyphus.tools as tools
import sisyphus.engine as engine
import sisyphus.graph as graph
import sisyphus.toolkit
import sisyphus.manager


def console(args):
    """ Start an interactive ipython console """

    user_ns = {'tk': sisyphus.toolkit,
               'config_files': args.config_files,
               }

    if args.load:
        jobs = [sisyphus.toolkit.load_job(i) for i in args.load]
        user_ns['jobs'] = jobs
        for i, job in enumerate(jobs):
            print("jobs[%i]: %s" % (i, job))
    elif not args.not_load_config:
        load_configs(args.config_files)

    # TODO Update welcome message
    welcome_msg = """
Info: IPCompleter.greedy = True is set to True.
This allows to auto complete lists and dictionaries entries, but may evaluates functions on tab.

Enter tk? for help"""

    import IPython
    from traitlets.config.loader import Config
    c = Config()
    c.InteractiveShellApp.exec_lines = [
                                        # register load_job and load_configs as magic
                                        # 'import IPython.core.magic\n'
                                        # 'shut_up=IPython.core.magic.register_line_magic(tk.load_job)\n'
                                        # 'shut_up=IPython.core.magic.register_line_magic(tk.load_configs)\n',

                                        # settings that make the ipython console behave more like a system console
                                        # 'del shut_up',  # shut_up is used to silence the return value
                                        # 'initialize()',
                                        '%rehashx',
                                        # '%autocall',
                                        '%config IPCompleter.greedy = True',
                                        'print(%s)' % repr(welcome_msg)] + args.commands

    IPython.start_ipython(config=c, argv=[], user_ns=user_ns)


# ### Notebook stuff, needs more testing
# TODO currently not working
def notebook(args):
    """ Starts interactive notebook session """

    notebook_file = args.filename

    import IPython
    from IPython.lib import passwd
    from socket import gethostname
    if not notebook_file.endswith('.ipynb'):
        notebook_file += '.ipynb'

    if not os.path.isfile(notebook_file):
        with open(notebook_file, 'w') as f:
            f.write("""{
 "metadata": {
  "name": "",
  "signature": "sha256:0f5ff51613f8ce0a6edf3b69cfa09d0dbecf33f4c03078419f58189b6059a373"
 },
 "nbformat": 3,
 "nbformat_minor": 0,
 "worksheets": [
  {
   "cells": [
    {
     "cell_type": "code",
     "collapsed": false,
     "input": [
      "from sisyphus.notebook import *\\n",
      "tk.gs.SIS_COMMAND = ['../sis']"
     ],
     "language": "python",
     "metadata": {},
     "outputs": [],
     "prompt_number": 1
    },
    {
     "cell_type": "code",
     "collapsed": false,
     "input": [
      "manager.load_file('config.py')"
     ],
     "language": "python",
     "metadata": {},
     "outputs": [],
     "prompt_number": 2
    },
    {
     "cell_type": "code",
     "collapsed": false,
     "input": [
      "manager.start()"
     ],
     "language": "python",
     "metadata": {},
     "outputs": []
    }
   ],
   "metadata": {}
  }
 ]
}""")

    password = gs.SIS_HASH(random.random())

    argv = []
    argv.append("notebook")
    argv.append(notebook_file)
    argv.append("--IPKernelApp.pylab='inline'")
    argv.append("--NotebookApp.ip=" + gethostname())
    argv.append("--NotebookApp.open_browser=False")
    argv.append("--NotebookApp.password=" + passwd(password))

    print("Notebook password: %s" % password)

    IPython.start_ipython(argv=argv)


# TODO currently not working
def connect(args):
    if len(args.argv) == 1:
        connect_file = args.argv[0]
    else:
        files = glob("ipython-kernel-*.json")
        files = [(os.path.getmtime(i), i) for i in files]
        files.sort()
        if len(files) > 0:
            connect_file = files[-1][1]
        else:
            connect_file = None
    assert connect_file and os.path.isfile(connect_file), "No connection file found"

    argv = []
    argv.append("console")
    argv.append("--existing=%s" % os.path.abspath(connect_file))
    logging.info("Connect to %s" % connect_file)
    import IPython
    IPython.start_ipython(argv=argv)
