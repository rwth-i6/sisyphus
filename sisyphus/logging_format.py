"""
Add color to logging output

"""

import logging

__author__ = "Jan-Thorsten Peter"
__email__ = "peter@cs.rwth-aachen.de"


def add_coloring_to_emit_ansi(func):
    """
    Adding color output to the python logging.StreamHandler
    coloring code take from:
    stackoverflow.com/questions/384076/how-can-i-color-python-logging-output
    """
    def color_mapping(*args):
        """ hidden function to change color """
        levelno = args[1].levelno
        if(levelno >= 50):
            color = '\x1b[31m'  # red
        elif(levelno >= 40):
            color = '\x1b[31m'  # red
        elif(levelno >= 30):
            color = '\x1b[33m'  # yellow
        elif(levelno >= 20):
            color = '\x1b[32m'  # green
        elif(levelno >= 10):
            color = '\x1b[35m'  # pink
        else:
            color = '\x1b[0m'  # normal
        args[1].msg = color + args[1].msg + '\x1b[0m'  # normal
        # print "after"
        return func(*args)
    return color_mapping


def add_coloring_to_logging():
    """
    Adds colors to logging output

    """
    logging.StreamHandler.emit = add_coloring_to_emit_ansi(
        logging.StreamHandler.emit)
