"""
Add color to logging output

"""

import logging

__author__ = "Jan-Thorsten Peter"
__email__ = "peter@cs.rwth-aachen.de"

color_end_marker = "\x1b[0m"


def color_mapping(levelno):
    if levelno >= 50:
        return "\x1b[31m"  # red
    elif levelno >= 40:
        return "\x1b[31m"  # red
    elif levelno >= 30:
        return "\x1b[33m"  # yellow
    elif levelno >= 20:
        return "\x1b[32m"  # green
    elif levelno >= 10:
        return "\x1b[35m"  # pink
    else:
        return "\x1b[0m"  # normal


def add_coloring_to_emit_ansi(func):
    """
    Adding color output to the python logging.StreamHandler
    coloring code take from:
    stackoverflow.com/questions/384076/how-can-i-color-python-logging-output
    """

    def add_color(*args):
        """hidden function to change color"""
        levelno = args[1].levelno
        color = color_mapping(levelno)
        args[1].msg = color + str(args[1].msg) + color_end_marker  # normal
        # print "after"
        return func(*args)

    return add_color


def add_coloring_to_logging():
    """
    Adds colors to logging output

    """
    logging.StreamHandler.emit = add_coloring_to_emit_ansi(logging.StreamHandler.emit)
