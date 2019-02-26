import os
import sys
import subprocess
import time
import logging
import pprint
import urwid
from queue import Queue
from threading import Semaphore
import sisyphus.global_settings as gs
from sisyphus.logging_format import color_end_marker, color_mapping
from sisyphus.job import Job
from sisyphus.job_path import Path


class RightButton(urwid.Button):
    def keypress(self, size, key):
        if key not in ('enter', ' ', 'right'):
            return key
        self._emit('click')


# This handler should be given to the logging. It will forward all given messages to the ui and write
# them into a log file.

class UiLoggingHandler(logging.Handler):
    def __init__(self, logger_box, redraw, log_file=None):
        logging.Handler.__init__(self)
        self.redraw = redraw
        self.logger_box = logger_box
        self.log_file = log_file

    def emit(self, record):

        levelno = record.levelno
        self.format(record)  # to create asctime

        msg = record.getMessage()
        if self.log_file:
            color = color_mapping(levelno)
            self.log_file.write('%s %s %s%s%s\n' % (record.asctime, record.levelname, color, msg, color_end_marker))
            self.log_file.flush()

        if(levelno >= 40):
            color = 'error'
        elif(levelno >= 30):
            color = 'warning'
        elif(levelno >= 20):
            color = 'info'
        else:
            color = 'debug'
        self.logger_box.append(urwid.Text([record.asctime, ' ', record.levelname, ' ',
                                           (color, record.getMessage())], wrap='clip'))
        # Keep a small buffer, everything else should be handled via the logfile
        while len(self.logger_box) > 40:
            del self.logger_box[0]

        self.logger_box.set_focus(len(self.logger_box)-1)
        self.redraw()


help_text = """Welcome to the sisyphus manager interface and its currently very short help page

%s
Quit the program by pressing esc or q
Press enter While the log window is active to view the whole log history with less
Press enter on selected job to see more details
Enter object view with enter, space or right arrow key
Go back to last object with backspace or left arrow key
""" % gs.SISYPHUS_LOGO


class SisyphusDisplay:
    palette = [
        ('body',         'light gray', 'black', 'standout'),
        ('note',         'black', 'light gray', 'standout'),
        ('header',       'white',      'dark red',   'bold'),
        ('button normal', 'light gray', 'black', 'standout'),
        ('button select', 'white',      'dark green'),
        ('exit',         'white',      'dark cyan'),

        ('question',      'dark red',      'yellow', 'bold'),

        ('error',         'dark red',      'black', 'standout'),
        ('warning',       'yellow',      'black', 'standout'),
        ('info',          'dark green',      'black', 'standout'),
        ('debug',         'light magenta',      'black', 'standout'),
        ('logger',        'light gray',      'black', 'standout'),
        ]

    def job_selected(self, w, job):
        d = job.__dict__
        d['____NAME__'] = job._sis_path()
        self.obj_selected(w, job)

    def show_items(self, items, length=80):
        # Empty list
        self.obj_body.body = []

        for k, v in items:
            if isinstance(v, Path):
                label = '%s %s<%s>' % (k, type(v).__name__, repr(v))
            else:
                label = '%s %s' % (k, pprint.pformat(v))
            button = RightButton(label, on_press=self.obj_selected, user_data=v)
            button = urwid.AttrWrap(button, 'button normal', 'button select')
            self.obj_body.body.append(button)

    def show_job(self, job):
        items = []
        attributes = job.__dict__
        for name in ('_sis_kwargs', '_sis_aliases', '_sis_keep_value', '_sis_stacktrace',
                     '_sis_blocks', '_sis_tags', '_sis_task_rqmt_overwrite', '_sis_vis_name',
                     '_sis_outputs', '_sis_environment'):
            attr = attributes[name]
            if attr:
                items.append((name[5:], attr))

        for k, v in attributes.items():
            if not k.startswith('_sis_'):
                items.append((k, v))

        self.show_items(items)

        self.loop.widget = self.obj_view
        self.redraw()

    def obj_selected(self, w, obj=None):
        self.stop_job_view_update = True
        self.obj_header.set_text(str(type(obj)))
        if isinstance(obj, Job):
            self.obj_header.set_text(obj._sis_path())
            self.show_job(obj)
        elif isinstance(obj, (tuple, list, set)):
            self.show_items([(str(pos), v) for pos, v in enumerate(obj)])
        elif isinstance(obj, (dict)):
            self.show_items(sorted((repr(k), v) for k, v in obj.items()))
        elif hasattr(obj, '__dict__'):
            self.show_items(sorted(obj.__dict__.items()))
        else:
            self.obj_body.body = [urwid.Text(repr(obj))]

        self.loop.widget = self.obj_view
        self.history.append(obj)
        self.redraw()

    def setup_view(self):
        self.logger_box = urwid.Text("Logging Box", wrap='clip')
        self.logger_box = urwid.SimpleListWalker([])

        self._state_overview = urwid.Text("Starting")
        # ListBox
        self.job_box = urwid.ListBox(urwid.SimpleListWalker([]))

        w = urwid.Pile([
             urwid.AttrWrap(self.job_box, 'body')])

        # Frame
        hdr = urwid.Text("Sisyphus | CWD: %s | Call: %s | Press h for help | press q or esc to quit" % \
                         (os.path.abspath('.'), ' '.join(sys.argv)), wrap='clip')
        self.header = hdr = urwid.AttrWrap(hdr, 'header')
        hdr = urwid.Pile([hdr,
                          (10, urwid.AttrWrap(urwid.ListBox(self.logger_box), 'body')),
                          urwid.AttrWrap(self._state_overview, 'note'),
                                ])

        self.main_view = urwid.Frame(header=hdr, body=w)

        # Exit message
        exit = urwid.BigText(('exit'," Quit? "), font=urwid.Thin6x6Font())
        self.exit_view = urwid.Overlay(exit, w, 'center', None, 'middle', None)

        self.question_text = urwid.Text("  ")
        self.question_queue = Queue()
        self.question_sem = Semaphore()
        self.question_view = urwid.Overlay(self.question_text, self.main_view,
                                           'center', ('relative', 80), 'middle', None)

        help = urwid.Text(help_text)
        self.help_view = urwid.Frame(header=self.header, body=urwid.ListBox([help]))

        self.setup_object_view()

        self.history = []
        self.stop_job_view_update = False
        self.current_jobs = []

    def setup_object_view(self):
        self.obj_header = urwid.Text("Name of object")
        hdr = urwid.Pile([self.header,
                          (10, urwid.AttrWrap(urwid.ListBox(self.logger_box), 'body')),
                          urwid.AttrWrap(self._state_overview, 'note'),
                          self.obj_header
                          ])
        self.obj_body = urwid.ListBox(urwid.SimpleListWalker([]))
        self.obj_view = urwid.Frame(header=hdr, body=self.obj_body)

    def update_job_view(self, jobs=None):
        if jobs:
            self.current_jobs = jobs

        if self.stop_job_view_update:
            return

        # Empty box
        self.job_box.body = []

        for state, job, info in self.current_jobs:
            if state in (gs.STATE_WAITING, gs.STATE_INPUT_PATH):
                continue

            if state in [gs.STATE_INPUT_MISSING,
                         gs.STATE_RETRY_ERROR,
                         gs.STATE_ERROR]:
                attri = 'error'
            elif state in [gs.STATE_INTERRUPTED, gs.STATE_UNKNOWN]:
                attri = 'warning'
            elif state in [gs.STATE_QUEUE,
                           gs.STATE_RUNNING,
                           gs.STATE_RUNNABLE,
                           gs.STATE_FINISHED
                           ]:
                attri = 'info'
            else:
                attri = None

            button = RightButton('%s %s' % (state, info), on_press=self.obj_selected, user_data=job)
            button = urwid.AttrWrap(button, attri, 'button select')
            self.job_box.body.append(button)

    def update_state_overview(self, prompt):
        self._state_overview.set_text(prompt)
        self.redraw()

    def ask_user(self, prompt):
        with self.question_sem:
            self.question_text.set_text(('question', prompt))
            self.loop.widget = self.question_view
            self.redraw()
            return self.question_queue.get()

    def setup(self):
        self.logs = []
        self.setup_view()
        self.loop = urwid.MainLoop(self.main_view, self.palette, unhandled_input=self.unhandled_input)
        self._external_event_pipe = self.loop.watch_pipe(self.external_event_handler)

    def external_event_handler(self, msg):
        for msg in msg.decode().split('\n'):
            if msg == 'start_manager':
                logging.info('Start manager')
            elif msg == 'redraw' or msg == '':
                pass
            else:
                logging.warning('Unknown external event: %s' % repr(msg))

    def redraw(self):
        os.write(self._external_event_pipe, b'redraw\n')

    def get_log_handler(self, log_file='log/manager.log.%s' % time.strftime('%Y%m%d%H%M%S')):
        self.log_file = log_file
        dir = os.path.dirname(log_file)
        if not os.path.isdir(dir):
            os.mkdir(dir)
        return UiLoggingHandler(self.logger_box, self.redraw, log_file=open(log_file, 'w'))

    def run(self):
        self.loop.run()

    def reset_view(self):
        self.loop.widget = self.main_view
        self.stop_job_view_update = False
        self.update_job_view()

    def unhandled_input(self, key):
        exit_keys = ('esc', 'q')
        if self.loop.widget == self.question_view:
            if isinstance(key, str) and (len(key) == 1 or key in exit_keys):
                self.question_queue.put(key)
                self.question_text.set_text('')
                self.reset_view()
                return True

        if key in ('h', 'H'):
            self.loop.widget = self.help_view
            return True

        if self.loop.widget == self.exit_view:
            if key in ('y', 'Y', 'enter'):
                self.question_queue.put(None)
                self.manager.stop()
                raise urwid.ExitMainLoop()
            else:
                self.reset_view()
                return True

        if key in ('left', 'backspace'):
            if self.history:
                # Drop current view
                self.history.pop()
            if self.history:
                obj = self.history.pop()
            else:
                obj = None

            if obj is None:
                self.reset_view()
            else:
                self.obj_selected(None, obj=obj)
            return True

        if key in exit_keys:
            if self.loop.widget == self.main_view:
                self.loop.widget = self.exit_view
            else:
                self.reset_view()
            return True

        if key == 'up':
            self.main_view.set_focus('header')
            self.obj_view.set_focus('header')
            return True
        elif key == 'down':
            self.main_view.set_focus('body')
            self.obj_view.set_focus('body')
            return True
        elif self.main_view.get_focus() == 'header' and key in ('up', 'enter'):
            self.loop.stop()
            subprocess.call(['less', '-r', '+G', self.log_file])
            self.loop.start()
            return True

        logging.debug("Unhandled input: %s" % str(key))
