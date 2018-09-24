from flask import request, jsonify, render_template, Flask, send_from_directory, url_for, make_response
from functools import wraps
import collections
import html
import logging
import os
import socket
import subprocess as sp
import sys
import threading
import time

from sisyphus.job import Job
from sisyphus.job_path import Path
from sisyphus.tools import cache_result
from sisyphus.visualize import visualize_block
import sisyphus.global_settings as gs
from sisyphus.block import all_root_blocks

app = Flask(__name__)
g_sis_graph = None
g_sis_engine = None

TaskItem = collections.namedtuple('TaskItem', ['name', 'state', 'state_bg_color', 'instances'])
JobItem = collections.namedtuple('JobItem', ['number',
                                             'sis_id',
                                             'name',
                                             'dependencies',
                                             'state',
                                             'state_bg_color',
                                             'tasks',
                                             'tasks_count',
                                             ])
OutputItem = collections.namedtuple('OutputItem', ['name', 'state', 'state_bg_color', 'sis_id'])


def state_to_color(state):
    if state == gs.STATE_FINISHED:
        return 'Green'
    elif state == 'cleaned':
        return 'Green'
    elif state == gs.STATE_RUNNING:
        return 'GreenYellow'
    elif state in [gs.STATE_ERROR, gs.STATE_UNKNOWN, gs.STATE_INTERRUPTED]:
        return 'Red'
    elif state in [gs.STATE_QUEUE, gs.STATE_RUNNABLE]:
        return 'SteelBlue'
    elif state == gs.STATE_WAITING:
        return 'Yellow'
    else:
        return 'White'


@cache_result(15)
def get_tasks_from_job(job):
    tasks = []
    if job._sis_runnable():
        finished = job._sis_finished()
        for task in job._sis_tasks():
            task_state = task.state(g_sis_engine)
            if finished and task_state != gs.STATE_FINISHED:
                task_state = 'cleaned'
            task_state_bg_color = state_to_color(task_state)
            tasks.append(TaskItem(
                task.name(),
                task_state,
                task_state_bg_color,
                len(task.task_ids()))
            )
    return tasks


def add_response_headers(headers={}):
    """This decorator adds the headers passed in to the response"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            resp = make_response(f(*args, **kwargs))
            h = resp.headers
            for header, value in headers.items():
                h[header] = value
            return resp
        return decorated_function
    return decorator


def keepalive(sec):
    return add_response_headers({'Keep-Alive': 'timeout=%d' % sec})


@app.route('/')
@keepalive(2)
def output_view():
    outputs = []
    for name, path in g_sis_graph.output.items():
        if path.creator:
            job = path.creator
            state = job._sis_state(g_sis_engine)
            state_bg_color = state_to_color(state)
            sis_id = job._sis_id()
        else:
            state = ""
            state_bg_color = "White"
            sis_id = str(path)
        outputs.append(OutputItem(name, state, state_bg_color, sis_id))

    outputs.sort(key=lambda x: x.name)
    return render_template("outputs.html", outputs=outputs)


@app.route('/all')
@keepalive(2)
def overview():
    job_list = []
    job_dict = {}

    for job in g_sis_graph.jobs_sorted():
        dependencies = set()
        for i in job._sis_inputs:
            if i.creator:
                dependencies.add(
                    job_dict[i.creator._sis_id()]
                )
        state = job._sis_state(g_sis_engine)
        bg_color = state_to_color(state)

        dependencies = sorted(dependencies)
        dependencies = [job_list[i] for i in dependencies]

        tasks = get_tasks_from_job(job)
        job_item = JobItem(len(job_list),
                           job._sis_id(),
                           str(job)[14:-1],
                           dependencies,
                           state,
                           bg_color,
                           tasks,
                           len(tasks)
                           )
        job_list.append(job_item)
        job_dict[job_item.sis_id] = job_item.number

    return render_template("overview.html", jobs=job_list)


def get_parameters():
    query_string = request.query_string.decode('utf8')
    queries = query_string.split('&')
    parameters = {}
    for query in queries:
        temp = query.split('=', 1)
        if len(temp) == 2:
            if temp[0] in parameters:
                logging.warning('Got argument multiple times: %s' % temp[0])
            parameters[temp[0]] = temp[1]
        else:
            logging.warning('Could not parse argument: %s' % temp[0])
    return parameters


def object_to_html(obj):
    if isinstance(obj, (str, int, float, bool)):
        return repr(obj)
    elif isinstance(obj, (list, tuple, set)):
        table = [(object_to_html(i),) for i in obj]
        if isinstance(obj, set):
            table.sort()
        return render_template("table.html", table=table)
    elif isinstance(obj, dict):
        table = sorted([(object_to_html(k), object_to_html(v)) for k, v in obj.items()])
        return render_template("table.html", table=table)
    elif isinstance(obj, Path):
        if obj.creator:
            return '%s : <a href="/info/%s">%s</a>' % (obj.path,
                                                       obj.creator._sis_id(),
                                                       obj.creator._sis_id())
        else:
            return obj.path
    elif isinstance(obj, Job):
        return '<a href="/info/%s">%s</a>' % (obj._sis_id(), obj._sis_id())
    elif hasattr(obj, 'html') and callable(obj.html):
        return obj.html()
    else:
        return html.escape(str(obj)).replace('\n', '<br/>')


# TODO remove this?
@app.route('/output')
@keepalive(2)
def list_outputs():
    pass


@app.route('/info/<path:sis_id>')
@keepalive(2)
def show_job_informations(sis_id):
    if not sis_id:
        logging.warning("No sis id given: " + str(parameters))
        return "No sis id given: " + str(parameters)

    job = g_sis_graph.job_by_id(sis_id)
    if job is None:
        return "Job not found: " + sis_id
    else:
        tasks = get_tasks_from_job(job)
        info = job.info()
        if info:
            info = object_to_html(info)

        task_logs = {}
        rqmt = {}
        if job._sis_runnable():
            for t in job._sis_tasks():
                rqmt[t.name()] = t.rqmt()
                if t.mini_task:
                    continue
                for tid in t.task_ids():
                    s = t.state(g_sis_engine, tid)
                    if s in [gs.STATE_INTERRUPTED, gs.STATE_ERROR, gs.STATE_FINISHED,
                             gs.STATE_RUNNING, gs.STATE_RETRY_ERROR]:
                        ll = []
                        try:
                            with open(t.path(gs.JOB_LOG, tid)) as log_file:
                                ll = log_file.readlines()
                        except IOError as e:
                            if e.errno != 2:
                                raise e
                        lines = ll[:10]
                        if s in [gs.STATE_INTERRUPTED, gs.STATE_ERROR, gs.STATE_FINISHED]:
                            lines.extend(ll[max(len(ll) - 10, 10):])
                        task_logs['%s.%d' % (t.name(), tid)] = ''.join(lines)

        return render_template("details.html",
                               job=job,
                               os=os,
                               tasks=tasks,
                               kwargs=object_to_html(job._sis_kwargs),
                               inputs=object_to_html(job._sis_inputs),
                               outputs=object_to_html(job._sis_outputs),
                               info=info,
                               rqmt=rqmt,
                               task_logs=task_logs
                               )


@app.route('/vis/')
@keepalive(2)
def visualize_root():
    return visualize('')


@app.route('/vis/<block_id>')
@keepalive(2)
def visualize(block_id):
    block_ids = block_id.split('.')
    try:
        block_ids = list(map(int, filter(lambda s: len(s) > 0, block_ids)))
    except ValueError:
        return 'Invalid block id (should be list of comma separated ints): %s' % block_id

    parent = None
    items = all_root_blocks
    for idx in block_ids:
        try:
            parent = items[idx]
            items = parent.filtered_children()
        except (IndexError, AttributeError):
            return 'Block does not exist'
    url_prefix = '/vis/' + block_id

    if parent is None:
        items += all_root_blocks[0].get_sub_blocks()
        return render_template("vis_overview.html", items=items)
    else:
        dot_file = visualize_block(parent, g_sis_engine, url_prefix)
        return sp.check_output(['dot', '-Tsvg'], input=dot_file, universal_newlines=True)


class HttpThread(threading.Thread):

    def __init__(self, port=0, debug=False):
        self.port = port
        self.debug = debug
        super(HttpThread, self).__init__()

    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        while True:
            try:
                sock.bind(('localhost', self.port))
                break
            except OSError as e:
                logging.warning('Could not bind to %d: %s' % (self.port, e))
                time.sleep(gs.WAIT_PERIOD_HTTP_RETRY_BIND)

        port = sock.getsockname()[1]
        sock.close()
        app.run(host='0.0.0.0',
                port=port,
                debug=self.debug)


def start(sis_graph=None, sis_engine=None, port=0, debug=False, thread=True):
    global g_sis_graph
    global g_sis_engine
    g_sis_graph = sis_graph
    g_sis_engine = sis_engine

    t = HttpThread(port, debug=debug)
    if thread:
        t.daemon = True
        t.start()
    else:
        t.run()
    return t

if __name__ == '__main__':
    from recipe import get_recipe
    from job_path import Path
    from graph import SISGraph

    test_recipe = get_recipe('task/test')
    job1 = test_recipe.Test(text=Path("input_text1.gz"))
    job2 = test_recipe.Test(text=Path("input_text2.gz"))
    job_merge1 = test_recipe.MergeInputs([job1.out, job2.out])
    job_merge2 = test_recipe.MergeInputs([job2.out, job1.out])
    job_merge3 = test_recipe.MergeInputs([job1.out, job2.out, job_merge1.out, job_merge2.out])
    sis_graph = SISGraph(output={'test': job_merge3.out})
    import engine
    sis_engine = engine.Engine()

    port = int(sys.argv[1])
    try:
        server = start(sis_graph=sis_graph,
                       sis_engine=sis_engine,
                       debug=True,
                       port=port,
                       thread=False)
    except OSError:
        server = start(sis_graph=sis_graph,
                       sis_engine=sis_engine,
                       debug=True,
                       port=0,
                       thread=False)
