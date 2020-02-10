import collections
import hashlib
import itertools as it
import re

import sisyphus.global_settings as gs
from sisyphus.tools import sis_hash
from sisyphus.block import Block
from sisyphus.job import Job


def visualize_block(block, engine, vis_url_prefix):
    """Creates a dot representation of a Block"""
    jobs = set()
    inputs = set()
    input_to_node = dict()
    links = set()
    counts = collections.Counter()

    result = []
    result.append('digraph G {\n')
    result.append('rankdir = TB;\n')

    # output jobs and blocks and gather info on inputs and links
    for idx, child in enumerate(block.filtered_children()):
        if isinstance(child, Block):
            result.append(
                dot_node(child.name, color_map[block_state(child, engine)],
                         'box3d',
                         vis_url_prefix + '.%d' % idx))

            bj = block_jobs(child)
            bei = block_external_inputs(child, bj)

            inputs.update(bei)
            counts.update(bei)
            links.update((i, child.name) for i in bei)

            for j in bj.values():
                input_to_node.update((o.rel_path(), child.name) for o in j._sis_outputs.values())

        elif isinstance(child, Job):
            job_name = job_id = child._sis_id()
            job_name = child.get_one_alias() if child.get_one_alias() is not None else job_name
            job_name = child.get_vis_name() if child.get_vis_name() is not None else job_name
            if job_id in jobs:
                continue
            jobs.add(job_id)
            result.append(dot_node(job_id, color_map[child._sis_state(engine)], 'folder', '/info/' + job_id, job_name))

            inputs.update(i.rel_path() for i in child._sis_inputs)
            counts.update(i.rel_path() for i in child._sis_inputs)
            links.update((i.rel_path(), job_id) for i in child._sis_inputs)
            input_to_node.update((o.rel_path(), job_id) for o in child._sis_outputs.values())

    merge_inputs_mapping = dict()
    merged_labels = dict()
    merged_creators = dict()

    for i in inputs:
        creator = input_to_node[i] if i in input_to_node else ''
        users = sorted(t[1] for t in filter(lambda l: l[0] == i, links))
        hash = sis_hash((creator, users))
        merge_inputs_mapping[i] = hash
        if hash not in merged_labels:
            merged_labels[hash] = [i.split('/')[-1]]
        else:
            merged_labels[hash].append(i.split('/')[-1])
        if len(creator) > 0:
            merged_creators[hash] = creator

    merged_links = set((merge_inputs_mapping[l[0]], l[1]) for l in links)
    merged_counts = collections.Counter(dict((merge_inputs_mapping[k], v) for k, v in counts.items()))

    # output inputs and the links from the creators to the inputs
    for h, l in merged_labels.items():
        result.append(dot_node(h, 'aquamarine', 'box', '', '\\n'.join(compact_inputs(l))))
        if h in merged_creators:
            result.append('"%s" -> "%s";\n' % (merged_creators[h], h))

    # output input-links
    common_inputs = set()
    for l in merged_links:
        if merged_counts[l[0]] <= gs.VIS_RELATIVE_MERGE_THRESHOLD * len(block.filtered_children()) or \
           merged_counts[l[0]] <= gs.VIS_ABSOLUTE_MERGE_THRESHOLD:
            result.append('"%s" -> "%s";\n' % l)
        else:
            common_inputs.add(l[0])

    # output common-inputs
    if len(common_inputs) > 0:
        result.append(dot_node('[Common Inputs]', 'white', 'box', ''))
        for ci in common_inputs:
            result.append('"%s" -> "[Common Inputs]";' % ci)

    result.append('}\n')
    return ''.join(result)


# -------------------- Internal --------------------
# List of all colors: https://www.graphviz.org/doc/info/colors.html
color_map = collections.defaultdict(lambda: 'gray')
color_map.update({gs.STATE_UNKNOWN: 'gray',
                  gs.STATE_ERROR: 'red',
                  gs.STATE_QUEUE_ERROR: 'red',
                  gs.STATE_RETRY_ERROR: 'red',
                  gs.STATE_INTERRUPTED: 'red',
                  gs.STATE_RUNNABLE: 'steelblue',
                  gs.STATE_WAITING: 'yellow',
                  gs.STATE_QUEUE: 'steelblue',
                  gs.STATE_RUNNING: 'greenyellow',
                  gs.STATE_FINISHED: 'darkgreen',
                  })


def block_state(block, engine):
    states = set()
    for c in block.filtered_children():
        if hasattr(c, '_sis_state'):
            states.add(c._sis_state(engine))
        else:
            states.add(block_state(c, engine))

    for state in [gs.STATE_UNKNOWN,
                  gs.STATE_ERROR,
                  gs.STATE_QUEUE_ERROR,
                  gs.STATE_RETRY_ERROR,
                  gs.STATE_INTERRUPTED,
                  gs.STATE_RUNNING,
                  gs.STATE_RUNNABLE,
                  gs.STATE_QUEUE,
                  gs.STATE_WAITING,
                  gs.STATE_FINISHED]:
        if state in states:
            return state


def dot_node(name, color, shape, url, label=None):
    if label is None:
        label = name
    return '"%s" [color=black,fillcolor=%s,style=filled,fontcolor=black,shape=%s,URL="%s",tooltip="",label="%s"];\n' % (
        name, color, shape, url, label)


def block_jobs(block):
    jobs = dict()
    for child in block.filtered_children():
        if isinstance(child, Block):
            jobs.update(block_jobs(child))
        elif isinstance(child, Job):
            jobs[child._sis_id()] = child
    return jobs


def block_external_inputs(block, jobs):
    inputs = set()
    for child in block.filtered_children():
        if isinstance(child, Block):
            inputs.update(block_external_inputs(child, jobs))
        elif isinstance(child, Job):
            inputs.update(i.rel_path()
                          for i in child._sis_inputs if i.creator is not None and i.creator._sis_id() not in jobs)
    return inputs

# this function merges inputs that are identical except for one number. These numbers are compacted into a range.
# example: ['alignment.cache.1', ..., 'alignment.cache.5', 'alignment.cache.7'] becomes ['alignment.cache.{1-5,7}']


def compact_inputs(inputs):
    def allsame(x):
        return len(set(x)) == 1

    def to_int(s):
        try:
            return int(s)
        except ValueError:
            return None

    inputs.sort()
    result = []
    # first group inputs by deleting numbers
    groups = [list(items) for k, items in it.groupby(inputs, lambda s: re.sub('\\d', '', s))]
    for g in groups:
        # compute (length of) longest common prefix/suffix
        lcp = ''.join(t[0] for t in it.takewhile(allsame, zip(*g)))
        lcs = (''.join(t[0] for t in it.takewhile(allsame, zip(*[item[::-1] for item in g]))))[::-1]
        llcp = len(lcp)
        llcs = len(lcs)

        # the fragments are (in the best case) only the numbers, otherwise we give up
        fragments = [to_int(item[llcp:-llcs] if llcs > 0 else item[llcp:]) for item in g]
        if None not in fragments:
            fragments.sort()
            r = []
            # compute range
            start = fragments[0]
            end = start
            for f in (fragments[1:] + [None]):
                if f == end + 1:
                    end = f
                else:
                    if start == end:
                        r.append(str(start))
                    else:
                        r.append('%d-%d' % (start, end))
                    start = end = f
            result += ['%s{%s}%s' % (lcp, ','.join(r), lcs)]
        else:
            result += g
    return result
