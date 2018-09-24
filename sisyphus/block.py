# Create Blocks

import os
import inspect
from sisyphus.global_settings import SIS_HASH as sis_hash


class Block(object):

    sis_graph = None

    def __init__(self, name, parents=None):
        self.name = name
        self.children = []
        if parents is None:
            self.parents = set()
        else:
            self.parents = parents
        self.last_active_blocks = None

    def __repr__(self):
        return "<Block %s.%s>" % (self.name, repr(self.parents))

    def __str__(self):
        return "<Block %s.%s>" % (self.name, repr(self.parents))

    def sub_block(self, name):
        child = Block(name, {self})
        self.children.append(child)
        return child

    def add_job(self, job):
        self.children.append(job)

    def add_block(self, block):
        self.children.append(block)

    def filtered_children(self):
        if self.sis_graph:
            jobs_in_graph = self.sis_graph.id_to_job_dict()
            return [child for child in self.children
                    if (isinstance(child, Block) and not child.empty()) or
                    (not isinstance(child, Block) and child._sis_id() in jobs_in_graph)]
        else:
            return self.children

    def get_sub_blocks(self):
         return [child for child in self.children
                if (isinstance(child, Block) and not child.empty())]

    def empty(self):
        return not bool(self.filtered_children())

    # with functionality
    def __enter__(self):
        assert self.last_active_blocks is None, "Enter Block twice: %s, %s" % (self.name, str(self.last_active_blocks))
        global active_blocks
        current_block = self
        self.last_active_blocks = active_blocks
        active_blocks = {current_block}

    def __exit__(self, type, value, traceback):
        assert self.last_active_blocks is not None, "Unknown previous active blocks"
        global active_blocks
        active_blocks = self.last_active_blocks
        self.last_active_blocks = None

    # Filesystem functions
    def __fs_directory__(self):
        """ Returns all items that should be listed by virtual filesystem
        :param job:
        :return:
        """

        yield '_name_%s' % self.name

        integer_length = str(len(str(len(self.filtered_children())-1)))
        name_template = '%0' + integer_length + 'i_%s'

        for pos, child in enumerate(self.filtered_children()):
            if isinstance(child, Block):
                name = name_template % (pos, child.name)
            else:  # assume it's a job
                name = name_template % (pos, child._sis_id().replace(os.path.sep, '_'))
            yield name

    def __fs_get__(self, step):
        if step == '_name_%s' % self.name:
            return None, self.name

        try:
            pos = int(step.split('_')[0])
            return None, self.filtered_children()[pos]
        except:
            raise KeyError(step)

active_blocks = set()
all_root_blocks = []


def set_root_block(name):
    """ Set new root block, usually with the name of the config file """
    global active_blocks
    current_block = Block(name)
    active_blocks = {current_block}
    all_root_blocks.append(current_block)


def sub_block(name):
    b = Block(name)
    add_to_active_blocks(b)
    return b


def add_to_active_blocks(current_block):
    for block in active_blocks:
        if block not in current_block.parents:
            block.add_block(current_block)
            current_block.parents.add(block)


class block(object):
    """ Open block and add job created inside of this block to it.
    Can also be used as decorator, the decorator can cache the output of a given function.

    Usage:
    with block('foo'):
        # everything here will be grouped to one block
        ....

    @block(cache=True):
    def bar():
        # block named bar will be opened when function is called
        ...
    """

    def __init__(self, name=None, cache=False):
        self.name = name
        self.cache = cache
        self.last_active_blocks = None

    def __enter__(self):
        assert self.last_active_blocks is None
        assert self.name is not None
        global active_blocks
        current_block = Block(self.name)
        add_to_active_blocks(current_block)
        self.last_active_blocks = active_blocks
        active_blocks = {current_block}

    def __exit__(self, type, value, traceback):
        assert self.last_active_blocks is not None
        global active_blocks
        active_blocks = self.last_active_blocks

    # if used used as decorated
    def __call__(self, f):
        cache = {}
        if self.name:
            block_name = self.name
        else:
            block_name = f.__name__

        if self.cache:
            if inspect.isfunction(f):
                def get_hash(args, kwargs):
                    return sis_hash((args, kwargs))
            elif inspect.ismethod(f):
                def get_hash(args, kwargs):
                    return sis_hash((f.__self__.__dict__, args, kwargs))
            else:
                get_hash = None
            assert get_hash is not None

            def block_f(*args, **kwargs):
                key = get_hash(args, kwargs)
                try:
                    ret, current_block = cache[key]
                except KeyError:
                    current_block = Block(block_name)
                    with current_block:
                        ret = f(*args, **kwargs)
                    cache[key] = ret, current_block
                add_to_active_blocks(current_block)
                return ret
        else:
            def block_f(*args, **kwargs):
                with block(block_name):
                    return f(*args, **kwargs)

        return block_f
