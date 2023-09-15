#!/usr/bin/env python3
import sys
import os
import collections
from ast import literal_eval
import pprint
import glob
import io


def print_help():
    print(
        """
WARNING: This script was written to convert Ducttape tape files into Sisyphus recipes. 
It was only tested on one fairly complex workflow I had at hand. It converted everything
except the summary feature and the submitter. There is a high chance it will break with
other workflows. Please let us know if this is the case and we try to fix it.

When run it will convert the given tape and tconf files into recipe and config directorys.

Usage: %s [tape directory|tape file|tconf file
"""
    )


DEBUG = False


def find_closing_parentheses(text):
    if DEBUG:
        print(99, text)
    open_par = text[0]
    if open_par == "(":
        close_par = ")"
    elif open_par == "[":
        close_par = "]"
    elif open_par == "{":
        close_par = "}"
    else:
        assert False, "Block needs to start with (, [, or {"
    param_count = 0
    pos = 1
    while pos < len(text):
        i = text[pos]
        if i == close_par:
            if param_count == 0:
                return pos
            else:
                param_count -= 1
        elif i == open_par:
            param_count += 1
        pos += 1
    assert False, "Did not find matching closing parentheses %s \ n %s" % (text, (open_par, close_par))


class BP:
    def __init__(self, name, values, module=None):
        if DEBUG:
            print("BP: ", name, values)
        if name[0] in ("'", '"'):
            print(name)
            self.name = literal_eval(name)
            print(999)
            assert False
        else:
            self.name = name

        self.values = collections.OrderedDict()
        for n, v in values:
            self.values[n] = v

        self.module = module

    def __str__(self):
        # return "BranchPoint(%s, [%s])" % (repr(self.name), ', '.join("(%s, %s)" % (repr(k), repr(v)) for (k, v) in self.values.items()))
        return "BranchPoint(%s, %s)" % (repr(self.name), pprint.pformat(tuple(self.values.items())))

    def __repr__(self):
        return str(self)


def find_next_param_block(text):
    if text and text[0] in "([":
        return find_closing_parentheses(text) + 1

    pos = 1
    while pos < len(text):
        i = text[pos]
        if i in "=":
            if text[pos + 1] == "(":
                next_pos = find_closing_parentheses(text[pos + 1 :]) + pos + 1
                if next_pos + 1 < len(text) and text[next_pos + 1] == " ":
                    next_pos += 1
                return next_pos
        if i in " \n":
            return pos
        pos += 1
    return None


def parse_param(param):
    if not isinstance(param, str):
        return param

    tmp = []
    # print(88,param)
    if param.startswith("("):
        # TODO Change input to always include )
        if param.endswith(")"):
            param = param[:-1]
        if DEBUG:
            print(88, param)
        name, rest = param[1:].split(":", 1)
        values = []

        while rest is not None:
            rest = rest.strip()
            pos = find_next_param_block(rest)
            # print(999, rest[:pos], pos)
            block = rest[:pos]
            if pos is None:
                rest = None
            else:
                rest = rest[pos + 1 :]

            if DEBUG:
                print(33, block, rest)

            equal = block.find("=")
            if equal != -1:
                key, value = block.split("=", 1)
                if DEBUG:
                    print(22, key, value)
                value = parse_param(value)
            else:
                key = value = block
            if key:
                values.append((key, value))
            else:
                assert not value

        return BP(name, values)
    return normalize_string(param)


class Block:
    def __init__(self):
        self.name = None
        self.description = collections.defaultdict(list)
        self.body = None

    def __repr__(self):
        return str(self)

    def __str__(self):
        n = self.description["name"]
        assert len(n) == 1
        return """%s %s: %s
    %s""" % (
            self.name,
            n[0],
            pprint.pformat(self.description),
            pprint.pformat(self.body),
        )


class JobCreator:
    def __init__(self, job, inputs):
        self.job = job
        self.inputs = inputs

    def __str__(self):
        return "JobCreator(%s, %s)" % (self.job, pprint.pformat(self.inputs))

    def __repr__(self):
        return str(self)


def normalize_string(text):
    if not isinstance(text, str):
        return text
    text = text.strip()
    if text.startswith('"') or text.startswith("'"):
        return literal_eval(text)
    else:
        return text


def parse_block(sline, f, comment):
    history = [sline]
    block = Block()
    # block.description['filename'] = filename
    block.description["comment"] = comment
    sline.reverse()
    block.name = sline.pop()
    body = []
    mode = "name"
    while mode not in ("{", "{}"):
        # Skip empty lines
        while not sline:
            sline = f.readline().split()
            history.append(sline)
            sline.reverse()
        n = sline.pop()

        # collect current and all related lines for each mode
        if n in (":", "<", ">", "::", "{", "{}"):
            if mode:
                if block.name != "global":
                    assert body, (mode, n, sline, history)
            l = block.description[mode]
            l.append(" ".join(body))

            mode = n
            body = []
            if mode == "{":
                assert not sline
        else:
            body.append(n)

    line = f.readline()
    sline = line.split()

    for m in (":", "<", ">", "::", "{", "{}"):
        if m in block.description and block.description[m]:
            if DEBUG:
                print(block.name, m)
            if DEBUG:
                print(block.description[m])

    if block.name in ("summary", "submitter", "versioner"):
        while not sline or sline != ["}"]:
            if sline:
                subblock = parse_block(sline, f, [])
                block.description["subblock"].append(subblock)
            line = f.readline()
            sline = line.split()

    while mode == "{" and (not sline or sline[0] != "}"):
        body.append(line)
        line = f.readline()
        sline = line.split()
    assert mode == "{}" or sline == ["}"]

    block.body = "".join(body)
    return block


def block_to_job_creator(block):
    org_name = block.description["name"]
    assert len(org_name) == 1
    org_name = org_name[0]
    job_name = toCamelCase(org_name)

    org_inputs = block.description["<"]
    org_params = block.description["::"]
    org_tools = block.description[":"]
    assert len(org_tools) <= 1
    org_tools = org_tools[0].split() if org_tools else []

    inputs_list = [tuple(i.split("=", 1)) for i in org_inputs]
    inputs_list += [tuple(i.split("=", 1)) for i in org_params if i != ".submitter=shell"]
    inputs_list += [(i, "$" + i) for i in org_tools]

    inputs_list = [(k.strip(), parse_param(v)) for k, v in inputs_list]

    # filename = block.description['filename']
    # assert filename.endswith('.tape')
    # filename = filename[:-5]
    # if DEBUG: print(666, org_name, (filename, job_name), inputs_list)
    return (org_name, job_name, inputs_list)


def block_to_job(block):
    org_name = block.description["name"]
    assert len(org_name) == 1
    org_name = org_name[0]
    job_name = toCamelCase(org_name)

    org_inputs = block.description["<"]
    org_params = block.description["::"]
    org_tools = block.description[":"]
    if len(org_tools) == 0:
        pass
    elif len(org_tools) == 1:
        org_tools = org_tools[0].split()
    else:
        assert False

    inputs_list = [i.split("=", 1)[0] for i in org_inputs]
    inputs_list += [i.split("=", 1)[0] for i in org_params if i != ".submitter=shell"]
    inputs_list += [i.split("=", 1)[0] for i in org_tools]
    set_inputs = "\n".join(["        self.%s = %s" % (i, i) for i in inputs_list])
    inputs = ", ".join(inputs_list)

    outputs = block.description[">"]

    output_list = []
    set_outputs = []
    for o in outputs:
        if "=" in o:
            name, value = o.split("=", 1)
        else:
            name = value = o
        name = normalize_string(name)
        value = normalize_string(value)
        output_list.append(name)
        set_outputs.append('        self.%s = self.output_path("%s")' % (name, value))
    set_outputs = "\n".join(set_outputs)

    # print("Task name: %s %s" % (org_name, job_name))
    # print("Inputs: %s" % inputs)
    # print("Params: %s" % params)
    # print("Outputs: %s" % outputs)
    # print("Tools: %s" % org_tools)
    # print

    exports = "\n".join(
        ["            \"export %s='{%s}'\\n\" \\" % (i.strip(), i.strip()) for i in inputs_list + output_list]
        + ["            '\\n' \\"]
    )
    exports = exports[12:]
    exports += '\n            "cd ../output \\n" \\'
    body = "\n".join(
        "            " + repr((line + "\n").replace("{", "{{").replace("}", "}}")) + " \\"
        for line in block.body.split("\n")
    )
    body = body[:-2]

    comment = "".join(i for i in block.description["comment"] if i != "#!/usr/bin/bash\n")

    template = """{comment}class {job_name}(Job):
    def __init__(self, {inputs}):
{set_inputs}
{set_outputs}

    def run(self):
        command = {exports}
{body}

        with open('command.sh', 'w') as f:
            f.write(command.format(**self.__dict__))
        self.sh('bash -ueo pipefail command.sh')

    def tasks(self):
        yield Task('run')
"""
    job = template.format(**locals())

    return job


def block_to_versioner(block):
    org_name = block.description["name"]
    assert len(org_name) == 1
    org_name = org_name[0]
    job_name = toCamelCase(org_name)

    # print(block)
    input_list = block.description["::"][0].split()
    output_list = []

    bodys = []
    outdir = None
    for sb in block.description["subblock"]:
        name = sb.description["name"][0]

        body = "\n".join(
            "            " + repr((line + "\n").replace("{", "{{").replace("}", "}}")) + " \\"
            for line in sb.body.split("\n")
        )
        body = body[12:-2]
        template = """        command=exports + {body}
        with open('{name}.sh', 'w') as f:
            f.write(command.format(**self.__dict__))
        self.sh('bash {name}.sh')
"""
        bodys.append(template.format(**locals()))

        for o in sb.description[">"][0].split():
            if name == "checkout":
                assert outdir is None
                outdir = repr(normalize_string(o))
            if o not in output_list:
                output_list.append(o)

    inputs = ", ".join(input_list)
    set_inputs = "\n".join(["        self.%s = %s" % (i, i) for i in input_list])
    set_outputs = []
    for o in output_list:
        if repr(normalize_string(o)) == outdir:
            set_outputs.append('        self.%s = self.output_path("%s", directory=True)' % (o, o))
        else:
            set_outputs.append('        self.%s = self.output_path("%s")' % (o, o))
    set_outputs = "\n".join(set_outputs)

    exports = "\n".join(
        ["            '    export %s={%s}\\n' \\" % (i, i) for i in input_list + output_list] + ["            '\\n' \\"]
    )
    exports = exports[12:]

    bodys = "\n".join(bodys)

    template = """class {job_name}(Job):
    def __init__(self, {inputs}):
{set_inputs}
{set_outputs}

    def run(self):
        exports = {exports}

{bodys}
    def tasks(self):
        yield Task('run')

config[('versioner', '{org_name}')] = ({job_name}, {outdir})
"""
    return template.format(**locals())


def block_to_plan(block):
    name = block.description["name"][0]
    out = ["def %s():" % name]
    targets = []
    branch_points = {}

    def add_targets():
        if targets:
            out.append("get_targets(%s, %s)\n" % (repr(targets), repr(branch_points)))
        else:
            assert not branch_points

    for line in io.StringIO(block.body):
        line = line.strip()
        if line.startswith("reach "):
            add_targets()
            assert line.endswith(" via")
            line = line[5:-3].strip()
            targets = [i.strip() for i in line.split(",")]
            if len(targets) == 1:
                targets = targets[0]
            assert targets
            branch_points = {}
        elif line.startswith("#"):
            out.append(line)
        else:
            last = None
            for bp in line.split("*"):
                bp = bp.strip()
                if bp:
                    if bp == ")":
                        assert last
                        bp = last + " * )"
                        last = None

                    assert bp.startswith("("), (bp, line)
                    if not bp.endswith(")"):
                        last = bp
                        continue
                    else:
                        assert last == None
                    bp = bp[1:-1]
                    key, values = bp.split(":")
                    values = values.split()
                    if len(values) == 1:
                        values = values[0]
                    branch_points[key] = values
    add_targets()
    # print(block)
    return "\n    ".join(out) + "\n"


def convert_file(in_file, out_file, imports=None):

    if imports:
        # tconf file
        out_file.write("from sisyphus import *\nfrom recipe.ducttape import *\n\n")
        out_file.write("# config is imported from recipe.ducttape\n")

        for i in imports:
            out_file.write("import_module('%s')\n" % i)
        out_file.write("\n")

    else:
        out_file.write(
            "from sisyphus import *\nPath = setup_path(__package__)\nfrom recipe.ducttape import *\n\nconfig={}\n\n"
        )
    line = in_file.readline()
    sline = line.split()
    task = None

    comment = []

    while line:
        if not sline:
            out_file.write("\n")
        elif sline[0].startswith("#"):
            out_file.write(line)
        elif sline[0] in ("task", "global", "summary", "submitter", "versioner", "package", "action", "plan"):

            block = parse_block(sline, in_file, comment)
            if block.name == "task":
                job = block_to_job(block)
                out_file.write(job)
                org_name, job, inputs_list = block_to_job_creator(block)
                out_file.write(
                    "config[%s] = config[%s] = %s\n"
                    % (repr(org_name), repr("." + org_name), str(JobCreator(job, inputs_list)))
                )
            elif block.name == "plan":
                out_file.write(block_to_plan(block))
                pass
            elif block.name == "submitter":
                # TODO implement (if needed...)
                pass

            elif block.name == "global":

                body = io.StringIO(block.body)
                line = body.readline()
                while line:
                    if line.strip().startswith("#") or line.strip() == "":
                        out_file.write(line.strip() + "\n")
                    else:
                        if "=" in line:
                            a, b = line.strip().split("=", 1)
                            if b.startswith("("):
                                tmp = b + body.read()
                                end = find_closing_parentheses(tmp) + 1
                                b = parse_param(tmp[:end])
                                body = io.StringIO(tmp[end:])
                            out_file.write("config[%s] = %s\n" % (repr(a), repr(normalize_string(b))))
                        else:
                            print("Skip: ", line)
                    line = body.readline()
            elif block.name == "versioner":
                # TODO implement
                versioner = block_to_versioner(block)
                out_file.write(versioner)
            elif block.name == "package":
                name = block.description["name"][0]
                params = {}
                for i in block.description["::"][0].split():
                    k, v = i.split("=")
                    assert k.startswith(".")
                    k = k[1:]
                    params[k] = normalize_string(v)
                versioner = params["versioner"]
                del params["versioner"]

                out_file.write(
                    """config['{name}']=lambda branch_points: get_versioner('{versioner}', {params})\n""".format(
                        **locals()
                    )
                )
            elif block.name == "summary":
                # TODO may implement
                print("%s\nTODO: summary\n%s\n%s\n" % ("-" * 80, block, "-" * 80))
            else:
                print("Unknown block")
                print(block)
                print("Unknown block")

            comment = []
            # print('HHH')
            # print(block)
        elif sline[0] == "import":
            pass
            # print("from recipe import %s" % sline[1].split('.')[0])
            # parse_file(os.path.join(directory, sline[1]), all_blocks)
        else:
            print(task)
            print("Can't read line: '%s' :  %s" % (sline[0], sline))
            print(f.readline())
            print(f.readline())
            print(f.readline())
            print(f.readline())
            assert False
            # sys.stdout.write(i)
            # print(i)
        line = in_file.readline()
        sline = line.split()


def main():
    out_dir = "."

    input_tape = []
    input_tconf = []

    for in_dir in sys.argv[1:]:
        if os.path.isfile(in_dir):
            if in_dir.endswith(".tape"):
                input_tape.append(in_dir)
            if in_dir.endswith(".tconf"):
                input_tconf.append(in_dir)
            else:
                print_help()
                os.exit(1)
        else:
            input_tape += glob.glob("%s/*.tape" % in_dir)
            input_tconf += glob.glob("%s/*.tconf" % in_dir)

    for i in ("recipe", "config"):
        try:
            os.mkdir("%s/%s" % (out_dir, i))
        except FileExistsError:
            pass

    dt_path = "%s/recipe/ducttape.py" % out_dir
    if not os.path.isfile(dt_path):
        with open(dt_path, "w") as f:
            f.write(ducttape)

    for in_tape in input_tape:
        out_recipe = "%s/recipe/%s.py" % (out_dir, os.path.basename(in_tape)[:-5])
        with open(in_tape) as in_f, open(out_recipe, "w") as out_f:
            convert_file(in_f, out_f)

    for in_tape in input_tconf:
        out_config = "%s/config/%s.py" % (out_dir, os.path.basename(in_tape)[:-6].replace("-", "_").replace(".", "_"))
        with open(in_tape) as in_f, open(out_config, "w") as out_f:
            convert_file(in_f, out_f, imports=[os.path.basename(in_tape)[:-5] for in_tape in input_tape])


def toCamelCase(name):
    out = []
    next_upper = True
    for i in name:
        if i == "_":
            next_upper = True
        elif next_upper:
            out.append(i.upper())
            next_upper = False
        else:
            out.append(i)
    return "".join(out)


ducttape = """from sisyphus import *
Path = setup_path(__package__)
import importlib 
import os 
import collections 

# This needs to be set in the config file
config = {}


def import_module(module_name):
    mod = importlib.import_module('recipe.%s' % module_name)
    config.update(mod.config)


class BranchPoint:
    all_points = collections.defaultdict(list)
    def __init__(self, name, values):
        self.name = name
        self.values = values
        
	# Store all possible keys 
        keys = [i[0] for i in self.values]
        assert '' not in keys
        saved_keys = BranchPoint.all_points[self.name]
        if saved_keys:
            assert saved_keys[0] == keys[0]
            assert sorted(saved_keys[:]) == sorted(keys[:])
        else:
            saved_keys += keys

    def __call__(self, branch_points):
        key = branch_points.get(self.name)
        if key is None:
            k, v = self.values[0]
            #print("%s %s %s" % (self.name, key, v))
            return v, {self.name: k}
        else:
            for k, v in self.values:
                if k == key:
                    #print("%s %s %s" % (self.name, key, v))
                    return v, {self.name: k}
        assert False, "Key not found: %s values: %s" % (key, self.values)


class JobCreator:
    def __init__(self, job, inputs):
        self.job = job
        self.inputs = inputs

    def __call__(self, branch_points):
        params = {}
        total_used_bps = {}
        for k, v in self.inputs:
            params[k], used_bps = parse_param(v, branch_points, k)
            total_used_bps.update(used_bps)
        job = self.job(**params)
        job.add_alias(uncamel_case(job.__class__.__name__) + os.path.sep + get_name(total_used_bps, short=True))
        return job, total_used_bps


_cache = collections.defaultdict(list)

def parse_param(param, branch_points, backup=None):
    return parse_param_helper(param, branch_points, backup=backup)
    cached = _cache[(param, backup)]
    for bp, res in cached:
        if bp == branch_points:
            return res

    res = parse_param_helper(param, branch_points, backup=backup)
    cached.append((branch_points, res))
    return res

def parse_param_helper(param, branch_points, backup=None):
    if isinstance(param, tk.Path):
        # TODO store used_bps in path
        return param, {}
    if not isinstance(param, str):
        # Is either Branchpoint or job
        param, used_bps = param(branch_points)
        # in both cases the output needs another update
        param, used_bps2 = parse_param(param, branch_points)
        total_used_bps = used_bps2.copy()
        total_used_bps.update(used_bps)
        return param, total_used_bps

    if param == '@':
        assert backup is not None
        return parse_param("$" + backup, branch_points)

    _filter_bps = set()
    def filter_bps(passthrough, bps):
        if _filter_bps:
            return passthrough, {k: v for k, v in bps.items() if k not in _filter_bps}
        else:
            return passthrough, bps

    if '[' in param:
        assert param.endswith(']')
        param, bp_mods = param[:-1].split('[')
        bp = branch_points.copy()
        for bp_mod in bp_mods.split(','):
            k, v = bp_mod.split(':')
            bp[k] = v
            _filter_bps.add(k)
    else:
        bp = branch_points

    if param.startswith('$'):
        param = param[1:]
        if '@' in param:
            path, job = param.split('@')
            job, used_bps = config.get('.'+job, config[job])(bp)
            return filter_bps(getattr(job, path), used_bps)
        else:
            return filter_bps(*parse_param(config[param], bp))
    elif param.startswith('@'):
        assert backup is not None
        job = param[1:]
        job, used_bps = config.get('.'+job, config[job])(bp)
        return filter_bps(getattr(job, backup), used_bps)
    else:
        return param, {}


def get_versioner(versioner, parameter):
    job, directory = config[('versioner', versioner)]
    job = job(**parameter)
    return getattr(job, directory), {}


def get_name(bps, short=True):
    points = []
    length = 0
    for k, v in sorted(bps.items()):
        # always skip the first entry
        if not short or BranchPoint.all_points[k][0] != v:
            n = '%s.%s' % (k, v)
            length += len(n) + 1
            if length > 250:
                points.append(os.path.sep)
                length = len(n)
            points.append(n)
    if not points:
        return 'Baseline.baseline'
    return '+'.join(points).replace(os.path.sep+'+', os.path.sep)


def uncamel_case(s):
    out = []
    for i in s:
        if i.isupper() and out:
            out += '_'
        out.append(i.lower())
    return ''.join(out)


counter = 0
def get_targets(targets, branch_points):

    if isinstance(targets, (list, tuple)):
        for target in targets:
            get_targets(target, branch_points) 
        return

    for k, v in branch_points.items():
        if v is '*':
            v = BranchPoint.all_points[k]
        if isinstance(v, (list, tuple)):
            for tmp in v:
                bp = branch_points.copy()
                bp[k] = tmp
                get_targets(targets, bp) 
            return

    global counter
    job, used_bps = config[targets](branch_points)
    #tk.register_output("%s.%03i" % (targets, counter), tk.Path('..', job))
    tk.register_output(targets + os.path.sep + get_name(used_bps, short=True), tk.Path('..', job))
    #tk.register_output(get_name(targets, used_bps, short=False), tk.Path('..', job))
    print(targets, get_name(used_bps, short=True))
    #print(get_name(targets, used_bps, short=False))
    #print("%s.%03i" % (targets, counter), used_bps)
    #print()
    counter += 1
"""

main()
