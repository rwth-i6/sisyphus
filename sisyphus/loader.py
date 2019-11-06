import os
import logging
import importlib
from ast import literal_eval
from importlib.machinery import PathFinder
import sisyphus.global_settings as gs


def load_config_file(filename):
    import sisyphus.toolkit as toolkit
    toolkit.current_config_ = os.path.abspath(filename)
    toolkit.set_root_block(filename)
    try:
        globals_ = {
            '__builtins__': globals()['__builtins__'],
            '__file__': filename,
            '__name__': filename,
            '__package__': None,
            '__doc__': None,
        }

        # Check if file parameters are given
        if '(' in filename:
            filename, parameters = filename.split('(', 1)
            parameters, _ = parameters.rsplit(')', 1)
            parameters = literal_eval('(%s,)' % parameters)
        else:
            parameters = None

        filename = filename.replace(os.path.sep, '.')  # allows to use tab completion for file selection
        assert all(part.isidentifier() for part in filename.split('.')), "Config name is invalid: %s" % filename
        module_name, function_name = filename.rsplit('.', 1)
        config = importlib.import_module(module_name)
        if parameters is not None:
            getattr(config, function_name)(*parameters)
        elif function_name != 'py':
            # If filename ends on py and no parameters are given we assume we should only read the config file
            getattr(config, function_name)()

    except AttributeError as e:
        # TODO needs to be updated or removed
        if str(e).endswith("object has no attribute 'add_user'"):
            logging.error("Are you using a non Path object as path? Maybe a Job object instead of it's output path?")
        raise e
    toolkit.current_config_ = None


def load_configs(filenames=None):
    """

    :param filenames: list of strings containing the path to a config file, load default config if nothing is given
    :return: a dict containing all output paths registered in this config
    """
    if not filenames:
        if os.path.isfile(gs.CONFIG_FILE_DEFAULT):
            filenames = [gs.CONFIG_FILE_DEFAULT]
        elif os.path.isdir(gs.CONFIG_PREFIX):
            filenames = [gs.CONFIG_FUNCTION_DEFAULT]
    assert filenames, "Neither config file nor config directory exists"

    if isinstance(filenames, str):
        filenames = [filenames]

    for filename in filenames:
        load_config_file(filename)


class RecipeFinder:

    @classmethod
    def find_spec(cls, fullname, path, target=None):
        for rprefix, rdir in ((gs.RECIPE_PREFIX, gs.RECIPE_PATH), (gs.CONFIG_PREFIX, gs.CONFIG_PATH)):
            if fullname.startswith(rprefix):
                if path is None:
                    path = [os.path.abspath(rdir)]
                elif isinstance(path, str):
                    path = [os.path.abspath(os.path.join(rdir, path))]
                spec = PathFinder.find_spec(fullname, path, target)
                return spec

    @classmethod
    def invalidate_caches(cls):
        PathFinder.invalidate_caches()
