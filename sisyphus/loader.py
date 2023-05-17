import os
import time
from collections import defaultdict
import logging
import importlib
import inspect
import asyncio
import asyncio.tasks
from ast import literal_eval
from importlib.machinery import PathFinder
import sisyphus.global_settings as gs


class ConfigManager:
    def __init__(self):
        self._config_readers = []
        self._waiting_reader = {}
        self.loop = asyncio.get_event_loop()
        self._reader_threads = defaultdict(dict)
        self._current_config = None

    @property
    def current_config(self):
        return self._current_config

    @current_config.setter
    def current_config(self, value):
        self._current_config = value

    def load_config_file(self, config_name):
        import sisyphus.toolkit as toolkit

        # Check if file parameters are given
        if '(' in config_name:
            filename, parameters = config_name.split('(', 1)
            parameters, _ = parameters.rsplit(')', 1)
            parameters = literal_eval('(%s,)' % parameters)
        else:
            filename = config_name
            parameters = []

        self.current_config = os.path.abspath(filename)
        toolkit.set_root_block(filename)

        # maybe remove import path prefix such as "recipe/"
        for load_path in gs.IMPORT_PATHS:
            if load_path.endswith('/') and filename.startswith(load_path):
                filename = filename[len(load_path):]
                break
        filename = filename.replace(os.path.sep, '.')  # allows to use tab completion for file selection
        assert all(part.isidentifier() for part in filename.split('.')), "Config name is invalid: %s" % filename
        module_name, function_name = filename.rsplit('.', 1)
        try:
            config = importlib.import_module(module_name)
        except SyntaxError:
            import sys
            if gs.USE_VERBOSE_TRACEBACK:
                sys.excepthook = sys.excepthook_org
            raise

        func = None
        try:
            func = getattr(config, function_name)
        except AttributeError:
            if function_name != 'py':
                # If filename ends on py and no function is found we assume we should only read the config file
                # otherwise we reraise the exception
                raise
            else:
                if gs.WARNING_NO_FUNCTION_CALLED:
                    logging.warning("No function named 'py' found in module '%s'"
                                    " (hide warning by setting WARNING_NO_FUNCTION_CALLED=False)" % module_name)

        task = None
        if inspect.iscoroutinefunction(func):
            # Run till the first await command is found

            async def set_root_block(root_block, used_config, async_func):
                toolkit.set_root_block(root_block)
                self.current_config = used_config
                await async_func
                self.current_config = None

            logging.info('Loading async config: %s (loaded module: %s)' % (config_name, module_name))
            task = self.loop.create_task(set_root_block(filename, self.current_config, func(*parameters)))
        elif func:
            func(*parameters)
            logging.info('Loaded config: %s (loaded module: %s)' % (config_name, module_name))
        self.continue_readers()

        assert self.current_config
        self._config_readers.append((self.current_config, task))
        self.current_config = None
        self.continue_readers()
        return task

    def load_configs(self, filenames=None):
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
            self.load_config_file(filename)

        # TODO: Make this more deteministic
        # continue_reader doesn't guarantee that all config readers had a chance to run, so we run in multiple times
        # to increase the chance that all readers continued as far as possible
        self.continue_readers()
        for i in filenames:
            self.continue_readers()
            self.continue_readers()

    def run_async_step(self):
        # If stop() is called before run_forever() is called,
        # the loop will poll the I/O selector once with a timeout of zero,
        # run all callbacks scheduled in response to I/O events (and those that were already scheduled), and then exit.
        # https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.run_forever
        if not self.reader_running():
            self.check_for_exceptions()
            return
        self.loop.stop()
        self.loop.run_forever()
        self.check_for_exceptions()

    def continue_readers(self):
        # If stop() is called before run_forever() is called,
        # the loop will poll the I/O selector once with a timeout of zero,
        # run all callbacks scheduled in response to I/O events (and those that were already scheduled), and then exit.
        # https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.run_forever
        if not self.reader_running():
            self.check_for_exceptions()
            return

        # Even for large setups all configs should be able to finish 50 tries
        for i in range(50):
            self.run_async_step()
            non_waiting = self.non_waiting_readers()
            if not non_waiting:
                break

        # TODO Enable again once we can track all readers reliable
        if False and non_waiting:
            for name, reader in non_waiting:
                logging.warning("Reader " + name + " is currently in a undefined mode, "
                                "continue anyway and hope for the best" + str(self._waiting_reader))

    def reader_running(self):
        """ Return True if any config reader is not finished yet.

        :return:
        """
        for _, reader in self._config_readers:
            if reader is None or reader.done() or reader.cancelled():
                continue
            else:
                return True
        return False

    def mark_reader_as_waiting(self, config_name):
        self._waiting_reader[config_name] = time.time()

    def unmark_reader_as_waiting(self, config_name):
        if config_name in self._waiting_reader:
            del self._waiting_reader[config_name]

    def non_waiting_readers(self):
        out = []
        for name, reader in self._config_readers:
            if reader and not reader.done() and not reader.cancelled():
                if name not in self._waiting_reader:
                    if not self._reader_threads[name]:
                        out.append((name, reader))
                    else:
                        for reader_thread in self._reader_threads[name]:
                            if reader_thread not in self._waiting_reader:
                                out.append((reader_thread, reader))
            return out

    def print_config_reader(self):
        """ Print running config reader

        :return:
        """
        running_reader = []
        for name, reader in self._config_readers:
            if reader is None or reader.done() or reader.cancelled():
                continue
            else:
                running_reader.append(name)
        if running_reader:
            logging.info("Configs waiting for jobs to finish: %s" % ' '.join(running_reader))

    def cancel_all_reader(self):
        for name, reader in self._config_readers:
            if reader and not reader.done() and not reader.cancelled():
                logging.warning("Stop config reader: %s" % name)
                reader.cancel()

    def check_for_exceptions(self):
        for name, reader in self._config_readers:
            if reader and reader.done():
                e = reader.exception()
                if e:
                    raise e

    def add_reader_thread(self, thread_name):
        reader_name = thread_name.split(':')[0]
        self._reader_threads[reader_name][thread_name] = time.time()

    def remove_reader_thread(self, thread_name):
        reader_name = thread_name.split(':')[0]
        del self._reader_threads[reader_name][thread_name]


class RecipeFinder:
    @classmethod
    def find_spec(cls, fullname, path, target=None):
        for load_path in gs.IMPORT_PATHS:
            if load_path.endswith(os.path.sep):
                module_dir = load_path[:-1]
                module_prefix = ''
            else:
                module_dir = os.path.dirname(load_path)
                if not module_dir:
                    module_dir = '.'
                module_prefix = os.path.basename(load_path)

            if not module_prefix or fullname == module_prefix or fullname.startswith(module_prefix + '.'):
                if path is None:
                    search_path = [os.path.abspath(module_dir)]
                elif isinstance(path, str):
                    search_path = [os.path.abspath(os.path.join(module_dir, path))]
                else:
                    search_path = path
                spec = PathFinder.find_spec(fullname, search_path, target)
                if spec:
                    return spec

    @classmethod
    def invalidate_caches(cls):
        PathFinder.invalidate_caches()


config_manager = ConfigManager()
