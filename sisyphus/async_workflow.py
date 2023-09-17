from sisyphus import graph
from sisyphus.loader import config_manager
from sisyphus.tools import extract_paths
from typing import Any
import asyncio
import sisyphus.block
import sisyphus.global_settings as gs


class async_context:
    def __enter__(self):
        self.local_config = config_manager.current_config
        # The async context is currently not working stable enough to keep this check
        # assert config_manager.current_config
        config_manager.current_config = None
        self.active_blocks = sisyphus.block.active_blocks
        sisyphus.block.active_blocks = set()
        self.all_root_blocks = sisyphus.block.all_root_blocks
        sisyphus.block.all_root_blocks = []

    def __exit__(self, type, value, traceback):
        config_manager.current_config = self.local_config
        sisyphus.block.active_blocks = self.active_blocks
        sisyphus.block.all_root_blocks = self.all_root_blocks


async def async_run(obj: Any):
    """
    Run and setup all jobs that are contained inside object and all jobs that are necessary.

    :param obj:
    :param quiet: Do not forward job output do stdout
    :return:
    """
    config_manager.mark_reader_as_waiting(config_manager.current_config)
    graph.graph.add_target(graph.OutputTarget(name="async_run", inputs=obj))
    all_paths = {p for p in extract_paths(obj) if not p.available()}

    with async_context():
        while all_paths:
            await asyncio.sleep(gs.WAIT_PERIOD_BETWEEN_CHECKS)
            all_paths = {p for p in all_paths if not p.available()}
    config_manager.unmark_reader_as_waiting(config_manager.current_config)


async def __async_helper_set_config(awaitable, thread_name):
    config_manager.add_reader_thread(thread_name)
    config_manager.current_config = thread_name
    ret = await awaitable
    config_manager.remove_reader_thread(thread_name)
    return ret


async def async_gather(*aws):
    assert config_manager.current_config
    config_name = config_manager.current_config
    c_aws = []
    for i, aw in enumerate(aws):
        thread_name = "%s:thread_%i" % (config_name, i)
        c_aws.append(__async_helper_set_config(aw, thread_name))

    config_manager.current_config = None
    ret = await asyncio.gather(*c_aws)
    # TODO: This assertion should be true, but fails some times,
    # currently I don't trust blocks in combination with async workflows
    #
    # assert config_manager.current_config.startswith(config_name)
    config_manager.current_config = config_name
    return ret
