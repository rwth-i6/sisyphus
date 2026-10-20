import unittest
from unittest import mock

import sisyphus.global_settings as gs
from sisyphus import manager as manager_module
from sisyphus.engine import DeferSubmission


class FakeTask:
    def __init__(self, name):
        self.name = name

    def resumeable(self):
        return True


class FakeJob:
    def __init__(self, name):
        self.task = FakeTask(name)
        self._sis_setup_since_restart = True

    def _sis_setup_directory(self, force=False):
        pass

    def _sis_setup(self):
        return False

    def _sis_next_task(self):
        return self.task


class FakeGraph:
    def __init__(self):
        self.jobs = {}
        self.targets = []
        self.active_targets = []

    def set_job_targets(self, engine):
        pass

    def get_jobs_by_status(self, engine, skip_finished):
        return self.jobs


class FakeEngine:
    """Defers the submissions of the tasks in ``deferred`` by 60 s, like the Slurm engine on a submit limit."""

    def __init__(self, clock):
        self.clock = clock
        self.deferred = set()
        self.submitted = []

    def submit(self, task):
        self.submitted.append(task.name)
        if task.name in self.deferred:
            raise DeferSubmission("submit limit", retry_at=self.clock.now + 60)

    def reset_cache(self):
        pass

    def stop_engine(self):
        pass


class Clock:
    def __init__(self):
        self.now = 1000.0


class ManagerDeferredSubmissionTest(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.graph = FakeGraph()
        self.engine = FakeEngine(self.clock)
        p = mock.patch.object(manager_module.time, "monotonic", side_effect=lambda: self.clock.now)
        p.start()
        self.addCleanup(p.stop)
        self.manager = manager_module.Manager(sis_graph=self.graph, job_engine=self.engine)
        self.addCleanup(self.manager.thread_pool.close)
        self.deferred_job, self.other_job = FakeJob("deferred"), FakeJob("other")

    def _run_jobs_at(self, now):
        self.clock.now = now
        self.engine.submitted = []
        self.manager.run_jobs()
        return sorted(self.engine.submitted)

    def test_deferred_job_waits_until_retry_time(self):
        self.graph.jobs = {gs.STATE_RUNNABLE: [self.deferred_job, self.other_job]}
        self.manager.update_jobs()
        self.engine.deferred = {"deferred"}
        self.assertEqual(self._run_jobs_at(1000.0), ["deferred", "other"])
        # other runnable jobs are still submitted during the cooldown
        self.assertEqual(self._run_jobs_at(1001.0), ["other"])
        self.assertEqual(self._run_jobs_at(1059.0), ["other"])
        # deadline expired, deferred again: next cooldown
        self.assertEqual(self._run_jobs_at(1060.0), ["deferred", "other"])
        self.assertEqual(self._run_jobs_at(1119.0), ["other"])
        # accepted after the second cooldown, then no deadline left
        self.engine.deferred = set()
        self.assertEqual(self._run_jobs_at(1120.0), ["deferred", "other"])
        self.assertEqual(self._run_jobs_at(1121.0), ["deferred", "other"])

    def test_resume_deferred(self):
        self.graph.jobs = {gs.STATE_INTERRUPTED_RESUMABLE: [self.deferred_job]}
        self.manager.update_jobs()
        self.engine.deferred = {"deferred"}
        self.manager.resume_jobs()
        self.assertEqual(self.engine.submitted, ["deferred"])
        self.clock.now = 1059.0
        self.manager.resume_jobs()
        self.assertEqual(self.engine.submitted, ["deferred"])
        self.clock.now = 1060.0
        self.manager.resume_jobs()
        self.assertEqual(self.engine.submitted, ["deferred", "deferred"])

    def _run_manager_loop_once(self):
        self.manager.update_jobs()
        self.manager.unpause()
        self.manager.mem_profile = None
        self.engine.submitted = []
        with mock.patch.object(self.manager, "startup", return_value=True), mock.patch.object(
            self.manager, "continue_manager_loop", side_effect=[True, False]
        ), mock.patch.object(manager_module, "config_manager"), mock.patch.object(
            manager_module, "finished_results_cache"
        ), mock.patch.object(manager_module.time, "sleep") as sleep:
            self.manager.auto_print_stat_overview = False
            self.manager.run()
        return sleep.call_args_list

    def test_manager_sleeps_when_only_deferred_jobs_runnable(self):
        self.graph.jobs = {gs.STATE_RUNNABLE: [self.deferred_job]}
        self.manager._submit_deferred_until = {self.deferred_job: 1060.0}
        self.assertEqual(self._run_manager_loop_once(), [mock.call(gs.WAIT_PERIOD_BETWEEN_CHECKS)])
        self.assertEqual(self.engine.submitted, [])

    def test_manager_no_sleep_with_other_runnable_job(self):
        self.graph.jobs = {gs.STATE_RUNNABLE: [self.deferred_job, self.other_job]}
        self.manager._submit_deferred_until = {self.deferred_job: 1060.0}
        self.assertEqual(self._run_manager_loop_once(), [])
        self.assertEqual(self.engine.submitted, ["other"])


if __name__ == "__main__":
    unittest.main()
