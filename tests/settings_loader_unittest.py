import os
import pickle
import unittest


class SettingsTest(unittest.TestCase):
    def setUp(self):
        self.TEST_SETTINGS_FILE = "tests/settings_test.py"
        if os.path.exists(self.TEST_SETTINGS_FILE):
            raise RuntimeError(f"'{self.TEST_SETTINGS_FILE} already exists, would be deleted by running the tests.")

        # Disable loading from settings file
        os.environ["SIS_GLOBAL_SETTINGS_FILE"] = ""

    def tearDown(self):
        if os.path.exists(self.TEST_SETTINGS_FILE):
            os.unlink(self.TEST_SETTINGS_FILE)

    def test_load_settings_from_file(self):
        import sisyphus.global_settings as gs

        pre_load_env_path = gs.DEFAULT_ENVIRONMENT_SET["PATH"]

        test_file_content = """file_test1 = 'foo'
file_test2 = 4
DEFAULT_ENVIRONMENT_SET['PATH'] += ':bar'

def test_function():
    return 42
"""
        with open(self.TEST_SETTINGS_FILE, "wt") as f:
            f.write(test_file_content)

        gs.GLOBAL_SETTINGS_FILE_CONTENT = ""
        gs.update_global_settings_from_file(self.TEST_SETTINGS_FILE)

        expected_content = f"##### Settings file: {self.TEST_SETTINGS_FILE} #####\n{test_file_content}\n"
        assert gs.GLOBAL_SETTINGS_FILE_CONTENT == expected_content
        assert gs.file_test1 == "foo"
        assert gs.file_test2 == 4
        assert gs.DEFAULT_ENVIRONMENT_SET["PATH"] == pre_load_env_path + ":bar"

        assert gs.test_function() == 42
        pickled_test_function = pickle.dumps(gs.test_function)
        unpickled_test_function = pickle.loads(pickled_test_function)
        assert unpickled_test_function() == 42

    def test_load_settings_from_env(self):
        import sisyphus.global_settings as gs

        os.environ["SIS_env_test1"] = '"bar"'
        os.environ["SIS_env_test2"] = "bar"
        os.environ["SIS_env_test3"] = "42"

        gs.GLOBAL_SETTINGS_FILE_CONTENT = ""
        gs.update_global_settings_from_env()

        assert gs.env_test1 == "bar"
        assert gs.env_test2 == "bar"
        assert gs.env_test3 == 42

        expected_content = """##### Settings from environment #####
GLOBAL_SETTINGS_FILE = ''
env_test1 = 'bar'
env_test2 = 'bar'
env_test3 = 42
"""
        assert gs.GLOBAL_SETTINGS_FILE_CONTENT == expected_content


if __name__ == "__main__":
    unittest.main()
