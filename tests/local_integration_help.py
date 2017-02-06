import os
import pytest
from shutil import rmtree
from pathlib import Path

from wal_e import log_help
from wal_e.cmd import parse_boolean_envvar


logger = log_help.WalELogger(__name__)


def local_integration_tests_enabled():
    """Helps skip integration tests without live credentials.

    Phrased in the negative to make it read better with 'skipif'.
    """
    return parse_boolean_envvar(
            os.getenv('WALE_LOCAL_INTEGRATION_TESTS'))

@pytest.fixture(scope='session')
def default_test_folder():
    return "/tmp/wal-e-testing/files/default_test_folder"


def apathetic_folder_delete(folder):
    rmtree(folder, True)


def insistent_folder_delete(conn, folder):
    rmtree(folder, True)


def insistent_folder_create(conn, folder, *args, **kwargs):
    Path(folder).mkdir(0o777, True, True)


class FreshFolder(object):

    def __init__(self, folder):
        self.folder = folder
        self.created_folder = False

    def __enter__(self):
        # Clean up a dangling folder from a previous test run, if
        # necessary.
        self.conn = apathetic_folder_delete(self.folder)

        return self

    def create(self, *args, **kwargs):
        folder = insistent_folder_create(self.conn, self.folder,
                                         *args, **kwargs)

        self.created_folder = True

        return folder

    def __exit__(self, typ, value, traceback):
        if not self.created_folder:
            return False

        insistent_folder_delete(self.conn, self.folder)

        return False
