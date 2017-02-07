import os
import pytest

from wal_e.cmd import parse_boolean_envvar


def remote_integration_tests_enabled():
    """Helps skip integration tests without live credentials.

    Phrased in the negative to make it read better with 'skipif'.
    """
    return parse_boolean_envvar(os.getenv('WALE_REMOTE_INTEGRATION_TESTS'))


@pytest.fixture(scope='session')
def test_remote_server():
    return "localhost/tmp/wal-e-testing/remote"
