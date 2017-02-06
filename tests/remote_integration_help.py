import os
import pytest

from wal_e.cmd import parse_boolean_envvar


def no_real_remote_credentials():
    """Helps skip integration tests without live credentials.

    Phrased in the negative to make it read better with 'skipif'.
    """
    if parse_boolean_envvar(
            os.getenv('WALE_REMOTE_INTEGRATION_TESTS')) is not True:
        return True

    if os.getenv('REMOTE_PRIVATE_KEY') is None:
        return True

    return False
