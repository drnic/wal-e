import pytest

from wal_e import exception
from wal_e.worker.local import local_deleter


def test_construction():
    """The constructor basically works."""
    local_deleter.Deleter()


def test_close_error():
    """Ensure that attempts to use a closed Deleter results in an error."""

    d = local_deleter.Deleter()
    d.close()

    with pytest.raises(exception.UserCritical):
        d.delete('no value should work')
