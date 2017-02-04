import pytest

from wal_e import exception
from wal_e.worker.files import files_deleter


def test_construction():
    """The constructor basically works."""
    files_deleter.Deleter()


def test_close_error():
    """Ensure that attempts to use a closed Deleter results in an error."""

    d = files_deleter.Deleter()
    d.close()

    with pytest.raises(exception.UserCritical):
        d.delete('no value should work')
