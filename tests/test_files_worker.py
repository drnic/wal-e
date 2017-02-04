# import pytest

from wal_e.worker.files import BackupList
from wal_e import storage

from files_integration_help import (
    FreshFolder,
)


def test_empty_latest_listing():
    """Test listing a 'backup-list LATEST' on an empty prefix."""
    folder_name = 'wal-e-test-empty-listing'
    layout = storage.StorageLayout('file://{0}/test-prefix'
                                   .format(folder_name))

    with FreshFolder(folder_name) as folder:
        folder.create()
        bl = BackupList(folder.conn, layout, False)
        found = list(bl.find_all('LATEST'))
        assert len(found) == 0
