from urllib.parse import urlparse

from wal_e.operator.backup import Backup
from wal_e.blobstore.local import calling_format
from wal_e.worker.local import local_worker


class LocalBackup(Backup):
    """
    A performs copies of PostgreSQL WAL files to local file system

    """
    def __init__(self, layout, gpg_key_id):
        super(LocalBackup, self).__init__(layout, None, gpg_key_id)
        url_tup = urlparse(layout.prefix)
        self.folder_path = url_tup.path
        self.cinfo = calling_format
        self.worker = local_worker
