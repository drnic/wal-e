from urllib.parse import urlparse

from wal_e.operator.backup import Backup
from wal_e.blobstore.files import calling_format
from wal_e.worker.files import files_worker


class FilesBackup(Backup):
    """
    A performs copies of PostgreSQL WAL files to local file system

    """
    def __init__(self, layout, gpg_key_id):
        super(FilesBackup, self).__init__(layout, None, gpg_key_id)
        url_tup = urlparse(layout.prefix)
        self.folder_path = url_tup.path
        self.cinfo = calling_format
        self.worker = files_worker
