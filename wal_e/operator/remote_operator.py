from urllib.parse import urlparse

from wal_e.operator.backup import Backup
from wal_e.blobstore.remote import calling_format
from wal_e.worker.remote import remote_worker


class RemoteBackup(Backup):
    """
    A performs copies of PostgreSQL WAL files to remote server's file system

    """
    def __init__(self, layout, gpg_key_id):
        super(RemoteBackup, self).__init__(layout, None, gpg_key_id)
        url_tup = urlparse(layout.prefix)
        print(url_tup)
        self.url_tup = url_tup
        self.cinfo = calling_format
        self.worker = remote_worker
