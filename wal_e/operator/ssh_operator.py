from urllib.parse import urlparse

from wal_e.operator.backup import Backup
from wal_e.blobstore.ssh import calling_format
from wal_e.worker.ssh import ssh_worker


class RemoteBackup(Backup):
    """
    A performs copies of PostgreSQL WAL files to ssh server's file system

    """
    def __init__(self, layout, creds, gpg_key_id):
        super(RemoteBackup, self).__init__(layout, creds, gpg_key_id)
        self.url_tup = urlparse(layout.prefix)
        self.cinfo = calling_format
        self.worker = ssh_worker
