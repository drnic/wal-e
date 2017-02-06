import json

from wal_e.blobstore import local
from wal_e.storage.base import BackupInfo


class LocalBackupInfo(BackupInfo):

    def load_detail(self, conn):
        if self._details_loaded:
            return

        uri = "{scheme}://{folder}/{path}".format(
            scheme=self.layout.scheme,
            folder=self.layout.store_name(),
            path=self.layout.basebackup_sentinel(self))

        data = json.loads(local.uri_get_file(None, uri, conn=conn)
                          .decode('utf-8'))
        for k, v in list(data.items()):
            setattr(self, k, v)

        self._details_loaded = True
