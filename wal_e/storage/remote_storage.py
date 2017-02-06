import json

from wal_e.blobstore import remote
from wal_e.storage.base import BackupInfo


class RemoteBackupInfo(BackupInfo):

    def load_detail(self, conn):
        if self._details_loaded:
            return

        uri = "{scheme}://{netloc}/{path}".format(
            scheme=self.layout.scheme,
            netloc=self.layout.store_name(),
            path=self.layout.basebackup_sentinel(self))

        data = json.loads(remote.uri_get_file(None, uri, conn=conn)
                          .decode('utf-8'))
        for k, v in list(data.items()):
            setattr(self, k, v)

        self._details_loaded = True
