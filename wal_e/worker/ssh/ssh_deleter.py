from wal_e import retries
from wal_e import log_help
from wal_e.worker.base import _Deleter


logger = log_help.WalELogger(__name__)


class Deleter(_Deleter):

    @retries.retry()
    def _delete_batch(self, page):
        # TODO ssh.Deleter._delete_batch
        # envdir /etc/wal-e.d/env/ wal-e delete --confirm everything
        print("_delete_batch", page)
        for blob in page:
            # TODO delete file from parent folder
            print("_delete_batch.blob", blob)
            pass
