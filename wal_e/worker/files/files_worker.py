"""
Files Storage workers

These are functions that are amenable to be called from other modules,
with the intention that they are used in gevent greenlets.
"""

# import datetime
# import gevent
# import re

from wal_e import log_help
# from wal_e import storage
# from wal_e.blobstore import files
# from wal_e.tar_partition import TarPartition
from wal_e.worker.base import _BackupList, _DeleteFromContext
from wal_e.worker.files.files_deleter import Deleter

logger = log_help.WalELogger(__name__)


class TarPartitionLister(object):
    def __init__(self, gs_conn, layout, backup_info):
        self.gs_conn = gs_conn
        self.layout = layout
        self.backup_info = backup_info

    def __iter__(self):
        return


class BackupFetcher(object):
    def __init__(self, gs_conn, layout, backup_info, local_root, decrypt):
        self.gs_conn = gs_conn
        self.layout = layout
        self.local_root = local_root
        self.backup_info = backup_info
        self.decrypt = decrypt


class BackupList(_BackupList):

    def _backup_detail(self, key):
        return key.get_contents_as_string()

    def _backup_list(self, prefix):
        return []


class DeleteFromContext(_DeleteFromContext):

    def __init__(self, gs_conn, layout, dry_run):
        super(DeleteFromContext, self).__init__(gs_conn, layout, dry_run)

        if not dry_run:
            self.deleter = Deleter()
        else:
            self.deleter = None

    def _container_name(self, key):
        return key.bucket.name

    def _backup_list(self, prefix):
        return []
