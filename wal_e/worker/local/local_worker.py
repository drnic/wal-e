"""
Local Storage workers

These are functions that are amenable to be called from other modules,
with the intention that they are used in gevent greenlets.
"""

# import datetime
import gevent
import re

from pathlib import Path
from os.path import getmtime

from wal_e import log_help
from wal_e import storage
from wal_e.blobstore import local
from wal_e.pipeline import get_download_pipeline
from wal_e.piper import PIPE
from wal_e.tar_partition import TarPartition
from wal_e.worker.base import _BackupList, _DeleteFromContext
from wal_e.worker.base import generic_weird_key_hint_message
from wal_e.worker.local.local_deleter import Deleter

logger = log_help.WalELogger(__name__)


class TarPartitionLister(object):
    def __init__(self, local_conn, layout, backup_info):
        self.local_conn = local_conn
        self.layout = layout
        self.backup_info = backup_info

    def __iter__(self):
        prefix = '/' + self.layout.basebackup_tar_partition_directory(
            self.backup_info)

        for file in Path(prefix).glob("**/*"):
            file_last_part = file.name.rsplit('/', 1)[-1]
            match = re.match(storage.VOLUME_REGEXP, file_last_part)
            if match is None:
                logger.warning(
                    msg='unexpected object found in tar volume directory',
                    detail=('The unexpected key is stored at "{0}".'
                            .format(file_last_part)),
                    hint=generic_weird_key_hint_message)
            else:
                yield file_last_part


class BackupFetcher(object):
    def __init__(self, conn, layout, backup_info, local_root, decrypt):
        self.conn = conn
        self.layout = layout
        self.local_root = local_root
        self.backup_info = backup_info
        self.decrypt = decrypt

    def fetch_partition(self, partition_name):
        part_abs_path = '/' + self.layout.basebackup_tar_partition(
            self.backup_info, partition_name)

        logger.info(
            msg='beginning partition copy',
            detail='The partition being copied is {0}.'
            .format(partition_name),
            hint='The absolute file path is {0}.'.format(part_abs_path))

        with get_download_pipeline(PIPE, PIPE, self.decrypt) as pl:
            g = gevent.spawn(local.write_and_return_error,
                             part_abs_path,
                             pl.stdin)
            TarPartition.tarfile_extract(pl.stdout, self.local_root)

            # Raise any exceptions guarded by write_and_return_error.
            exc = g.get()
            if exc is not None:
                raise exc


class BackupList(_BackupList):

    def _backup_detail(self, key):
        return key.get_contents_as_string()

    def _backup_list(self, prefix):
        prefix = '/' + prefix

        class Key:
            def __init__(self, file_path):
                self.name = str(file_path)
                self.last_modified = getmtime(self.name)

        return map(Key, Path(prefix).glob("**/*"))


class DeleteFromContext(_DeleteFromContext):
    # TODO DeleteFromContext

    def __init__(self, local_conn, layout, dry_run):
        super(DeleteFromContext, self).__init__(local_conn, layout, dry_run)

        if not dry_run:
            self.deleter = Deleter()
        else:
            self.deleter = None

    def _container_name(self, key):
        return key.bucket.name

    def _backup_list(self, prefix):
        return []
