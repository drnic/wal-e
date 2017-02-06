import gevent
import shutil

from pathlib import Path
from os.path import getsize
from os.path import dirname
from urllib.parse import urlparse

from wal_e import files
from wal_e import log_help
from wal_e.pipeline import get_download_pipeline
from wal_e.piper import PIPE

logger = log_help.WalELogger(__name__)


def uri_put_file(creds, uri, fp, content_type=None):
    assert fp.tell() == 0
    assert uri.startswith('remote://')

    print(urlparse(uri))
    raise NotImplementedError()


def uri_get_file(creds, url, conn=None):
    raise NotImplementedError()


def do_lzop_get(creds, url, path, decrypt, do_retry=True):
    """
    Get and decompress a Remote Server URL

    This streams the content directly to lzop; the compressed version
    is never stored on disk.

    """
    assert url.endswith('.lzo'), 'Expect an lzop-compressed file'
    assert url.startswith('remote://')

    raise NotImplementedError()


def write_and_return_error(src_path, stream):
    raise NotImplementedError()
