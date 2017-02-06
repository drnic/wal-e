# import gevent
import shutil

from urllib.parse import urlparse
from os.path import dirname
import subprocess
import tempfile

# from wal_e import files
from wal_e import log_help
# from wal_e.pipeline import get_download_pipeline
# from wal_e.piper import PIPE

logger = log_help.WalELogger(__name__)


def uri_put_file(creds, uri, fp, content_type=None):
    assert fp.tell() == 0
    assert uri.startswith('remote://')

    dst_path = urlparse(uri).path
    dst_dir = dirname(dst_path)

    # ssh/scp file to creds.host
    proc = subprocess.Popen([
        'ssh', '-i', creds.identity_file,
        '%s@%s' % (creds.user, creds.host),
        'mkdir -p %s && cat > %s' % (dst_dir, dst_path)], stdin=subprocess.PIPE)

    try:
        outs, errs = proc.communicate(input=fp.read(), timeout=15)
    except TimeoutExpired:
        proc.kill()
        outs, errs = proc.communicate()

    if proc.returncode != 0:
        raise SystemExit(proc.returncode)

    class FileWrapper:
        def __init__(self, size):
            self.size = size

    return FileWrapper(100)


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
