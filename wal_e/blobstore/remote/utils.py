import gevent
import shutil

from urllib.parse import urlparse
from os.path import dirname
import subprocess

from wal_e import files
from wal_e import log_help
from wal_e.blobstore.remote import calling_format
from wal_e.pipeline import get_download_pipeline
from wal_e.piper import PIPE

logger = log_help.WalELogger(__name__)


def uri_put_file(creds, uri, fp, content_type=None):
    assert fp.tell() == 0
    assert uri.startswith('remote://')

    dst_path = urlparse(uri).path
    dst_dir = dirname(dst_path)

    # ssh/scp file to creds.host
    host = '%s@%s' % (creds.user, creds.host)
    # stat -f %z returns the file size, which is wrapped in FileWrapper
    cmd = 'mkdir -p %s && cat > %s && ' \
        'stat -f "%%z" %s' % (dst_dir, dst_path, dst_path)
    proc = subprocess.Popen([
        'ssh', '-i', creds.identity_file,
        host, cmd], stdin=subprocess.PIPE, stdout=subprocess.PIPE)

    try:
        outs, errs = proc.communicate(input=fp.read(), timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
        outs, errs = proc.communicate()

    if proc.returncode != 0:
        raise SystemExit(proc.returncode)

    class FileWrapper:
        def __init__(self, size):
            self.size = size

    return FileWrapper(int.from_bytes(outs, byteorder='big'))


def do_lzop_get(creds, uri, path, decrypt, do_retry=True):
    """
    Get and decompress a Remote Server URL

    This streams the content directly to lzop; the compressed version
    is never stored on disk.

    """
    assert uri.endswith('.lzo'), 'Expect an lzop-compressed file'
    assert uri.startswith('remote://')

    def download():
        with files.DeleteOnError(path) as decomp_out:
            with get_download_pipeline(PIPE, decomp_out.f, decrypt) as pl:
                g = gevent.spawn(write_and_return_error, creds, uri, pl.stdin)

                try:
                    exc = g.get()
                    if exc is not None:
                        raise exc
                except FileNotFoundError as e:
                    # Do not retry if the blob not present, this
                    # can happen under normal situations.
                    pl.abort()
                    logger.warning(
                        msg=('could no longer locate object while '
                             'performing wal restore'),
                        detail=('The absolute URI that could not be '
                                'located is {url}.'.format(url=url)),
                        hint=('This can be normal when Postgres is trying '
                              'to detect what timelines are available '
                              'during restoration.'))
                    decomp_out.remove_regardless = True
                    return False

            logger.info(
                msg='completed remote file fetch and decompression',
                detail='File downloaded and decompressed "{uri}" to "{path}"'
                .format(uri=uri, path=path))
        return True

    return download()


def uri_get_file(creds, uri, conn=None):
    assert uri.startswith('remote://')
    object_path = urlparse(uri).path

    if conn is None:
        conn = calling_format.connect(creds)
    return conn.get_file(object_path)

def write_and_return_error(creds, uri, stream):
    resp_chunk_size = 8192
    try:
        response = uri_get_file(creds, uri, None)
        stream.write(response.read())

        # chunk = response.read(resp_chunk_size)
        # response.seek(0)
        # while True:
        #     try:
        #         chunk = response.read(resp_chunk_size)
        #         stream.write(chunk)
        #     except EOFError:
        #         break

        stream.flush()
    except Exception as e:
        return e
    finally:
        stream.close()
