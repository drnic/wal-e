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
    assert uri.startswith('files://')

    dst_path = urlparse(uri).path
    Path(dirname(dst_path)).mkdir(0o777, True, True)

    with open(dst_path, "wb") as dst:
        byte = fp.read(1)
        while byte != "":
            dst.write(byte)
            byte = fp.read(1)
        dst.close()

    # TODO - perhaps the following?
    # shutil.copyfileobj(reader, stream)
    # stream.flush()

    # To maintain consistency with the S3 version of this function we must
    # return an object with a certain set of attributes.  Currently, that set
    # of attributes consists of only 'size'
    return getsize(dst_path)


def uri_get_file(creds, url, conn=None):
    src_path = urlparse(url).path
    return open(src_path, "r")


def do_lzop_get(creds, url, path, decrypt, do_retry=True):
    """
    Get and decompress a Local Files URL

    This streams the content directly to lzop; the compressed version
    is never stored on disk.

    """
    assert url.endswith('.lzo'), 'Expect an lzop-compressed file'
    assert url.startswith('files://')

    def download():
        with files.DeleteOnError(path) as decomp_out:
            src_path = urlparse(url).path
            with get_download_pipeline(PIPE, decomp_out.f, decrypt) as pl:
                g = gevent.spawn(write_and_return_error, src_path, pl.stdin)

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
                msg='completed file copy and decompression',
                detail='File copied and decompressed "{url}" to "{path}"'
                .format(url=url, path=path))
        return True

    return download()


def write_and_return_error(src_path, stream):
    try:
        with open(src_path, "r") as reader:
            shutil.copyfileobj(reader, stream)
            stream.flush()
    except Exception as e:
        return e
    finally:
        stream.close()
