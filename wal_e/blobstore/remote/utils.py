import gevent

from urllib.parse import urlparse

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

    conn = calling_format.connect(creds)
    return conn.put_file(fp, dst_path)


def do_lzop_get(creds, uri, path, decrypt, do_retry=True):
    """
    Get and decompress a Remote Server URL

    This streams the content directly to lzop; the compressed version
    is never stored on disk.

    """
    assert uri.endswith('.lzo'), 'Expect an lzop-compressed file'
    assert uri.startswith('remote://')

    def download():
        conn = calling_format.connect(creds)
        with files.DeleteOnError(path) as decomp_out:
            with get_download_pipeline(PIPE, decomp_out.f, decrypt) as pl:
                g = gevent.spawn(write_and_return_error,
                                 conn,
                                 uri,
                                 pl.stdin)

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
                                'located is {uri}.'.format(uri=uri)),
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
    return conn.get_file(object_path).read()


def write_and_return_error(conn, uri, stream):
    try:
        contents = uri_get_file(None, uri, conn)
        stream.write(contents)
        stream.flush()

        # resp_chunk_size = 8192
        # response.seek(0)
        # chunk = response.read(resp_chunk_size)
        # while True:
        #     try:
        #         chunk = response.read(resp_chunk_size)
        #         stream.write(chunk)
        #     except EOFError:
        #         break

    except Exception as e:
        return e
    finally:
        stream.close()
