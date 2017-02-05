from pathlib import Path
from os.path import getsize
from os.path import dirname
from urllib.parse import urlparse


def uri_put_file(creds, uri, fp, content_type=None):
    assert fp.tell() == 0
    assert uri.startswith('files://')

    url_tup = urlparse(uri)
    print(("uri_put_file", creds, url_tup, fp, content_type))
    dst_path = url_tup.path
    Path(dirname(dst_path)).mkdir(0o777, True, True)

    with open(dst_path, "wb") as dst:
        byte = fp.read(1)
        while byte != "":
            dst.write(byte)
            byte = fp.read(1)
        dst.close()

    # To maintain consistency with the S3 version of this function we must
    # return an object with a certain set of attributes.  Currently, that set
    # of attributes consists of only 'size'
    return getsize(dst_path)
