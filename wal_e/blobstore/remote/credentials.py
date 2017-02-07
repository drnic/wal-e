from wal_e import log_help

logger = log_help.WalELogger(__name__)


class Credentials(object):
    def __init__(self, user, identity_file, host, port):
        self.user = user
        self.identity_file = identity_file
        self.host = host
        self.port = port or '22'

        if not self.user:
            logger.error(
                msg=('remote strategy requires user'),
                hint=('Pass in via REMOTE_USER env var.'))

        if not self.identity_file:
            logger.error(
                msg=('remote strategy requires identity file'),
                hint=('Pass in via REMOTE_IDENTITY_FILE env var.'))

        if not self.host:
            logger.error(
                msg=('remote strategy requires host'),
                hint=('Pass in via REMOTE_HOST env var.'))
