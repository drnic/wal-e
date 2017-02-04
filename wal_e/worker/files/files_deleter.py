# from wal_e import retries
from wal_e import log_help
from wal_e.worker.base import _Deleter


logger = log_help.WalELogger(__name__)


class Deleter(_Deleter):

    def __init__(self, folder):
        super(Deleter, self).__init__()
        self.folder
