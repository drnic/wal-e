from wal_e import log_help


logger = log_help.WalELogger(__name__)


def apathetic_folder_delete(folder_name):
    logger.warning(
        msg='todo apathetic_folder_delete',
        detail='folder_name {0}.'.format(folder_name))

    # TODO
    return


def insistent_folder_delete(conn, folder_name):
    logger.warning(
        msg='todo insistent_folder_delete',
        detail='folder_name {0}.'.format(folder_name))
    return


def insistent_folder_create(conn, folder_name, *args, **kwargs):
    logger.warning(
        msg='todo insistent_folder_create',
        detail='folder_name {0}.'.format(folder_name))
    return "folder-create-object"


class FreshFolder(object):

    def __init__(self, folder_name):
        self.folder_name = folder_name
        self.created_folder = False

    def __enter__(self):
        # Clean up a dangling folder from a previous test run, if
        # necessary.
        self.conn = apathetic_folder_delete(self.folder_name)

        return self

    def create(self, *args, **kwargs):
        folder = insistent_folder_create(self.conn, self.folder_name,
                                         *args, **kwargs)

        self.created_folder = True

        return folder

    def __exit__(self, typ, value, traceback):
        if not self.created_folder:
            return False

        insistent_folder_delete(self.conn, self.folder_name)

        return False
