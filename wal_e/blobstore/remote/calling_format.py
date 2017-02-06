import subprocess


class RemoteServerConnection:
    def __init__(self, creds):
        self.creds = creds

    def get_file(self, path):
        host = '%s@%s' % (self.creds.user, self.creds.host)
        cmd = 'cat %s' % path
        proc = subprocess.Popen([
            'ssh', '-i', self.creds.identity_file,
            host, cmd], stdout=subprocess.PIPE)

        return proc.stdout


def connect(creds):
    """
    Construct a wrapper for target server access,
    and confirm can connect.
    """
    return RemoteServerConnection(creds)
