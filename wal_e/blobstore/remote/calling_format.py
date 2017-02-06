class RemoteServerConnection:
    def __init__(self, creds):
        self.creds = creds


def connect(creds):
    """
    Construct a wrapper for target server access,
    and confirm can connect.
    """
    return RemoteServerConnection(creds)
