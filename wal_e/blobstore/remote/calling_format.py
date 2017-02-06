import subprocess


class RemoteServerConnection:
    def __init__(self, creds):
        self.creds = creds
        self.user_host = '%s@%s' % (self.creds.user, self.creds.host)

    def get_file(self, path):
        cmd = 'cat %s' % path
        proc = subprocess.Popen([
            'ssh', '-i', self.creds.identity_file,
            self.user_host, cmd], stdout=subprocess.PIPE)

        return proc.stdout

    def list_files(self, prefix):
        '''
        $ stat -f "%N::%z" /path/to/storage/**/*
        /path/to/storage::102
        /path/to/storage/basebackups_005::136
        /path/to/storage/basebackups_005/base_000000000000000000000000_00000000::136
        /path/to/storage/basebackups_005/base_000000000000000000000000_00000000/extended_version.txt::15
        /path/to/storage/basebackups_005/base_000000000000000000000000_00000000/tar_partitions::102
        /path/to/storage/basebackups_005/base_000000000000000000000000_00000000/tar_partitions/part_00000000.tar.lzo::1782
        /path/to/storage/basebackups_005/base_000000000000000000000000_00000000_backup_stop_sentinel.json::306
        '''
        cmd = 'stat -f "%%N::%%z" %s**/*' % prefix
        proc = subprocess.Popen([
            'ssh', '-i', self.creds.identity_file,
            self.user_host, cmd], stdout=subprocess.PIPE)

        files_info = proc.stdout.read().decode().split('\n')[:-1]

        class FileRef:
            def __init__(self, file_info):
                items = file_info.split("::")
                self.name = items[0]
                self.last_modified = int(items[1])

        return map(FileRef, files_info)


def connect(creds):
    """
    Construct a wrapper for target server access,
    and confirm can connect.
    """
    return RemoteServerConnection(creds)
