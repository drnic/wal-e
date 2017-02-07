import subprocess

from os.path import dirname


class RemoteServerConnection:
    def __init__(self, creds):
        self.creds = creds
        self.user_host = '%s@%s' % (self.creds.user, self.creds.host)

    def get_file(self, path):
        cmd = 'cat %s' % path
        proc = subprocess.Popen([
            'ssh',
            '-o', 'StrictHostKeyChecking=no',
            '-i', self.creds.identity_file,
            '-p', self.creds.port,
            self.user_host, cmd], stdout=subprocess.PIPE)

        return proc.stdout

    def put_file(self, fp, dst_path):
        dst_dir = dirname(dst_path)

        # stat -c %s returns the file size, which is wrapped in FileWrapper
        #   ubuntu: -c %s
        #   darwin: -f %z
        cmd = 'mkdir -p %s && ' \
            'cat > %s && ' \
            '[[ $(uname) == "Linux" ]] && ' \
            'stat -c "%%s" %s || ' \
            'stat -f "%%z" %s' % (dst_dir, dst_path, dst_path, dst_path)
        with subprocess.Popen([
            'ssh',
            '-o', 'StrictHostKeyChecking=no',
            '-i', self.creds.identity_file,
            '-p', self.creds.port,
            self.user_host,
            cmd], stdin=subprocess.PIPE, stdout=subprocess.PIPE) as proc:

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

            size = int(outs.decode())
            return FileWrapper(size)

    def list_files(self, prefix):
        '''
        $ stat -c "%n::%s" /path/to/storage/**/*
        /path/to/storage::102
        /path/to/storage/basebackups_005::136
        /path/to/storage/basebackups_005/base_000000000000000000000000_00000000::136
        /path/to/storage/basebackups_005/base_000000000000000000000000_00000000/extended_version.txt::15
        /path/to/storage/basebackups_005/base_000000000000000000000000_00000000/tar_partitions::102
        /path/to/storage/basebackups_005/base_000000000000000000000000_00000000/tar_partitions/part_00000000.tar.lzo::1782
        /path/to/storage/basebackups_005/base_000000000000000000000000_00000000_backup_stop_sentinel.json::306
        '''
        # try both ubuntu then darwin flags
        cmd = '[[ $(uname) == "Linux" ]] && ' \
            'stat -c "%%n::%%s" %s**/* || ' \
            'stat -f "%%N::%%z" %s**/*' % (prefix, prefix)
        with subprocess.Popen([
            'ssh',
            '-o', 'StrictHostKeyChecking=no',
            '-i', self.creds.identity_file,
            '-p', self.creds.port,
            self.user_host, cmd], stdout=subprocess.PIPE) as proc:

            stdout = proc.stdout.read().decode()
            files_info = stdout.split('\n')[:-1]

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
