from pathlib import Path


class StorageClient:
    def __init__(self, log_file_path: str, aof_file_path):
        self.log_file_path = log_file_path
        self.aof_file_path = aof_file_path
        # Ensure files exist.
        Path(self.log_file_path).touch(exist_ok=True)
        Path(self.aof_file_path).touch(exist_ok=True)
        self.dict = {}

    def __enter__(self):
        self.aof_connection = open(self.aof_file_path, 'a+')

    def __exit__(self):
        self.aof_connection.close()

    def load(self):
        self.dict = {}
        with open(self.log_file_path, 'r+') as f:
            for line in f.readlines():
                key, val = line.split(':')
                self.dict[key] = val

    def dump(self):
        with open(self.log_file_path, 'w+') as f:
            for key, val in self.dict:
                f.write(f"{key}:{val}")

    def write_aof(self, cmd):
        self.aof_connection.write(cmd + '\n')

    def replay_aof(self):
        self.dict = {}
        for line in self.aof_connection.readlines():
            if "SET" in line:
                cmd, key, val = line.split(':')
                self.dict[key] = val
            if "DEL" in line:
                cmd, key = line.split(":")
                del self.dict[key]

    def __setitem__(self, key, value):
        self.dict[key] = value
        self.write_aof(f"SET:{key}:{value}")

    def __getitem__(self, key):
        return self.dict[key]

    def __delitem__(self, key):
        try:
            del self.dict[key]
        except KeyError:
            raise
        # Only write in log if key exists.
        self.write_aof(f"DEL:{key}")
