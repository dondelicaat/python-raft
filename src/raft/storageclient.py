from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class StorageClient:
    def __init__(self, aof_file_path):
        self.aof_file_path = aof_file_path
        # Ensure files exist.
        Path(self.aof_file_path).touch(exist_ok=True)
        self.dict = {}

    def __enter__(self):
        logger.info("Setting up connection to %s", format(self.aof_file_path))
        self.aof_connection = open(self.aof_file_path, 'a+')

    def __exit__(self, exception_type, exception_value, traceback):
        logger.info("Closing connection")
        self.aof_connection.close()

    def write_aof(self, cmd):
        self.aof_connection.write(cmd + '\n')
        self.aof_connection.flush()

    def replay_aof(self):
        logger.info("Replaying aof log.")
        self.dict = {}
        # need to set cursor to start at file since in append mode.
        self.aof_connection.seek(0)
        for line in self.aof_connection.readlines():
            if "SET" in line:
                cmd, key, val = line.strip().split(':')
                self.dict[key] = val
            if "DEL" in line:
                cmd, key = line.strip().split(":")
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
