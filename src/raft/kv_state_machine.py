import logging

logger = logging.getLogger(__name__)


class KeyValStore:
    def __init__(self):
        self.dict = {}

    def apply_log_command(self, command):
        command_parts = command.strip().split(':')
        message_id = command_parts[0]
        command = command_parts[1]
        if command == "SET":
            key = command_parts[2]
            val = command_parts[3]
            self.dict[key] = val
        elif command == "DEL":
            key = command_parts[2]
            del self.dict[key]
        elif command == "NOOP":
            pass

    def __setitem__(self, key, value):
        self.dict[key] = value

    def __getitem__(self, key):
        return self.dict[key]

    def __delitem__(self, key):
        try:
            del self.dict[key]
        except KeyError:
            raise
