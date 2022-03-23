from dataclasses import dataclass
from typing import List
from collections import UserList


@dataclass
class LogEntry:
    term: int
    # command: str


class TermNotOk(Exception):
    pass


class OneIndexList(UserList):
    def __init__(self, _list=None):
        super().__init__()
        if _list:
            self.data = _list

    def __getitem__(self, index):
        if isinstance(index, slice):
            start, stop = None, None
            if index.start:
                start = index.start - 1
            if index.stop:
                stop = index.stop - 1
            return self.data[slice(start, stop)]
        else:
            return self.data[index - 1]

    def __setitem__(self, index, value):
        self.data[index - 1] = value

    def __delitem__(self, index):
        del self.data[index - 1]

    def append(self, value: LogEntry):
        self.data.append(value)


class Log:
    def __init__(self):
        self.logs = OneIndexList()
        
    def recover(self):
        pass

    def append_entries(self, prev_log_index, prev_log_term, entries: List[LogEntry]):
        if prev_log_index == 0:
            self.truncate()
            for entry in entries:
                self.logs.append(entry)
            return

        prev_log_entry = self.logs[prev_log_index]
        if prev_log_entry.term != prev_log_term:
            raise TermNotOk(f"Current prev log entry: {prev_log_entry.term} != {prev_log_term}")

        for index, entry in enumerate(entries):
            entry_log_index = prev_log_index + index + 1
            if len(self.logs) < entry_log_index:
                self.logs.append(entry)
            else:
                current_entry = self.logs[entry_log_index]
                if current_entry.term != entry.term:
                    self.logs[entry_log_index] = entry
                    self.truncate(entry_log_index + 1)

    def truncate(self, index=1):
        self.logs = self.logs[:index]

    def __getitem__(self, index):
        return self.logs[index]
