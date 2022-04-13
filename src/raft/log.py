from dataclasses import dataclass
from typing import List
from collections import UserList
from logging import getLogger

logger = getLogger(__name__)


@dataclass
class LogEntry:
    term: int
    # command: str


class TermNotOk(Exception):
    pass


class LogNotCaughtUpException(Exception):
    pass


class MessageConflict(Exception):
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
            return OneIndexList(self.data[slice(start, stop)])
        else:
            return self.data[index - 1]

    def __setitem__(self, index, value):
        self.data[index - 1] = value

    def __delitem__(self, index):
        del self.data[index - 1]

    def to_list(self):
        return self.data

    def append(self, value: LogEntry):
        self.data.append(value)


class Log:
    def __init__(self, log_file):
        self.logs = OneIndexList()
        self.log_file = log_file

    def append_entries(self, prev_log_index, prev_log_term, entries: List[LogEntry]):
        if prev_log_index > len(self.logs):
            raise LogNotCaughtUpException()

        if prev_log_index != 0 and self.logs[prev_log_index].term != prev_log_term:
            raise TermNotOk(f"Current prev log entry: {self.logs[prev_log_index].term} != {prev_log_term}")

        for idx, entry in enumerate(entries, prev_log_index + 1):
            if idx <= len(self.logs) and self.logs[idx].term != entry.term:
                # we already have an entry at idx that conflicts
                self.truncate(idx)
            elif idx <= len(self.logs):
                continue
            elif entry != entry:
                raise MessageConflict(f"{entry} does not equal {entry}")

            self.append(entry)

    def append(self, entry):
        self.logs.append(entry)
        self.write_to_log(entry)

    def write_to_log(self, entry: LogEntry):
        logger.info(f"Writing and flushing logs to file!! {entry}")
        self.log_file.write(f"{entry.term}\n")
        self.log_file.flush()

    def replay(self):
        logger.info("Replaying aof log.")
        # need to set cursor to start at file since in append mode.
        self.log_file.seek(0)
        for line in self.log_file.readlines():
            self.logs.append(LogEntry(int(line.strip())))

    def truncate(self, index=1):
        self.logs = self.logs[:index]

    def __getitem__(self, index):
        return self.logs[index]

    def __len__(self):
        return len(self.logs)
