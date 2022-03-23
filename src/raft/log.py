from dataclasses import dataclass
from typing import List


@dataclass
class LogEntry:
    term: int
    # command: str


class TermNotOk(Exception):
    pass


class Log:
    def __init__(self):
        self.logs = []
        
    def recover(self):
        pass

    def append(self, entry: LogEntry):
        self.logs.append(entry)

    def append_entries(self, prev_log_index, prev_log_term, entries: List[LogEntry]):
        prev_log_entry = self.get_log_entry(prev_log_index)
        if prev_log_entry.term != prev_log_term:
            raise TermNotOk(f"Current prev log entry: {prev_log_term} != {prev_log_term}")

        for index, entry in enumerate(entries):
            entry_log_index = prev_log_index + index
            if len(self.logs) < entry_log_index:
                self.append(entry)
            else:
                current_entry = self.get_log_entry(entry_log_index)
                if current_entry.term != entry.term:
                    self.logs[entry_log_index - 1] = entry
                    self.truncate(entry_log_index)

    # Both functions under here have off by one.
    def get_log_entry(self, index) -> LogEntry:
        return self.logs[index - 1]

    def get_log_entries(self, index) -> List[LogEntry]:
        return self.logs[index - 1:]

    def truncate(self, index):
        self.logs = self.logs[:index - 1]

    def get_last_index(self) -> int:
        return len(self.logs)
