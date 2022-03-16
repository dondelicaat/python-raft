from dataclasses import dataclass


@dataclass
class AppendEntries:
    term: int = 0
