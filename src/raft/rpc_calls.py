import pickle
from typing import NamedTuple, Union, List

from raft.log import LogEntry

AppendEntriesRequest = NamedTuple(
    "AppendEntriesRequest",
    term=int,
    leader_id=str,
    prev_log_index=int,
    prev_log_term=int,
    entries=List[LogEntry],
    leader_commit=int
)
AppendEntriesReply = NamedTuple(
    "AppendEntriesReply",
    term=int,
    succes=bool,
    last_log_index=int,
)
RequestVoteRequest = NamedTuple(
    "RequestVoteRequest",
    term=int,
    candidate_id=str,
    last_log_index=int,
    last_log_term=int,
)
RequestVoteReply = NamedTuple(
    "RequestVoteReply",
    term=int,
    vote_granted=bool,
)
Command = NamedTuple(
    "Command",
    cmd=str,
)

Close = NamedTuple("Close")
Ok = NamedTuple(
    "Ok",
)
Action = Union[AppendEntriesRequest, RequestVoteRequest, AppendEntriesReply, RequestVoteReply, Command, Ok]


class Message:
    def __init__(self, action: Action):
        self.action = action

    def __bytes__(self):
        return pickle.dumps(self.__dict__)

    @classmethod
    def from_bytes(cls, bytes):
        return cls(**pickle.loads(bytes))


