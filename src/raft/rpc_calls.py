import pickle
from typing import NamedTuple, Union, List, Tuple

from raft.log import LogEntry

AppendEntriesRequest = NamedTuple(
    "AppendEntriesRequest",
    term=int,
    leader_id=int,
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
    entries_added=int,
)
RequestVoteRequest = NamedTuple(
    "RequestVoteRequest",
    term=int,
    candidate_id=Tuple[str, int],
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

SetValue = NamedTuple("SetValue", message_id=str, key=str, value=str)
GetValue = NamedTuple("GetValue", message_id=str, key=str)
DelValue = NamedTuple("DelValue", message_id=str, key=str)
Value    = NamedTuple("Value", value=str)
Forward  = NamedTuple("Forward", leader_host=str, leader_port=int)
Close = NamedTuple("Close")
Ok = NamedTuple(
    "Ok",
)
Action = Union[
    AppendEntriesRequest, RequestVoteRequest, AppendEntriesReply, RequestVoteReply, Command,
    SetValue, GetValue, DelValue, Value, Close, Ok]


class Message:
    def __init__(self, action: Action, sender: Tuple[str, int], receiver: Tuple[str, int]):
        self.action = action
        self.sender = sender
        self.receiver = receiver

    def __bytes__(self):
        return pickle.dumps(self.__dict__)

    @classmethod
    def from_bytes(cls, bytes):
        return cls(**pickle.loads(bytes))



