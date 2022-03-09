import pickle
from typing import NamedTuple, Union

SetValue = NamedTuple("SetValue", key=str, value=str)
GetValue = NamedTuple("GetValue", key=str)
DelValue = NamedTuple("DelValue", key=str)
Value    = NamedTuple("Value", value=str)
Ok       = NamedTuple("Ok", value=bool)
Action = Union[SetValue, GetValue, DelValue, Value, Ok]


class Message:
    def __init__(self, action: Action):
        self.action = action

    def __bytes__(self):
        return pickle.dumps(self.__dict__)

    @classmethod
    def from_bytes(cls, bytes):
        return cls(**pickle.loads(bytes))


