import logging
from queue import Queue

from raft.raft_state_machine import RoleStateMachine
from raft.rpc_calls import Message, AppendEntriesRequest, RequestVoteRequest, Command

logger = logging.getLogger(__name__)


class MessageProcessor:
    def __init__(self, in_queue: Queue, out_queue: Queue, state_machine: RoleStateMachine):
        self.out_queue = out_queue
        self.in_queue = in_queue
        self.state_machine = state_machine

    def process(self):
        while True:
            message, client_id = self.in_queue.get(block=True)
            resp = self.handle_msg(message)
            self.out_queue.put((resp, client_id))

    def handle_msg(self, msg: Message) -> Message:
        if isinstance(msg.action, AppendEntriesRequest):
            self.state_machine.handle_append_entries(msg.action)
        elif isinstance(msg.action, RequestVoteRequest):
            pass
        elif isinstance(msg.action, Command):
            pass
        else:
            raise ValueError("Uknown type.")
