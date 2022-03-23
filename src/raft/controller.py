from queue import Queue

from raft.fixed_header_message import FixedHeaderMessageProtocol
from raft.log import Log
from raft.raft_state_machine import RoleStateMachine


class RaftServerController:
    def __init__(self,
        protocol: FixedHeaderMessageProtocol
    ):
        input_queue = Queue()
        output_queue = Queue()
        self.state_machine = RoleStateMachine(
            number_of_machines=5,
            outbox=output_queue,
            log=Log()
        )
        self.message_receiver = MessageProcessor(
            in_queue=self.input_queue,
            state_machine=self.state_machine
        )

    def run(self):
        pass
