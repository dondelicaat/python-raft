from queue import Queue
from random import randint

from raft.rpc_calls import AppendEntries


class RoleStateMachine:
    def __init__(
        self,
        number_of_machines,
        outbox: Queue,
        role="follower",
        current_term=0,
        votes_received=0,
        timeout_provider=lambda: randint(150, 300),
    ):
        self.role = role
        self.number_of_machines = number_of_machines
        self.votes_received = votes_received
        self.current_term = current_term
        self.timeout_ms = None
        self.timeout_provider = timeout_provider
        self.reset_timeout()
        self.outbox = outbox

    def reset_timeout(self):
        self.timeout_ms = self.timeout_provider()

    def handle_append_entries(self, message: AppendEntries):
        self.reset_timeout()
        if message.term >= self.current_term and (self.role == "candidate" or self.role == "leader"):
            self._set_follower()

    def handle_tick(self):
        self.timeout_ms -= 1
        if self.timeout_ms <= 0:
            self.handle_timeout()

    def handle_timeout(self):
        if self.role == "follower" or self.role == "candidate":
            self._set_candidate()

    def handle_vote(self):
        self.votes_received += 1
        if self.received_majority_vote() and self.role == "candidate":
            self._set_leader()

    def received_majority_vote(self):
        return self.votes_received / self.number_of_machines > 0.5

    def _set_candidate(self):
        self.role = "candidate"
        self.votes_received = 1
        self.current_term += 1
        self.reset_timeout()

    def _set_leader(self):
        self.role = "leader"
        self.votes_received = 0
        self.reset_timeout()

    def _set_follower(self):
        self.role = "follower"
        self.votes_received = 0
        self.reset_timeout()

