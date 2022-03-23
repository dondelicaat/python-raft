from queue import Queue
from random import randint
from typing import List

from raft.log import Log, LogEntry, TermNotOk
from raft.rpc_calls import AppendEntriesRequest, AppendEntriesReply, RequestVoteReply, \
    RequestVoteRequest, Command, Action


class RoleStateMachine:
    def __init__(
        self,
        number_of_machines,
        outbox: Queue,
        log: Log,
        role="follower",
        current_term=0,
        timeout_provider=lambda: randint(150, 300),
    ):
        self.role = role
        self.number_of_machines = number_of_machines
        self.outbox = outbox
        self.log = log
        self.leader_id = None

        self.votes_received = 0
        self.voted_for = None  # todo, some id?
        self.current_term = current_term

        self.timeout_ms = None
        self.timeout_provider = timeout_provider
        self.reset_timeout()

        self.commit_index = 0
        self.last_applied = 0

        # nextIndex = array[indexOfFollower <-> entries]
        # matchIndex = array[indexReplicatedFollowers <-> entries]

    def reset_timeout(self):
        self.timeout_ms = self.timeout_provider()

    def handle_message(self, message: Action):
        reply = None

        if isinstance(message, AppendEntriesRequest):
            if message.term < self.current_term:
                reply = AppendEntriesReply(self.current_term, False)
            else:
                reply = self.handle_append_entries(message)
        elif isinstance(message, RequestVoteRequest):
            pass
        else:
            raise ValueError("Uknown message type.")

    def handle_append_entries(self, message: AppendEntriesRequest) -> AppendEntriesReply:
        self.reset_timeout()
        if self.role == "follower":
            try:
                self.log.append_entries(
                    prev_log_index=message.prev_log_index,
                    prev_log_term=message.prev_log_term,
                    entries=message.entries,
                )

                if message.leader_commit > self.commit_index:
                    self.commit_index = min(message.leader_commit, self.log.get_last_index())

                return AppendEntriesReply(self.current_term, True)
            except (TermNotOk, IndexError):
                return AppendEntriesReply(self.current_term, False)




        if message.term >= self.current_term and (self.role == "candidate" or self.role == "leader"):
            self.leader_id = message.leader_id
            self._set_follower()

    def handle_request_vote(self, message: RequestVoteRequest) -> RequestVoteReply:
        if message.term < self.current_term:
            return RequestVoteReply(self.current_term, False)
        if (
            self.voted_for is None or self.voted_for == message.candidate_id
            and message.last_log_index >= self.log.get_last_applied_index()
        ):  # should term also be the same?
            self.voted_for = message.candidate_id
            return RequestVoteReply(self.current_term, True)
        return RequestVoteReply(self.current_term, False)

    def handle_tick(self):
        self.timeout_ms -= 1
        if self.timeout_ms <= 0:
            self.handle_timeout()

    def handle_timeout(self):
        if self.role == "follower" or self.role == "candidate":
            self._set_candidate()
        elif self.role == "leader":
            self.reset_timeout()

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
        self.voted_for = None  # Todo: set to self.id
        self.broadcast_election()
        self.reset_timeout()

    def _set_leader(self):
        self.role = "leader"
        self.votes_received = 0
        self.voted_for = None
        self.reset_timeout()

    def _set_follower(self):
        self.role = "follower"
        self.votes_received = 0
        self.voted_for = None
        self.reset_timeout()

    def broadcast_election(self):
        request_vote_request = RequestVoteRequest(
            term=self.current_term,
            candidate_id=..., #todo: self.id,
            last_log_index=self.last_applied,
            last_log_term=self.log.get_log_entry(self.last_applied).term,
        )
        for server in self.servers:
            self.outbox.put(server.id, request_vote_request)

    def broadcast_messages(self, messages: List[LogEntry]):
        append_entries_request = AppendEntriesRequest(
            term=self.current_term,
            leader_id=..., #todo: self.id
            prev_log_index=...,
            prev_log_term=...,
            entries=messages,
            leader_commit=...,
        )

        for server in self.servers:
            self.outbox.put(server.id, append_entries_request)

    def handle_client_request(self, msg: Command, client_id: str):
        if self.role == "leader":
            ... # handle_add_msg leader rules
            self.outbox.put((client_id, msg))
        if self.role == "follower":
            # Redirects the msg to the leader.
            self.outbox.put((self.leader_id, msg))
