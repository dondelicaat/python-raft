from enum import Enum
from queue import Queue
from random import randint

from raft.log import Log, TermNotOk, LogNotCaughtUpException
from raft.metadata_backend import MetadataBackend
from raft.rpc_calls import AppendEntriesRequest, AppendEntriesReply, RequestVoteReply, \
    RequestVoteRequest, Message


class Role(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


class Raft:
    def __init__(
        self,
        servers,
        server_id,
        outbox: Queue,
        log: Log,
        metadata_backend: MetadataBackend,
        timeout_provider=lambda: randint(150, 300),
    ):
        self.role = 'follower'
        self.servers = servers  # List[id, tuple(port, host)] including itself.
        self.server_id = server_id
        self.outbox = outbox
        self.log = log
        self.leader_id = None
        self.metadata_backend = metadata_backend
        self._voted_for = None  # todo, some id?
        self._current_term = None

        self.request_timeout_ms = 50
        self.timeout_ms = None
        self.timeout_provider = timeout_provider
        self.set_timeout()

        self.commit_index = 0
        self.last_applied = 0

        self.next_index = []
        self.match_index = []
        self.votes_received = set()

        self.log.replay()

    @property
    def voted_for(self):
        if self._voted_for is None:
            state = self.metadata_backend.read()
            self._voted_for = state['voted_for']
        return self._voted_for

    @voted_for.setter
    def voted_for(self, value):
        self.metadata_backend.write({'voted_for': value, 'current_term': self._current_term})
        self._voted_for = value

    @property
    def current_term(self):
        if self._current_term is None:
            state = self.metadata_backend.read()
            self._current_term = state['current_term']
        return self._current_term

    @current_term.setter
    def current_term(self, value):
        self.metadata_backend.write({'voted_for': self._voted_for, 'current_term': value})
        self._current_term = value

    def set_timeout(self):
        self.timeout_ms = self.timeout_provider()

    def handle_heartbeat(self):
        if self.role != "leader":
            return

        last_log_index = len(self.log)
        for server_id, server in enumerate(self.servers):
            server_next_idx = self.next_index[server_id]

            entries = []
            if server_next_idx <= last_log_index:
                entries = self.log[server_next_idx:].to_list()

            if server_next_idx != 0:
                prev_log_index = server_next_idx - 1
                prev_log_term = self.log[prev_log_index].term
            else:
                prev_log_index = 0
                prev_log_term = 0

            append_entries_request = AppendEntriesRequest(
                term=self.current_term,
                entries=entries,
                leader_id=self.leader_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                leader_commit=self.commit_index
            )
            self.outbox.put((append_entries_request, server_id))

    def broadcast_election(self):
        assert self.role == 'candidate'

        for server_id, _ in enumerate(self.servers):
            if server_id == self.server_id:
                continue

            last_log_index = len(self.log)
            last_log_term = self.log[last_log_index]
            request_vote_request = RequestVoteRequest(
                term=self.current_term, candidate_id=self.server_id,
                last_log_term=last_log_term, last_log_index=last_log_index
            )
            self.outbox.put((request_vote_request, server_id))

    def handle_append_entries(self, message: AppendEntriesRequest):
        assert self.role == "follower"
        self.set_timeout()

        try:
            self.log.append_entries(
                prev_log_index=message.prev_log_index,
                prev_log_term=message.prev_log_term,
                entries=message.entries,
            )
        except (TermNotOk, LogNotCaughtUpException):
            self.outbox.put((AppendEntriesReply(self.current_term, False, -1), self.server_id))
            return

        if message.leader_commit > self.commit_index:
            self.commit_index = min(message.leader_commit, len(self.log))

        last_log_index = len(self.log)
        self.outbox.put((AppendEntriesReply(self.current_term, True, last_log_index), self.server_id))

    def handle_request_vote(self, message: RequestVoteRequest):
        if (
            self.voted_for is None or self.voted_for == message.candidate_id
            and message.last_log_index >= len(self.log)
        ):  # should term also be the same? # see page 8 4.2.1 last paragraph test message.last_log_index logic as well.
            self.voted_for = message.candidate_id
            self.set_timeout()
            self.outbox.put((RequestVoteReply(self.current_term, True), self.server_id))

        self.outbox.put((RequestVoteReply(self.current_term, False), self.server_id))

    def handle_request_vote_reply(self, message: RequestVoteReply, client_id):
        if self.role != 'candidate':
            return

        if message.vote_granted:
            self.votes_received.add(client_id)

        if len(self.votes_received) / len(self.servers) > 0.5:
            self._set_leader()

    def handle_append_entries_reply(self, message: AppendEntriesReply, server_id):
        if not message.succes:
            self.next_index[server_id] -= 1
        else:
            self.next_index[server_id] = message.last_log_index + 1
            self.match_index[server_id] = message.last_log_index + 1

    def handle_tick(self):
        self.timeout_ms -= 1
        if self.timeout_ms <= 0:
            self.handle_timeout()

    def handle_timeout(self):
        if self.role == "follower":
            self._set_candidate()
        if self.role == "candidate":
            self._set_candidate()
        elif self.role == "leader":  # todo: shouldn't happen
            self.set_timeout()

    def _set_candidate(self):
        self.role = "candidate"
        self.current_term += 1
        self.voted_for = self.server_id
        self.votes_received.add(self.server_id)
        self.set_timeout()
        self.broadcast_election()

    def _set_leader(self):
        self.role = "leader"
        self.voted_for = None
        last_log_index = len(self.log)
        self.next_index = [last_log_index + 1 for _ in self.servers]  # array[indexOfFollower <-> entries]
        self.match_index = [0 for _ in self.servers]  # array[indexReplicatedFollowers <-> entries]
        self.set_timeout()

    def _set_follower(self):
        self.role = "follower"
        self.voted_for = None
        self.set_timeout()

    def handle_msg(self, msg: Message, client_id):
        if self.current_term < msg.action.term and self.role != 'follower':
            self.current_term = msg.action.term
            self._set_follower()
            return

        if isinstance(msg.action, AppendEntriesRequest):
            if msg.action.term < self.current_term:
                return
            elif self.role != 'follower':
                self.current_term = msg.action.term
                self.leader_id = msg.action.leader_id
                self._set_follower()
            else:
                self.handle_append_entries(msg.action)

        elif isinstance(msg.action, AppendEntriesReply):
            self.handle_append_entries_reply(msg.action, client_id)

        elif isinstance(msg.action, RequestVoteRequest):
            if msg.action.term < self.current_term:
                return
            self.handle_request_vote(msg.action)

        elif isinstance(msg.action, RequestVoteReply):
            self.handle_request_vote_reply(msg.action, client_id)

        else:
            raise ValueError(f"Unknown request {msg}")

