import logging
import math
from enum import Enum
from queue import Queue
from random import randint

from raft.log import Log, TermNotOk, LogNotCaughtUpException
from raft.metadata_backend import MetadataBackend
from raft.rpc_calls import AppendEntriesRequest, AppendEntriesReply, RequestVoteReply, \
    RequestVoteRequest, Message


logger = logging.getLogger(__name__)


class Role(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


class Raft:
    def __init__(
        self,
        servers: dict,
        server_id: int,
        outbox: Queue,
        log: Log,
        metadata_backend: MetadataBackend,
        timeout_provider=lambda: randint(5, 50),
    ):
        self.role = 'follower'
        self.servers = servers
        self.server_id = server_id
        self.outbox = outbox
        self.leader_id = None
        self.metadata_backend = metadata_backend
        self._voted_for = None
        self._current_term = None

        self.commit_index = 0
        self.last_applied = 0

        self.next_index = {}
        self.match_index = {}
        self.votes_received = set()

        self.client_messages_received = []

        self.log = log
        self.log.replay()

        self.timeout_ms = None
        self.heartbeat_interval = 10
        self.heartbeat_timeout_ms = None
        self.timeout_provider = timeout_provider
        self.set_timeout()

    def reset_heartbeat(self, timeout):
        self.heartbeat_timeout_ms = timeout

    @property
    def voted_for(self):
        if self._voted_for is None:
            state = self.metadata_backend.read()
            self._voted_for = state['voted_for']
        return self._voted_for

    @voted_for.setter
    def voted_for(self, value):
        self.metadata_backend.write({'voted_for': value, 'current_term': self.current_term})
        self._voted_for = value

    @property
    def current_term(self):
        if self._current_term is None:
            state = self.metadata_backend.read()
            self._current_term = state['current_term']
        return self._current_term

    @current_term.setter
    def current_term(self, value):
        self.metadata_backend.write({'voted_for': self.voted_for, 'current_term': value})
        self._current_term = value

    def set_timeout(self):
        self.timeout_ms = self.timeout_provider()

    def handle_heartbeat(self):
        if self.role != "leader":
            return

        last_log_index = len(self.log)
        for server_id, _ in self.servers.items():
            if server_id == self.server_id:
                continue

            server_next_idx = self.next_index[server_id]

            entries = []
            if server_next_idx <= last_log_index:
                entries = self.log[server_next_idx:].to_list()

            if server_next_idx > 1:
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
            self.outbox.put(Message(append_entries_request, sender=self.server_id, receiver=server_id))

        self.reset_heartbeat(self.heartbeat_interval)

    def broadcast_election(self):
        assert self.role == 'candidate'

        for server_id, _ in self.servers.items():
            if server_id == self.server_id:
                continue

            last_log_index = len(self.log)
            if last_log_index > 0:
                last_log_term = self.log[last_log_index]
            else:
                last_log_term = self.current_term
            request_vote_request = RequestVoteRequest(
                term=self.current_term, candidate_id=self.server_id,
                last_log_term=last_log_term, last_log_index=last_log_index
            )
            self.outbox.put(Message(request_vote_request, sender=self.server_id, receiver=server_id))

    def handle_append_entries(self, message: AppendEntriesRequest, receiver_id):
        assert self.role == "follower"
        self.set_timeout()

        try:
            self.log.append_entries(
                prev_log_index=message.prev_log_index,
                prev_log_term=message.prev_log_term,
                entries=message.entries,
            )
        except (TermNotOk, LogNotCaughtUpException):
            self.outbox.put(Message(
                AppendEntriesReply(self.current_term, False, -1, entries_added=0)
                , sender=self.server_id, receiver=receiver_id))
            return

        if message.leader_commit > self.commit_index:
            self.commit_index = min(message.leader_commit, len(self.log))

        last_log_index = len(self.log)
        self.outbox.put(
            Message(
                AppendEntriesReply(self.current_term, True, last_log_index, entries_added=len(message.entries)),
                sender=self.server_id,
                receiver=receiver_id
            ))

    def handle_request_vote(self, message: RequestVoteRequest, receiver_id):
        logger.info(
            f"Handle request vote; voted_for {self.voted_for}, server_id: {self.server_id}, "
            f"log len: {len(self.log)}  candidate id {message.candidate_id}, "
            f"candidate message last log {message.last_log_index}"
        )

        if (
            (self.voted_for is None or self.voted_for == message.candidate_id)
            and message.last_log_index >= len(self.log)
        ):
            logger.info(f"Vote granted to {message.candidate_id}!")
            self.voted_for = message.candidate_id
            self.set_timeout()
            self.outbox.put(Message(RequestVoteReply(self.current_term, True), sender=self.server_id, receiver=receiver_id))

        logger.info(f"Vote not granted to {message.candidate_id}!")
        self.outbox.put(Message(RequestVoteReply(self.current_term, False), sender=self.server_id, receiver=receiver_id))

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
        elif not message.entries_added:
            self.next_index[server_id] = message.last_log_index
            self.match_index[server_id] = message.last_log_index
        else:
            self.next_index[server_id] = message.last_log_index + 1
            self.match_index[server_id] = message.last_log_index + 1

        self.commit()

    def commit(self):
        logger.info(f"match_index: {self.match_index}, commit index: {self.commit_index}, logs: {self.log.logs}, current term: {self.current_term}")
        assert len(self.match_index) > 2 and len(self.match_index) % 2 == 1

        match_indices = sorted([index for index in self.match_index.values()])
        largest_majority_index = match_indices[math.floor(len(self.match_index) / 2)]
        for idx in range(self.commit_index, largest_majority_index + 1):
            if idx == 0:
                continue
            if self.log[idx].term == self.current_term:
                self.commit_index = idx

    def handle_tick(self):
        self.timeout_ms -= 1
        self.heartbeat_interval -= 1
        if self.heartbeat_interval <= 0:
            self.handle_heartbeat()
        if self.timeout_ms <= 0:
            self.handle_timeout()

    def handle_timeout(self):
        logger.info("Handle timeout called")
        if self.role == "follower":
            self._set_candidate()
        elif self.role == "candidate":
            self._set_candidate()
        elif self.role == "leader":  # todo: shouldn't happen
            self.set_timeout()

    def _set_candidate(self):
        logger.info(f"set candidate called by {self.server_id} with {self.current_term}")
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
        self.next_index = {idx: last_log_index + 1 for idx in self.servers}
        self.match_index = {idx: 0 for idx in self.servers}
        self.votes_received = set()
        self.set_timeout()
        self.reset_heartbeat(timeout=0)

    def _set_follower(self):
        logger.info("Set role to follower")
        self.role = "follower"
        self.voted_for = None
        self.votes_received = set()
        self.set_timeout()

    def handle_msg(self, msg: Message):
        if self.current_term < msg.action.term:
            self.current_term = msg.action.term
            self.voted_for = None
            if self.role != 'follower':
                self._set_follower()

        if isinstance(msg.action, AppendEntriesRequest):
            if msg.action.term < self.current_term:
                return
            elif self.role != 'follower':
                self.current_term = msg.action.term
                self.leader_id = msg.action.leader_id
                self._set_follower()
            else:
                self.handle_append_entries(msg.action, receiver_id=msg.sender)

        elif isinstance(msg.action, AppendEntriesReply):
            self.handle_append_entries_reply(msg.action, msg.sender)

        elif isinstance(msg.action, RequestVoteRequest):
            if msg.action.term < self.current_term:
                return
            self.handle_request_vote(msg.action, receiver_id=msg.sender)

        elif isinstance(msg.action, RequestVoteReply):
            self.handle_request_vote_reply(msg.action, client_id=msg.sender)

        else:
            raise ValueError(f"Unknown request {msg}")

