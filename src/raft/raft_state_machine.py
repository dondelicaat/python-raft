from enum import Enum
from queue import Queue
from random import randint

from raft.log import Log, TermNotOk, LogNotCaughtUpException
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
        outbox: Queue,
        log: Log,
        role="follower",
        timeout_provider=lambda: randint(150, 300),
    ):
        self.role = role
        self.servers = servers
        self.num_servers = len(servers) + 1  # counting for itself.
        self.outbox = outbox
        self.log = log
        self.leader_id = None
        self.server_id = None

        self.votes_received = 0
        self.voted_for = None  # todo, some id?
        self.current_term = 0  # Fetch from persisted state

        self.request_timeout_ms = 50
        self.timeout_ms = None
        self.timeout_provider = timeout_provider
        self.set_timeout()

        self.commit_index = 0
        self.last_applied = 0

        self.next_index = []
        self.match_index = []
        self.num_entries_added = [0 for _ in servers]

        # self.log.replay()
        if role == "leader":
            last_log_index = len(self.log)
            self.next_index = [last_log_index + 1 for _ in servers]  # array[indexOfFollower <-> entries]
            self.match_index = [0 for _ in servers]  # array[indexReplicatedFollowers <-> entries]

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
                entries = self.log[server_next_idx:]

            self.num_entries_added[server_id] = len(entries)

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
            self.outbox.put((append_entries_request, self.leader_id))

    def handle_append_entries(self, message: AppendEntriesRequest):
        assert self.role == "follower"
        self.set_timeout()

        try:
            self.log.append_entries(
                prev_log_index=message.prev_log_index,
                prev_log_term=message.prev_log_term,
                entries=message.entries,
            )

            if message.leader_commit > self.commit_index:
                self.commit_index = min(message.leader_commit, len(self.log))
            success = True

        except (TermNotOk, LogNotCaughtUpException):
            success = False

        self.outbox.put((AppendEntriesReply(self.current_term, success), self.server_id))

    def handle_request_vote(self, message: RequestVoteRequest) -> RequestVoteReply:
        if (
            self.voted_for is None or self.voted_for == message.candidate_id
            and message.last_log_index >= len(self.log)
        ):  # should term also be the same? # see page 8 4.2.1 last paragraph
            self.voted_for = message.candidate_id
            self.set_timeout()
            return RequestVoteReply(self.current_term, True)

        return RequestVoteReply(self.current_term, False)

    def handle_request_vote_reply(self, message: RequestVoteReply):
        raise NotImplemented()

    def handle_append_entries_reply(self, message: AppendEntriesReply, server_id):
        if not message.succes:
            self.next_index[server_id] -= 1
        else:
            self.next_index[server_id] += self.num_entries_added[server_id]
            self.match_index[server_id] += self.num_entries_added[server_id]

    def handle_tick(self):
        self.timeout_ms -= 1
        if self.timeout_ms <= 0:
            self.handle_timeout()

    def handle_timeout(self):
        if self.role == "follower":
            self._set_candidate()
        if self.role == "candidate":
            # if num_pc - no_response = majjority and timeout
            self._set_candidate()
        elif self.role == "leader":
            self.set_timeout()

    def handle_vote(self):
        self.votes_received += 1
        # Turn into a set of votes => handles idempotent votes.
        if self.received_majority_vote() and self.role == "candidate":
            self._set_leader()

    def received_majority_vote(self):
        return self.votes_received / self.num_servers > 0.5

    def _set_candidate(self):
        self.role = "candidate"
        self.votes_received = 1
        self.current_term += 1
        self.voted_for = None  # Todo: set to self.id
        # todo: self.broadcast_election()
        self.set_timeout()

    def _set_leader(self):
        self.role = "leader"
        self.votes_received = 0
        self.voted_for = None
        last_log_index = len(self.log)
        self.next_index = [last_log_index + 1 for _ in self.servers]  # array[indexOfFollower <-> entries]
        self.match_index = [0 for _ in self.servers]  # array[indexReplicatedFollowers <-> entries]
        self.set_timeout()

    def _set_follower(self):
        self.role = "follower"
        self.votes_received = 0
        self.voted_for = None
        self.set_timeout()

    def handle_msg(self, msg: Message, client_id):
        if self.current_term < msg.action.term:
            self._set_follower()
            return

        if isinstance(msg.action, AppendEntriesRequest):
            if self.role != 'follower':
                self.current_term = msg.action.term
                self.leader_id = msg.action.leader_id
                self._set_follower()
            else:
                self.handle_append_entries(msg.action)

        elif isinstance(msg.action, AppendEntriesReply):
            self.handle_append_entries_reply(msg.action, client_id)

        elif isinstance(msg.action, RequestVoteRequest):
            self.handle_request_vote(msg.action)

        elif isinstance(msg.action, RequestVoteReply):
            self.handle_request_vote_reply(msg.action)

        else:
            raise ValueError(f"Unknown request {msg}")

