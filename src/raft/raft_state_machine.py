from queue import Queue
from random import randint

from raft.log import Log, TermNotOk
from raft.rpc_calls import AppendEntriesRequest, AppendEntriesReply, RequestVoteReply, \
    RequestVoteRequest, Message


class Raft:
    def __init__(
        self,
        servers,
        inbox: Queue,
        outbox: Queue,
        log: Log,
        role="follower",
        current_term=0,
        timeout_provider=lambda: randint(150, 300),
    ):
        self.role = role
        self.servers = servers
        self.num_servers = len(servers) + 1  # counting for itself.
        self.outbox = outbox
        self.inbox = inbox
        self.log = log
        self.leader_id = None

        self.votes_received = 0
        self.voted_for = None  # todo, some id?
        self.current_term = current_term

        self.request_timeout_ms = 50
        self.timeout_ms = None
        self.timeout_provider = timeout_provider
        self.set_timeout()

        self.commit_index = 0
        self.last_applied = 0

        self.next_index = []
        self.match_index = []

        # self.log.replay()
        # nextIndex = array[indexOfFollower <-> entries]
        # matchIndex = array[indexReplicatedFollowers <-> entries]

    def set_timeout(self):
        self.timeout_ms = self.timeout_provider()

    def handle_append_entries(self, message: AppendEntriesRequest) -> AppendEntriesReply:
        if message.term < self.current_term:
            return AppendEntriesReply(self.current_term, False)

        self.set_timeout()

        assert self.role == "follower"

        try:
            self.log.append_entries(
                prev_log_index=message.prev_log_index,
                prev_log_term=message.prev_log_term,
                entries=message.entries,
            )

            if message.leader_commit > self.commit_index:
                self.commit_index = min(message.leader_commit, len(self.log))

            return AppendEntriesReply(self.current_term, True)
        except (TermNotOk, IndexError):
            return AppendEntriesReply(self.current_term, False)

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
        pass

    def handle_append_entries_reply(self, message: AppendEntriesReply):
        pass

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
        self.set_timeout()

    def _set_follower(self):
        self.role = "follower"
        self.votes_received = 0
        self.voted_for = None
        self.set_timeout()

    def handle_msg(self, msg: Message):
        if msg.action.term < self.current_term:
            return None

        elif msg.action.term >= self.current_term:
            self.current_term = msg.action.term
            self.leader_id = msg.action.leader_id
            self._set_follower()
            return AppendEntriesReply(self.current_term, False)  # todo: is this correct?

        if isinstance(msg.action, AppendEntriesRequest):
            reply = self.handle_append_entries(msg.action)
        elif isinstance(msg.action, RequestVoteRequest):
            reply = self.handle_request_vote(msg.action)
        else:
            raise ValueError(f"Unknown request {msg}")

        return Message.from_bytes(bytes(reply))
    #
    # def broadcast_election(self):
    #     request_vote_request = RequestVoteRequest(
    #         term=self.current_term,
    #         candidate_id=..., #todo: self.id,
    #         last_log_index=self.last_applied,
    #         last_log_term=self.log.get_log_entry(self.last_applied).term,
    #     )
    #     for server in self.servers:
    #         self.outbox.put(server.id, request_vote_request)
    #
    # def broadcast_messages(self, messages: List[LogEntry]):
    #     append_entries_request = AppendEntriesRequest(
    #         term=self.current_term,
    #         leader_id=..., #todo: self.id
    #         prev_log_index=...,
    #         prev_log_term=...,
    #         entries=messages,
    #         leader_commit=...,
    #     )
    #
    #     for server in self.servers:
    #         self.outbox.put(server.id, append_entries_request)
    #
    # def handle_client_request(self, msg: Command, client_id: str):
    #     if self.role == "leader":
    #         ... # handle_add_msg leader rules
    #         self.outbox.put((client_id, msg))
    #     if self.role == "follower":
    #         # Redirects the msg to the leader.
    #         self.outbox.put((self.leader_id, msg))
