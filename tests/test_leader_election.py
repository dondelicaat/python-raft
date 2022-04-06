from queue import Queue
from unittest.mock import MagicMock

import pytest

from raft.log import LogEntry, Log, OneIndexList
from raft.raft_state_machine import Raft
from raft.rpc_calls import AppendEntriesRequest, Message, RequestVoteReply, RequestVoteRequest
from tests.test_leader_follow_replication import get_log_entries


@pytest.fixture
def backend_metadata_mock():
    backend = MagicMock()
    # Since only called on startup we can simply mock it here.
    backend.read.return_value = {'voted_for': None, 'current_term': 0}
    return backend


@pytest.mark.parametrize("time_difference,initial_role,expected_role", [
    (-1, "follower", "candidate"),
    (0, "follower", "candidate"),
    (1, "follower", "follower"),
    (-1, "candidate", "candidate"),
    (0, "candidate", "candidate"),
    (1, "candidate", "candidate"),
    (-1, "leader", "leader"),
    (0, "leader", "leader"),
    (1, "leader", "leader"),
])
def test_timeout(time_difference, initial_role, expected_role, backend_metadata_mock):
    timeout = 150

    raft = Raft(
        servers={},
        server_id=0,
        outbox=MagicMock(),
        metadata_backend=backend_metadata_mock,
        timeout_provider=lambda: timeout + time_difference,
        log=MagicMock(),
    )

    if initial_role == 'candidate':
        raft._set_candidate()
    elif initial_role == 'leader':
        raft._set_leader()

    for i in range(timeout):
        raft.handle_tick()

    assert raft.role == expected_role


@pytest.mark.parametrize("number_of_machines,number_of_votes,initial_role,expected_role", [
    (5, 1, "follower", "follower"),
    (5, 3, "follower", "follower"),
    (5, 2, "candidate", "candidate"),
    (5, 3, "candidate", "leader"),
])
def test_votes(number_of_machines, number_of_votes, initial_role, expected_role, backend_metadata_mock):
    candidate_id = 1
    test_servers = {idx: ("test", "test") for idx in range(number_of_machines)}
    raft = Raft(
        servers=test_servers,
        server_id=candidate_id,
        outbox=MagicMock(),
        metadata_backend=backend_metadata_mock,
        log=MagicMock(),
    )
    raft.role = initial_role
    raft.current_term = 1
    raft.voted_for = candidate_id
    request_vote_reply = RequestVoteReply(0, True)

    for i in range(number_of_votes):
        raft.handle_msg(Message(request_vote_reply, sender=i, receiver=candidate_id), 1)

    assert raft.role == expected_role



@pytest.mark.parametrize("number_of_machines,number_of_votes,initial_role,expected_role", [
    (5, 1, "follower", "follower"),
    (5, 3, "follower", "follower"),
    (5, 2, "candidate", "candidate"),
    (5, 3, "candidate", "leader"),
])
def test_votes(number_of_machines, number_of_votes, initial_role, expected_role, backend_metadata_mock):
    candidate_id = 0
    test_servers = {idx: ("test", "test") for idx in range(number_of_machines)}
    raft = Raft(
        servers=test_servers,
        server_id=candidate_id,
        outbox=MagicMock(),
        metadata_backend=backend_metadata_mock,
        log=MagicMock(),
    )
    raft.role = initial_role
    raft.current_term = 1
    raft.voted_for = candidate_id
    request_vote_reply = RequestVoteReply(0, True)

    for i in range(number_of_votes):
        raft.handle_msg(Message(request_vote_reply, sender=i, receiver=candidate_id))

    assert raft.role == expected_role


@pytest.mark.parametrize("number_of_machines,number_of_votes,initial_role,expected_role", [
    (5, 1, "follower", "follower"),
])
def test_start_election(number_of_machines, number_of_votes, initial_role, expected_role, backend_metadata_mock):
    shared_queue = Queue()
    candidate_log = Log(log_file=MagicMock())
    candidate_log.logs = OneIndexList(get_log_entries([1]))
    test_servers = {idx: ("test", "test") for idx in range(number_of_machines)}
    raft_candidate = Raft(
        servers=test_servers,
        server_id=0,
        outbox=shared_queue,
        metadata_backend=backend_metadata_mock,
        log=candidate_log,
    )
    raft_candidate.current_term = 1
    raft_candidate._set_candidate()

    for server_id in [1, 2, 3, 4]:
        raft_follower = Raft(
            servers=test_servers,
            server_id=server_id,
            outbox=shared_queue,
            metadata_backend=backend_metadata_mock,
            log=MagicMock(),
        )
        raft_follower.role = 'follower'
        request_vote = shared_queue.get()
        raft_follower.handle_msg(request_vote)

    for _ in [1, 2, 3, 4]:
        request_vote_reply = shared_queue.get()
        raft_candidate.handle_msg(request_vote_reply)

    assert raft_candidate.role == 'leader'


@pytest.mark.parametrize("current_term,sender_term,initial_role,expected_role", [
    (0, 0, "follower", "follower"),
    (5, 6, "candidate", "follower"),
    (5, 6, "leader", "follower"),
    (5, 4, 'leader', 'leader'),
])
def test_handle_append_entries_request(current_term, sender_term, initial_role, expected_role, backend_metadata_mock):
    test_servers = {idx: ("test", "test") for idx in range(2)}
    raft = Raft(
        server_id=0,
        servers=test_servers,
        metadata_backend=backend_metadata_mock,
        outbox=MagicMock(),
        log=MagicMock(),
    )
    raft.role = initial_role
    raft.current_term = current_term

    append_entries_request = AppendEntriesRequest(
        term=sender_term,
        leader_id=1,
        prev_log_index=123,
        prev_log_term=2,
        leader_commit=1,
        entries=[LogEntry(3)]
    )
    raft.handle_msg(Message(append_entries_request, sender=1, receiver=0))

    assert raft.role == expected_role


@pytest.mark.parametrize("follower_log_entries,follower_term,follower_voted_for,candidate_term,candidate_last_log_index,candidate_last_log_term,expected",
    [
        ([LogEntry(1)], 1, None, 1, 1, 1, True),
        ([LogEntry(1)], 1, 1, 1, 1, 1, True),
        ([LogEntry(1)], 1, 2, 1, 1, 1, False),
        ([LogEntry(1), LogEntry(1)], 1, 1, 1, 2, 1, True),  # candidate.last_log_indedx == len(follower.logs)
        ([LogEntry(1), LogEntry(1), LogEntry(1)], 1, 1, 1, 2, 1, False), # candidate.last_log_indedx < len(follower.logs)
    ]
)
def test_granting_vote(follower_log_entries, follower_term, follower_voted_for, candidate_term,
                       candidate_last_log_index, candidate_last_log_term, expected,
                       backend_metadata_mock):
    test_servers = {idx: ("test", "test") for idx in range(2)}
    follower_log = Log(log_file=MagicMock())
    follower_log.logs = OneIndexList(follower_log_entries)
    shared_queue = Queue()
    raft_follower = Raft(
        server_id=0,
        servers=test_servers,
        metadata_backend=backend_metadata_mock,
        outbox=shared_queue,
        log=follower_log,
    )
    raft_follower.current_term = follower_term
    raft_follower.voted_for = follower_voted_for

    request_vote_request = RequestVoteRequest(
        candidate_id=1, last_log_index=candidate_last_log_index,
        last_log_term=candidate_last_log_term, term=candidate_term
    )

    raft_follower.handle_msg(Message(request_vote_request, sender=1, receiver=0))

    msg = shared_queue.get()
    assert msg.action.vote_granted == expected



@pytest.mark.parametrize("follower_log_entries,follower_term,follower_voted_for,candidate_term,candidate_last_log_index,candidate_last_log_term",
    [
        ([LogEntry(1)], 2, None, 1, 1, 1),  # Follow.current_term > candidate.term
    ]
)
def test_message_dropped(follower_log_entries, follower_term, follower_voted_for, candidate_term,
                         candidate_last_log_index, candidate_last_log_term, backend_metadata_mock):
    test_servers = {idx: ("test", "test") for idx in range(2)}
    follower_log = Log(log_file=MagicMock())
    follower_log.logs = OneIndexList(follower_log_entries)
    shared_queue = Queue()
    raft_follower = Raft(
        server_id=0,
        servers=test_servers,
        metadata_backend=backend_metadata_mock,
        outbox=shared_queue,
        log=follower_log,
    )
    raft_follower.current_term = follower_term
    raft_follower.voted_for = follower_voted_for

    request_vote_request = RequestVoteRequest(
        candidate_id=1, last_log_index=candidate_last_log_index,
        last_log_term=candidate_last_log_term, term=candidate_term
    )

    raft_follower.handle_msg(Message(request_vote_request, sender=1, receiver=0))
    assert shared_queue.empty()
