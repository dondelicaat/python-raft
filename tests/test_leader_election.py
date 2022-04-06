from queue import Queue
from unittest.mock import MagicMock

import pytest

from raft.log import LogEntry, Log, OneIndexList
from raft.raft_state_machine import Raft
from raft.rpc_calls import AppendEntriesRequest, Message, RequestVoteReply
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
        role=initial_role,
        servers=[],
        server_id=0,
        outbox=MagicMock(),
        metadata_backend=backend_metadata_mock,
        timeout_provider=lambda: timeout + time_difference,
        log=MagicMock(),
    )

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
    raft = Raft(
        servers=[_ for _ in range(number_of_machines)],
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
        raft.handle_msg(Message(request_vote_reply), i)

    assert raft.role == expected_role



@pytest.mark.parametrize("number_of_machines,number_of_votes,initial_role,expected_role", [
    (5, 1, "follower", "follower"),
    (5, 3, "follower", "follower"),
    (5, 2, "candidate", "candidate"),
    (5, 3, "candidate", "leader"),
])
def test_votes(number_of_machines, number_of_votes, initial_role, expected_role, backend_metadata_mock):
    candidate_id = 1
    raft = Raft(
        servers=[_ for _ in range(number_of_machines)],
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
        raft.handle_msg(Message(request_vote_reply), i)

    assert raft.role == expected_role


@pytest.mark.parametrize("number_of_machines,number_of_votes,initial_role,expected_role", [
    (5, 1, "follower", "follower"),
])
def test_start_election(number_of_machines, number_of_votes, initial_role, expected_role, backend_metadata_mock):
    shared_queue = Queue()
    candidate_log = Log(log_file=MagicMock())
    candidate_log.logs = OneIndexList(get_log_entries([1]))
    raft_candidate = Raft(
        servers=[_ for _ in range(number_of_machines)],
        server_id=0,
        outbox=shared_queue,
        metadata_backend=backend_metadata_mock,
        log=candidate_log,
    )
    raft_candidate.current_term = 1
    raft_candidate._set_candidate()

    for server_id in [1, 2, 3, 4]:
        raft_follower = Raft(
            servers=[_ for _ in range(number_of_machines)],
            server_id=server_id,
            outbox=shared_queue,
            metadata_backend=backend_metadata_mock,
            log=MagicMock(),
        )
        raft_follower.role = 'follower'
        request_vote, client = shared_queue.get()
        raft_follower.handle_msg(Message(request_vote), client_id=client)

    for _ in [1, 2, 3, 4]:
        request_vote_reply, client = shared_queue.get()
        raft_candidate.handle_msg(Message(request_vote_reply), client_id=client)

    assert raft_candidate.role == 'leader'


@pytest.mark.parametrize("current_term,sender_term,initial_role,expected_role", [
    (0, 0, "follower", "follower"),
    (5, 6, "candidate", "follower"),
    (5, 6, "leader", "follower"),
    (5, 4, 'leader', 'leader'),
])
def test_handle_append_entries_request(current_term, sender_term, initial_role, expected_role, backend_metadata_mock):
    raft = Raft(
        server_id=1,
        servers=[_ for _ in range(5)],
        metadata_backend=backend_metadata_mock,
        outbox=MagicMock(),
        log=MagicMock(),
    )
    raft.role = initial_role
    raft.current_term = current_term

    append_entries_request = AppendEntriesRequest(
        term=sender_term,
        leader_id="testleader",
        prev_log_index=123,
        prev_log_term=2,
        leader_commit=1,
        entries=[LogEntry(3)]
    )
    raft.handle_msg(Message(append_entries_request), client_id=2)

    assert raft.role == expected_role
