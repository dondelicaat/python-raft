from unittest.mock import MagicMock

import pytest

from raft.log import LogEntry
from raft.raft_state_machine import Raft
from raft.rpc_calls import AppendEntriesRequest, Message, RequestVoteReply


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
