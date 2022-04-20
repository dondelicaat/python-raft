import pytest

from queue import Queue
from unittest.mock import MagicMock

from raft.log import Log, OneIndexList, LogEntry
from raft.raft_state_machine import Raft

def get_log_entries(list):
    return [LogEntry(item, "testcommand") for item in list]


@pytest.fixture
def backend_metadata_mock():
    backend = MagicMock()
    # Since only called on startup we can simply mock it here.
    backend.read.return_value = {'voted_for': None, 'current_term': 0}
    return backend


@pytest.mark.parametrize("leader_log_entries,follower_log_entries,expected",
    [
        ([], [], []),
        (get_log_entries([1]), [], get_log_entries([1])),
        (get_log_entries([1, 2]), [], get_log_entries([1, 2])),
        (get_log_entries([1, 2]), get_log_entries([1]), get_log_entries([1, 2])),
        (get_log_entries([1, 2]), get_log_entries([2]), get_log_entries([1, 2])),
        (get_log_entries([1, 2]), get_log_entries([2, 3, 6]), get_log_entries([1, 2])),
        (
            get_log_entries([1, 1, 1, 4, 4, 5, 5, 6, 6, 6]),
            get_log_entries([1, 1, 1, 4, 4, 5, 5, 6, 6]),
            get_log_entries([1, 1, 1, 4, 4, 5, 5, 6, 6, 6]),
        ),
        (
            get_log_entries([1, 1, 1, 4, 4, 5, 5, 6, 6, 6]),
            get_log_entries([1, 1, 1, 4]),
            get_log_entries([1, 1, 1, 4, 4, 5, 5, 6, 6, 6]),
        ),
        (
            get_log_entries([1, 1, 1, 4, 4, 5, 5, 6, 6, 6]),
            get_log_entries([1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7]),
            get_log_entries([1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7]),
        ),
        (
            get_log_entries([1, 1, 1, 4, 4, 5, 5, 6, 6, 6]),
            get_log_entries([1, 1, 1, 4, 4, 4, 4]),
            get_log_entries([1, 1, 1, 4, 4, 5, 5, 6, 6, 6]),
        ),
        (
            get_log_entries([1, 1, 1, 4, 4, 5, 5, 6, 6, 6]),
            get_log_entries([1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3]),
            get_log_entries([1, 1, 1, 4, 4, 5, 5, 6, 6, 6]),
        )
    ]
)
def test_replay_leader_log_single_follower(leader_log_entries, follower_log_entries, expected, backend_metadata_mock):
    shared_queue = Queue()
    test_servers = {0: ("test", "test"), 1: ("test", "test")}

    leader_log = Log(log_file=MagicMock())
    leader_log.logs = OneIndexList(leader_log_entries)
    leader = Raft(
        servers=test_servers,
        outbox=shared_queue,
        log=leader_log,
        metadata_backend=backend_metadata_mock,
        server_id=0,
        state_machine=MagicMock()
    )
    leader._set_leader()
    leader.current_term = 1
    leader.commit = MagicMock()

    follower_log = Log(log_file=MagicMock())
    follower_log.logs = OneIndexList(follower_log_entries)
    follower = Raft(
        servers=test_servers,
        outbox=shared_queue,
        log=follower_log,
        metadata_backend=backend_metadata_mock,
        server_id=1,
        state_machine=MagicMock(),
    )
    follower.current_term = 0

    while True:
        leader.handle_heartbeat()
        leader_msg = leader.outbox.get()

        follower.handle_msg(leader_msg)
        follower_msg = follower.outbox.get()
        leader.handle_msg(follower_msg)

        # No more entries send so and follower success status so log should be replicated.
        if len(leader_msg.action.entries) == 0 and follower_msg.action.succes:
            break

    assert follower.log.logs == expected


@pytest.mark.parametrize("leader_log_entries,follower_log_entries,initial_match_index,expected_match_index",
    [
        ([], [], {0: 0, 1: 0}, {0: 0, 1: 0})
    ]
)
def test_expected_match_index(
        leader_log_entries, follower_log_entries, backend_metadata_mock,
        initial_match_index, expected_match_index
):
    shared_queue = Queue()
    test_servers = {0: ("test", "test"), 1: ("test", "test")}

    leader_log = Log(log_file=MagicMock())
    leader_log.logs = OneIndexList(leader_log_entries)
    leader = Raft(
        servers=test_servers,
        outbox=shared_queue,
        log=leader_log,
        metadata_backend=backend_metadata_mock,
        server_id=0,
    )
    leader._set_leader()
    leader.current_term = 1
    leader.commit = MagicMock()
    leader.match_index = initial_match_index

    follower_log = Log(log_file=MagicMock())
    follower_log.logs = OneIndexList(follower_log_entries)
    follower = Raft(
        servers=test_servers,
        outbox=shared_queue,
        log=follower_log,
        metadata_backend=backend_metadata_mock,
        server_id=1,
        state_machine=MagicMock()
    )
    follower.current_term = 0

    leader.handle_heartbeat()
    leader_msg = leader.outbox.get()

    follower.handle_msg(leader_msg)
    follower_msg = follower.outbox.get()
    leader.handle_msg(follower_msg)

    assert leader.match_index == expected_match_index
