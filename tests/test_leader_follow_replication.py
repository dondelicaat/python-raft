import pytest

from queue import Queue
from unittest.mock import MagicMock

from raft.log import Log, OneIndexList, LogEntry
from raft.raft_state_machine import Raft
from raft.rpc_calls import Message


def get_log_entries(list):
    return [LogEntry(item) for item in list]


@pytest.mark.parametrize("leader_log_entries,follower_log_entries,expected",
    [
        ([LogEntry(1)], [], [LogEntry(1)]),
        ([LogEntry(1), LogEntry(2)], [], [LogEntry(1), LogEntry(2)]),
        ([LogEntry(1), LogEntry(2)], [LogEntry(1)], [LogEntry(1), LogEntry(2)]),
        ([LogEntry(1), LogEntry(2)], [LogEntry(2)], [LogEntry(1), LogEntry(2)]),
        ([LogEntry(1), LogEntry(2)], [LogEntry(2), LogEntry(3), LogEntry(6)], [LogEntry(1), LogEntry(2)]),
        (
            [LogEntry(1), LogEntry(1), LogEntry(1), LogEntry(4), LogEntry(4), LogEntry(5), LogEntry(5), LogEntry(6), LogEntry(6), LogEntry(6)],
            [LogEntry(1), LogEntry(1), LogEntry(1), LogEntry(4), LogEntry(4), LogEntry(5), LogEntry(5), LogEntry(6), LogEntry(6)],
            [LogEntry(1), LogEntry(1), LogEntry(1), LogEntry(4), LogEntry(4), LogEntry(5), LogEntry(5), LogEntry(6), LogEntry(6), LogEntry(6)],
        ),
        (
                [LogEntry(1), LogEntry(1), LogEntry(1), LogEntry(4), LogEntry(4), LogEntry(5),
                 LogEntry(5), LogEntry(6), LogEntry(6), LogEntry(6)],
                [LogEntry(1), LogEntry(1), LogEntry(1), LogEntry(4)],
                [LogEntry(1), LogEntry(1), LogEntry(1), LogEntry(4), LogEntry(4), LogEntry(5),
                 LogEntry(5), LogEntry(6), LogEntry(6), LogEntry(6)],
        ),
        (
                [LogEntry(1), LogEntry(1), LogEntry(1), LogEntry(4), LogEntry(4), LogEntry(5),
                 LogEntry(5), LogEntry(6), LogEntry(6), LogEntry(6)],
                [LogEntry(1), LogEntry(1), LogEntry(1), LogEntry(4)],
                [LogEntry(1), LogEntry(1), LogEntry(1), LogEntry(4), LogEntry(4), LogEntry(5),
                 LogEntry(5), LogEntry(6), LogEntry(6), LogEntry(6)],
        ),
        (
                [LogEntry(1), LogEntry(1), LogEntry(1), LogEntry(4), LogEntry(4), LogEntry(5),
                 LogEntry(5), LogEntry(6), LogEntry(6), LogEntry(6)],
                [LogEntry(1), LogEntry(1), LogEntry(1), LogEntry(4), LogEntry(4), LogEntry(5),
                 LogEntry(5), LogEntry(6), LogEntry(6), LogEntry(6), LogEntry(7), LogEntry(7)],
                [LogEntry(1), LogEntry(1), LogEntry(1), LogEntry(4), LogEntry(4), LogEntry(5),
                 LogEntry(5), LogEntry(6), LogEntry(6), LogEntry(6), LogEntry(7), LogEntry(7)],
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
def test_replay_leader_log_single_follower(leader_log_entries, follower_log_entries, expected):
    shared_queue = Queue()
    test_servers = ["test_server"]

    leader_log = Log(log_file=MagicMock())
    leader_log.logs = OneIndexList(leader_log_entries)
    leader = Raft(
        servers=test_servers,
        outbox=shared_queue,
        log=leader_log,
        role="leader",
    )
    leader.current_term = 1

    follower_log = Log(log_file=MagicMock())
    follower_log.logs = OneIndexList(follower_log_entries)
    follower = Raft(
        servers=test_servers,
        outbox=shared_queue,
        log=follower_log,
        role="follower",
    )
    follower.current_term = 0
    follower.server_id = 0

    while True:
        leader.handle_heartbeat()
        leader_action, leader_id = leader.outbox.get()
        # No more entries send so logs should be up to date.

        follower.handle_msg(Message(leader_action), leader_id)
        follower_action, client_id = follower.outbox.get()
        leader.handle_msg(Message(follower_action), client_id)

        if len(leader_action.entries) == 0 and follower_action.succes:
            break

    assert follower.log.logs == expected

