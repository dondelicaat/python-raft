import pytest

from queue import Queue
from unittest.mock import MagicMock

from raft.log import Log, OneIndexList, LogEntry
from raft.raft_state_machine import Raft
from raft.rpc_calls import Message


@pytest.mark.parametrize("leader_log_entries,follower_log_entries,expected",
    [
        # ([LogEntry(1)], [], [LogEntry(1)]),
        ([LogEntry(1), LogEntry(2)], [], [LogEntry(1), LogEntry(2)]),
        # (0, 0, [], [LogEntry(1), LogEntry(2), LogEntry(2)], [LogEntry(1), LogEntry(2), LogEntry(2)]),
        # (2, 2, [LogEntry(1), LogEntry(2)], [LogEntry(3), LogEntry(3), LogEntry(5)],
        # [LogEntry(1), LogEntry(2), LogEntry(3), LogEntry(3), LogEntry(5)]),
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
        action, leader_id = leader.outbox.get()
        follower.handle_msg(Message(action), leader_id)
        action, client_id = follower.outbox.get()
        leader.handle_msg(Message(action), client_id)

    # assert follower.log.logs == expected

