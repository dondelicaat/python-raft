from queue import Queue
from unittest.mock import MagicMock

from raft.log import LogEntry, Log, OneIndexList
from raft.raft_state_machine import Raft
import pytest


@pytest.mark.parametrize("leader_log_entries,match_index,leader_term,expected_commit_index",
    [
        ([LogEntry(1), LogEntry(2), LogEntry(2)], {0: 3, 1: 3, 2: 3}, 2, 3),
        ([LogEntry(1), LogEntry(2), LogEntry(2)], {0: 1, 1: 1, 2: 3}, 1, 1),
    ]
)
def test_raft_leader_commit(leader_log_entries, match_index, leader_term, expected_commit_index):
    leader_log = Log(log_file=MagicMock())
    leader_log.logs = OneIndexList(leader_log_entries)
    leader = Raft(
        servers=MagicMock(),
        outbox=MagicMock(),
        log=leader_log,
        metadata_backend=MagicMock(),
        server_id=0,
    )
    leader.current_term = leader_term
    leader.commit_index = 0
    leader.match_index = match_index
    leader.commit()
    assert leader.commit_index == expected_commit_index

