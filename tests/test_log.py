from raft.log import Log, LogEntry
import pytest


@pytest.fixture
def leader_log_fixture():
    leader_log = [
        LogEntry(1),
        LogEntry(1),
        LogEntry(1),
        LogEntry(4),
        LogEntry(4),
        LogEntry(5),
        LogEntry(5),
        LogEntry(6),
        LogEntry(6),
        LogEntry(6),
    ]
    log = Log()
    log.logs = leader_log
    return log


def test_same_log_missing_one_entry(leader_log_fixture):
    follower_logs = [
        LogEntry(1),
        LogEntry(1),
        LogEntry(1),
        LogEntry(4),
        LogEntry(4),
        LogEntry(5),
        LogEntry(5),
        LogEntry(6),
        LogEntry(6),
    ]

    leader_log_index = 10
    latest_entry = leader_log_fixture.get_log_entry(leader_log_index)
    prev_log_index = 9
    prev_entry = leader_log_fixture.get_log_entry(prev_log_index)
    prev_log_term = prev_entry.term

    follower_log = Log()
    follower_log.logs = follower_logs
    follower_log.append_entries(prev_log_index, prev_log_term, [latest_entry])

    assert follower_log.logs == leader_log_fixture.logs
