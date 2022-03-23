from copy import copy

from raft.log import Log, LogEntry, TermNotOk
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


def test_same_log_missing_one_entry(leader_log_fixture):
    follower_logs = [
        LogEntry(1),
        LogEntry(1),
        LogEntry(1),
        LogEntry(4),
    ]

    for missing_index in range(1, 4):
        follower_log = Log()
        follower_log.logs = copy(follower_logs)

        prev_log_index = missing_index - 1
        prev_entry = leader_log_fixture.get_log_entry(missing_index - 1)
        prev_log_term = prev_entry.term

        entries = leader_log_fixture.get_log_entries(missing_index)

        follower_log.append_entries(prev_log_index, prev_log_term, entries)

        assert follower_log.logs == leader_log_fixture.logs

    missing_index = 6
    with pytest.raises(IndexError):
        follower_log = Log()
        follower_log.logs = copy(follower_logs)

        prev_log_index = missing_index - 1
        prev_entry = leader_log_fixture.get_log_entry(missing_index - 1)
        prev_log_term = prev_entry.term

        entries = leader_log_fixture.get_log_entries(missing_index)
        print(entries)

        follower_log.append_entries(prev_log_index, prev_log_term, entries)


def test_wrong_terms_log(leader_log_fixture):
    follower_logs = [
        LogEntry(1),
        LogEntry(1),
        LogEntry(1),
        LogEntry(2),
        LogEntry(2),
        LogEntry(2),
        LogEntry(3),
        LogEntry(3),
        LogEntry(3),
        LogEntry(3),
        LogEntry(3),
    ]

    for missing_index in range(1, 5):
        follower_log = Log()
        follower_log.logs = copy(follower_logs)

        prev_log_index = missing_index - 1
        prev_entry = leader_log_fixture.get_log_entry(missing_index - 1)
        prev_log_term = prev_entry.term

        entries = leader_log_fixture.get_log_entries(missing_index)
        print(entries)

        follower_log.append_entries(prev_log_index, prev_log_term, entries)
        print(missing_index)
        assert follower_log.logs == leader_log_fixture.logs


    missing_index = 6
    with pytest.raises(TermNotOk):
        follower_log = Log()
        follower_log.logs = copy(follower_logs)

        prev_log_index = missing_index - 1
        prev_entry = leader_log_fixture.get_log_entry(missing_index - 1)
        prev_log_term = prev_entry.term

        entries = leader_log_fixture.get_log_entries(missing_index)
        print(entries)

        follower_log.append_entries(prev_log_index, prev_log_term, entries)
