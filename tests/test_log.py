from unittest.mock import MagicMock

from raft.log import Log, LogEntry, TermNotOk, OneIndexList
import pytest


@pytest.fixture(autouse=True)
def log_fixture():
    log = MagicMock()
    log = Log(log)
    log.logs = OneIndexList([])
    log.write_to_log = MagicMock()
    yield log


def test_index_error(log_fixture):
    print(log_fixture)
    print(log_fixture.logs)
    pass


def test_term_error():
    pass


@pytest.mark.parametrize("prev_log_term,prev_log_index,initial_log,entries,expected",
    [
        (0, 0, [LogEntry(1)], [LogEntry(2)], [LogEntry(2)]),
        # (0, 0, [], [LogEntry(1), LogEntry(2), LogEntry(2)], [LogEntry(1), LogEntry(2), LogEntry(2)]),
        # (2, 2, [LogEntry(1), LogEntry(2)], [LogEntry(3), LogEntry(3), LogEntry(5)],
        # [LogEntry(1), LogEntry(2), LogEntry(3), LogEntry(3), LogEntry(5)]),
    ]
)
def test_overwrite_and_truncate(log_fixture, prev_log_term, prev_log_index, initial_log, entries, expected):
    log_fixture.logs = OneIndexList(initial_log)
    log_fixture.append_entries(prev_log_term=prev_log_term, prev_log_index=prev_log_index,
                               entries=entries)
    assert log_fixture.logs == expected


@pytest.mark.parametrize("prev_log_term,prev_log_index,initial_log,entries,expected",
    [
        (0, 0, [], [LogEntry(1)], [LogEntry(1)]),
        (0, 0, [], [LogEntry(1), LogEntry(2), LogEntry(2)], [LogEntry(1), LogEntry(2), LogEntry(2)]),
        (2, 2, [LogEntry(1), LogEntry(2)], [LogEntry(3), LogEntry(3), LogEntry(5)],
        [LogEntry(1), LogEntry(2), LogEntry(3), LogEntry(3), LogEntry(5)]),
    ]
)
def test_append(log_fixture, prev_log_term, prev_log_index, initial_log, entries, expected):
    log_fixture.logs = OneIndexList(initial_log)
    log_fixture.append_entries(prev_log_term=prev_log_term, prev_log_index=prev_log_index, entries=entries)
    assert log_fixture.logs == expected


#
# def test_same_log_missing_one_entry(leader_log_fixture):
#     follower_logs = OneIndexList([
#         LogEntry(1),
#         LogEntry(1),
#         LogEntry(1),
#         LogEntry(4),
#         LogEntry(4),
#         LogEntry(5),
#         LogEntry(5),
#         LogEntry(6),
#         LogEntry(6),
#     ])
#
#     leader_log_index = 10
#     latest_entry = leader_log_fixture[leader_log_index]
#     prev_log_index = 9
#     prev_entry = leader_log_fixture[prev_log_index]
#     prev_log_term = prev_entry.term
#     print(f"previous log term{prev_log_term} and index {prev_log_index}")
#
#     follower_log = Log()
#     follower_log.logs = follower_logs
#     follower_log.append_entries(prev_log_index, prev_log_term, [latest_entry])
#
#     assert follower_log.logs == leader_log_fixture.logs
#
#
# def test_same_log_missing2_one_entry(leader_log_fixture):
#     follower_logs = OneIndexList([
#         LogEntry(1),
#         LogEntry(1),
#         LogEntry(1),
#         LogEntry(4),
#     ])
#
#     for missing_index in range(1, 4):
#         follower_log = Log()
#         follower_log.logs = copy(follower_logs)
#
#         prev_log_index = missing_index - 1
#         prev_entry = leader_log_fixture[missing_index]
#         prev_log_term = prev_entry.term
#
#         entries = leader_log_fixture[missing_index:]
#
#         follower_log.append_entries(prev_log_index, prev_log_term, entries)
#
#         assert follower_log.logs == leader_log_fixture.logs
#
#     missing_index = 6
#     with pytest.raises(IndexError):
#         follower_log = Log()
#         follower_log.logs = copy(follower_logs)
#
#         prev_log_index = missing_index - 1
#         prev_entry = leader_log_fixture[missing_index]
#         prev_log_term = prev_entry.term
#
#         entries = leader_log_fixture[missing_index:]
#         print(entries)
#
#         follower_log.append_entries(prev_log_index, prev_log_term, entries)
#
#
# def test_wrong_terms_log(leader_log_fixture):
#     follower_logs = OneIndexList([
#         LogEntry(1),
#         LogEntry(1),
#         LogEntry(1),
#         LogEntry(2),
#         LogEntry(2),
#         LogEntry(2),
#         LogEntry(3),
#         LogEntry(3),
#         LogEntry(3),
#         LogEntry(3),
#         LogEntry(3),
#     ])
#
#     for missing_index in range(1, 5):
#         follower_log = Log()
#         follower_log.logs = copy(follower_logs)
#
#         prev_log_index = missing_index - 1
#         prev_entry = leader_log_fixture[prev_log_index]
#         prev_log_term = prev_entry.term
#
#         entries = leader_log_fixture[missing_index:]
#
#         follower_log.append_entries(prev_log_index, prev_log_term, entries)
#         assert follower_log.logs == leader_log_fixture.logs
#
#
#     missing_index = 6
#     with pytest.raises(TermNotOk):
#         follower_log = Log()
#         follower_log.logs = copy(follower_logs)
#
#         prev_log_index = missing_index - 1
#         prev_entry = leader_log_fixture[prev_log_index]
#         prev_log_term = prev_entry.term
#
#         entries = leader_log_fixture[missing_index:]
#         print(entries)
#
#         follower_log.append_entries(prev_log_index, prev_log_term, entries)

