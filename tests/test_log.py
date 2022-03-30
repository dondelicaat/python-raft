from unittest.mock import MagicMock

from raft.log import Log, LogEntry, TermNotOk, OneIndexList, LogNotCaughtUpException
import pytest


@pytest.fixture(autouse=True)
def log_fixture():
    log = MagicMock()
    log = Log(log)
    log.logs = OneIndexList([])
    log.write_to_log = MagicMock()
    yield log


@pytest.mark.parametrize("prev_log_term,prev_log_index,initial_log,entries,expected",
    [
        (0, 0, [LogEntry(1)], [LogEntry(1)], [LogEntry(1)]),
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


@pytest.mark.parametrize("prev_log_term,prev_log_index,initial_log,entries,expected",
    [
        (0, 0, [LogEntry(1)], [LogEntry(2)], [LogEntry(2)]),
        (0, 0, [LogEntry(1), LogEntry(2)], [LogEntry(1), LogEntry(3), LogEntry(5)],
        [LogEntry(1), LogEntry(3), LogEntry(5)]),
        (1, 1, [LogEntry(1), LogEntry(2)], [LogEntry(1), LogEntry(3), LogEntry(5)],
        [LogEntry(1), LogEntry(1), LogEntry(3), LogEntry(5)]),
        (0, 0, [LogEntry(1), LogEntry(2)], [LogEntry(1), LogEntry(3), LogEntry(5)],
        [LogEntry(1), LogEntry(3), LogEntry(5)]),
    ]
)
def test_overwrite_and_truncate(log_fixture, prev_log_term, prev_log_index, initial_log, entries, expected):
    log_fixture.logs = OneIndexList(initial_log)
    log_fixture.append_entries(prev_log_term=prev_log_term, prev_log_index=prev_log_index,
                               entries=entries)
    assert log_fixture.logs == expected


@pytest.mark.parametrize("prev_log_term,prev_log_index,initial_log,entries",
    [
        (2, 1, [LogEntry(1)], [LogEntry(4)]),
        (5, 3, [LogEntry(1), LogEntry(2), LogEntry(4), LogEntry(4)], [LogEntry(6), LogEntry(7)])
    ]
)
def test_term_not_ok_error(log_fixture, prev_log_term, prev_log_index, initial_log, entries):
    log_fixture.logs = OneIndexList(initial_log)
    with pytest.raises(TermNotOk):
        log_fixture.append_entries(prev_log_term=prev_log_term, prev_log_index=prev_log_index,
                                   entries=entries)


@pytest.mark.parametrize("prev_log_term,prev_log_index,initial_log,entries",
    [
        (2, 3, [LogEntry(1)], [LogEntry(4)]),
        (3, 5, [LogEntry(1), LogEntry(2), LogEntry(4), LogEntry(4)], [LogEntry(6), LogEntry(7)])
    ]
)
def test_log_not_caught_up(log_fixture, prev_log_term, prev_log_index, initial_log, entries):
    log_fixture.logs = OneIndexList(initial_log)
    with pytest.raises(LogNotCaughtUpException):
        log_fixture.append_entries(prev_log_term=prev_log_term, prev_log_index=prev_log_index,
                                   entries=entries)
