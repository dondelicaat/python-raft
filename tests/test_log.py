import tempfile
from unittest.mock import MagicMock

from raft.log import Log, LogEntry, TermNotOk, OneIndexList, LogNotCaughtUpException
import pytest

from tests.test_leader_follow_replication import get_log_entries


@pytest.fixture(autouse=True)
def log_fixture():
    log = MagicMock()
    log = Log(log)
    log.logs = OneIndexList([])
    log.write_to_log = MagicMock()
    yield log


@pytest.mark.parametrize("prev_log_term,prev_log_index,initial_log,entries,expected",
    [
        (0, 0, [LogEntry(1, 'test')], [LogEntry(1, 'test')], [LogEntry(1, 'test')]),
        (0, 0, [], [LogEntry(1, 'test')], [LogEntry(1, 'test')]),
        (0, 0, [], get_log_entries([1, 2, 2]), get_log_entries([1, 2, 2])),
        (2, 2, get_log_entries([1, 2]), get_log_entries([3, 3, 5]),
        get_log_entries([1, 2, 3, 3, 5])),
    ]
)
def test_append(log_fixture, prev_log_term, prev_log_index, initial_log, entries, expected):
    log_fixture.logs = OneIndexList(initial_log)
    log_fixture.append_entries(prev_log_term=prev_log_term, prev_log_index=prev_log_index, entries=entries)
    assert log_fixture.logs == expected


@pytest.mark.parametrize("prev_log_term,prev_log_index,initial_log,entries,expected",
    [
        (0, 0, [LogEntry(1, 'test')], [LogEntry(2, 'test')], [LogEntry(2, 'test')]),
        (0, 0, get_log_entries([1, 2]), get_log_entries([1, 3, 5]), get_log_entries([1, 3, 5])),
        (1, 1, get_log_entries([1, 2]), get_log_entries([1, 3, 5]), get_log_entries([1, 1, 3, 5])),
        (0, 0, get_log_entries([1, 2]), get_log_entries([1, 3, 5]), get_log_entries([1, 3, 5])),
    ]
)
def test_overwrite_and_truncate(log_fixture, prev_log_term, prev_log_index, initial_log, entries, expected):
    log_fixture.logs = OneIndexList(initial_log)
    log_fixture.append_entries(prev_log_term=prev_log_term, prev_log_index=prev_log_index,
                               entries=entries)
    assert log_fixture.logs == expected


@pytest.mark.parametrize("prev_log_term,prev_log_index,initial_log,entries",
    [
        (2, 1, [LogEntry(1, 'test')], [LogEntry(4, 'test')]),
        (5, 3, get_log_entries([1, 2, 4, 4]), get_log_entries([6, 7]))
    ]
)
def test_term_not_ok_error(log_fixture, prev_log_term, prev_log_index, initial_log, entries):
    log_fixture.logs = OneIndexList(initial_log)
    with pytest.raises(TermNotOk):
        log_fixture.append_entries(prev_log_term=prev_log_term, prev_log_index=prev_log_index,
                                   entries=entries)


@pytest.mark.parametrize("prev_log_term,prev_log_index,initial_log,entries",
    [
        (2, 3, [LogEntry(1, 'test')], [LogEntry(4, 'test')]),
        (3, 5, [LogEntry(1, 'test'), LogEntry(2, 'test'), LogEntry(4, 'test'), LogEntry(4, 'test')], [LogEntry(6, 'test'), LogEntry(7, 'test')])
    ]
)
def test_log_not_caught_up(log_fixture, prev_log_term, prev_log_index, initial_log, entries):
    log_fixture.logs = OneIndexList(initial_log)
    with pytest.raises(LogNotCaughtUpException):
        log_fixture.append_entries(prev_log_term=prev_log_term, prev_log_index=prev_log_index,
                                   entries=entries)


@pytest.mark.parametrize("log,index,expected_entries",
[
    ([LogEntry(1, 'test'), LogEntry(4, 'test')], 2, LogEntry(4, 'test')),
    ([LogEntry(1, 'test'), LogEntry(4, 'test')], slice(None, None), [LogEntry(1, 'test'), LogEntry(4, 'test')]),
    ([LogEntry(1, 'test'), LogEntry(4, 'test')], slice(1, 2), [LogEntry(1, 'test')]),
    ([LogEntry(1, 'test'), LogEntry(4, 'test')], slice(2, 3), [LogEntry(4, 'test')]),
])
def test_log_fetch_entry(log, index, expected_entries):
    log_fixture.logs = OneIndexList(log)
    assert log_fixture.logs[index] == expected_entries


def test_log_write_replay():
    log_entries = get_log_entries([1, 2, 3, 4, 5])
    with tempfile.NamedTemporaryFile('w+') as temp_file:
        log = Log(temp_file)
        for entry in log_entries:
            log.write_to_log(entry)

        replayed_log = Log(temp_file)
        replayed_log.replay()

        assert replayed_log.logs == log_entries
