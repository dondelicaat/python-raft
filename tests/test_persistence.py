import tempfile
from unittest.mock import MagicMock

from raft.metadata_backend import MetadataBackend
from raft.raft_state_machine import Raft


def test_persistence():
    with tempfile.NamedTemporaryFile('rb+') as tmp:
        metadata_backend = MetadataBackend(tmp)

        raft = Raft(
            servers={},
            server_id=0,
            outbox=MagicMock(),
            metadata_backend=metadata_backend,
            log=MagicMock(),
        )

        raft2 = Raft(
            servers={},
            server_id=0,
            outbox=MagicMock(),
            metadata_backend=metadata_backend,
            log=MagicMock(),
        )
        raft_term = 5
        raft.current_term = raft_term
        assert raft2.current_term == raft_term
        raft_voted_for = 7
        raft.voted_for = raft_voted_for
        assert raft2.voted_for == raft_voted_for

