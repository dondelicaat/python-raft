from unittest.mock import MagicMock

from raft.log import Log, OneIndexList
from raft.raft_state_machine import Raft


leader_log = Log(log_file=MagicMock())
leader_log.logs = OneIndexList([])
leader = Raft(
    servers=[],
    outbox=MagicMock(),
    log=leader_log,
    role="leader",
)

leader.current_term = 0

follower_log = Log(log_file=MagicMock())
follower_log.logs = OneIndexList([])
follower = Raft(
    servers=[],
    outbox=MagicMock(),
    log=leader_log,
    role="follower",
)

follower.current_term = 0



def test_replay_leader_log_single_follower():
    leader.heartbeat()
    msg = leader.outbox.get()

    follower.handl_msg(msg)
    response = follower.outbox.get()

    leader.handle_msg(...)
