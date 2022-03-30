from unittest.mock import MagicMock

import pytest

from raft.raft_state_machine import RoleStateMachine
from raft.rpc_calls import AppendEntriesRequest


@pytest.mark.parametrize("time_difference,initial_role,expected_role", [
    (-1, "follower", "candidate"),
    (0, "follower", "candidate"),
    (1, "follower", "follower"),
    (-1, "candidate", "candidate"),
    (0, "candidate", "candidate"),
    (1, "candidate", "candidate"),
    (-1, "leader", "leader"),
    (0, "leader", "leader"),
    (1, "leader", "leader"),
])
def test_timeout(time_difference, initial_role, expected_role):
    timeout = 150
    machine = RoleStateMachine(
        role=initial_role,
        number_of_machines=1,
        outbox=MagicMock(),
        timeout_provider=lambda: timeout + time_difference,
        log=MagicMock(),
    )

    for i in range(timeout):
        machine.handle_tick()

    assert machine.role == expected_role


@pytest.mark.parametrize("number_of_machines,number_of_votes,initial_role,expected_role", [
    (5, 1, "follower", "follower"),
    (5, 3, "follower", "follower"),
    (5, 2, "candidate", "candidate"),
    (5, 3, "candidate", "leader"),
])
def test_votes(number_of_machines, number_of_votes, initial_role, expected_role):
    machine = RoleStateMachine(
        role=initial_role,
        number_of_machines=number_of_machines,
        outbox=MagicMock(),
        log=MagicMock(),
    )

    for i in range(number_of_votes):
        machine.handle_vote()

    assert machine.role == expected_role


@pytest.mark.parametrize("current_term,message_term,initial_role,expected_role", [
    (0, 0, "follower", "follower"),
    (5, 6, "candidate", "follower"),
    (5, 6, "leader", "follower"),
])
def test_new_leader_detected(current_term, message_term, initial_role, expected_role):
    machine = RoleStateMachine(
        role=initial_role,
        number_of_machines=5,
        current_term=current_term,
        outbox=MagicMock(),
        log=MagicMock(),
    )

    msg = AppendEntriesRequest(
        term=message_term,
        leader_id="testleader",
        prev_log_index=123,
        leader_commit="hshshs",
        entries=["set", "get", "delete"]
    )
    machine.handle_append_entries(msg)

    assert machine.role == expected_role
