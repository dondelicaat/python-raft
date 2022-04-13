import os
import time
from queue import Queue
from threading import Thread
from typing import TextIO, BinaryIO

from raft.fixed_header_message import FixedHeaderMessageProtocol
from raft.log import Log
from raft.metadata_backend import MetadataBackend
from raft.raft_server import RaftServer
from raft.raft_state_machine import Raft


class RaftServerController:
    def __init__(self, server_id: int, num_servers: int,
                 log_file_handler: TextIO, metadata_file_handler: BinaryIO):
        base_port = 9090
        self.host = 'localhost'
        self.port = base_port + server_id
        self.log_file = log_file_handler
        self.persistent_metadata = metadata_file_handler
        servers = {idx: ('localhost', base_port + idx) for idx in range(num_servers)}

        assert len(servers) > 2 and len(servers) % 2 == 1

        self.inbox = Queue()
        self.outbox = Queue()
        self.raft = Raft(
            servers=servers,
            server_id=server_id,
            outbox=self.outbox,
            log=Log(self.log_file),
            metadata_backend=MetadataBackend(file_handle=self.persistent_metadata)
        )
        self.raft_server = RaftServer(
            host=self.host,
            port=self.port,
            protocol=FixedHeaderMessageProtocol(8),
            inbox=self.inbox,
        )

    def do_tick(self):
        self.raft.handle_tick()

    def do_heartbeat(self):
        while True:
            self.raft.handle_heartbeat()
            # Heartbeat each 10 ms
            time.sleep(0.01)

    def drive_clock(self, ticks_per_second=1.0):
        while True:
            time.sleep(1 / ticks_per_second)
            self.do_tick()

    def handle_inbox(self):
        while True:
            message, client_id = self.inbox.get(block=True)
            self.raft.handle_msg(message)

    def handle_outbox(self):
        while True:
            client_id, message = self.outbox.get()
            self.raft_server.send(client_id, message)

    def run(self):
        raft_server_thread = Thread(target=self.raft_server.run)
        handle_outbox_thread = Thread(target=self.handle_outbox)
        handle_input_thread = Thread(target=self.handle_inbox)
        raft_clock_driver = Thread(target=self.drive_clock)
        raft_heartbeat_driver = Thread(target=self.do_heartbeat)

        raft_server_thread.start()
        handle_outbox_thread.start()
        handle_input_thread.start()
        raft_clock_driver.start()
        raft_heartbeat_driver.start()

        while True:
            print(f"Raft status: role {self.raft.role}, current term: {self.raft.current_term}, votes received {self.raft.votes_received}", end='\r')
            time.sleep(0.1)


if __name__ == "__main__":
    server_id = int(os.environ.get('SERVER_ID'))
    num_servers = int(os.environ.get('NUM_SERVERS'))
    with open(f"/tmp/data_{server_id}.log", 'w+') as log_file, \
         open(f"/tmp/metadata_{server_id}", 'wb+') as metadata_file:

        raft_server_controller = RaftServerController(
            log_file_handler=log_file,
            metadata_file_handler=metadata_file,
            server_id=server_id,
            num_servers=num_servers,
        )
        raft_server_controller.run()
