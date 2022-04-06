import os
import time
from queue import Queue
from threading import Thread

from raft.fixed_header_message import FixedHeaderMessageProtocol
from raft.log import Log
from raft.metadata_backend import MetadataBackend
from raft.raft_server import RaftServer
from raft.raft_state_machine import Raft


class RaftServerController:
    def __init__(self, server_id: int, num_servers: int):  #  pass file handles to raft_server_controller
        base_port = 9090
        self.host = 'localhost'
        self.port = base_port + server_id
        try:
            self.log_file = open(f"/tmp/data_{server_id}.log", 'w+')
            self.persistent_metadata = open(f"/tmp/metadata_{server_id}", 'wb+')
        except OSError as e:
            print(f"OSError: {e}")
            raise
        servers = {idx: ('localhost', base_port + idx) for idx in range(num_servers)}

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
        # leader_heartbeat_driver()

        raft_server_thread.start()
        handle_outbox_thread.start()
        handle_input_thread.start()
        raft_clock_driver.start()


if __name__ == "__main__":
    raft_server_controller = RaftServerController(
        server_id=int(os.environ.get('SERVER_ID')),
        num_servers=int(os.environ.get('NUM_SERVERS')),
    )
    raft_server_controller.run()
