import logging
import os
import time
from pathlib import Path
from queue import Queue
from threading import Thread
from typing import TextIO, BinaryIO

from raft.fixed_header_message import FixedHeaderMessageProtocol
from raft.log import Log
from raft.metadata_backend import MetadataBackend
from raft.raft_server import RaftServer
from raft.raft_state_machine import Raft

logging.basicConfig(
    filename=f"/tmp/raft/debug_logs/log_{os.environ.get('SERVER_NUMBER')}",
    filemode='w',
    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
    datefmt='%H:%M:%S',
    level=logging.DEBUG)
logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


class RaftServerController:
    def __init__(self, server_number: int, num_servers: int,
                 log_file_handler: TextIO, metadata_file_handler: BinaryIO):
        base_port = 9090
        self.host = 'localhost'
        self.port = base_port + server_number
        self.log_file = log_file_handler
        self.persistent_metadata = metadata_file_handler
        servers = [('localhost', base_port + idx) for idx in range(num_servers)]
        server_id = ('localhost', base_port + server_number)

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
            servers=servers,
            server_id=server_id,
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
            message = self.inbox.get(block=True)
            self.raft.handle_msg(message)

    def handle_outbox(self):
        while True:
            message = self.outbox.get()
            self.raft_server.send(message)

    def run(self):
        raft_server_thread = Thread(target=self.raft_server.run)
        handle_outbox_thread = Thread(target=self.handle_outbox)
        handle_input_thread = Thread(target=self.handle_inbox)
        raft_clock_driver = Thread(target=self.drive_clock)

        raft_server_thread.start()
        handle_outbox_thread.start()
        handle_input_thread.start()
        raft_clock_driver.start()

        while True:
            os.system('clear')
            print("---Raft status---")
            print(f"timeout      : {self.raft.timeout_ms}")
            print(f"role         : {self.raft.role}")
            print(f"term         : {self.raft.current_term}")
            print(f"commit_index : {self.raft.commit_index}")
            print(f"leader_id:   : {self.raft.leader_id}")
            print(f"votes recvd  : {self.raft.votes_received}")
            print(f"logs         : {self.raft.log.logs}")
            time.sleep(0.1)


if __name__ == "__main__":
    server_number = int(os.environ.get('SERVER_NUMBER'))
    num_servers = int(os.environ.get('NUM_SERVERS'))
    raft_data_log = f"/tmp/raft/data_{server_number}.log"
    raft_metadata = f"/tmp/raft/metadata_{server_number}"
    Path(raft_data_log).touch()
    Path(raft_metadata).touch()
    with open(raft_data_log, 'r+') as log_file, \
         open(raft_metadata, 'rb+') as metadata_file:

        raft_server_controller = RaftServerController(
            log_file_handler=log_file,
            metadata_file_handler=metadata_file,
            server_number=server_number,
            num_servers=num_servers,
        )
        raft_server_controller.run()
