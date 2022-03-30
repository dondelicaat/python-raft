import os
import time
from queue import Queue
from threading import Thread

from raft.fixed_header_message import FixedHeaderMessageProtocol
from raft.log import Log
from raft.raft_server import RaftServer
from raft.raft_state_machine import Raft


class RaftServerController:
    def __init__(self, server_id: int, num_servers: int):
        base_port = 9090
        self.host = 'localhost'
        self.port = base_port + server_id
        try:
            self.log_file = open(f"/tmp/data_{server_id}.log", 'w+')
        except OSError:
            print(f"OSError: {e}")
            raise
        servers = [('localhost', base_port + idx) for idx in range(num_servers) if idx != server_id]

        input_queue = Queue()
        output_queue = Queue()
        self.raft = Raft(
            servers=servers,
            inbox=input_queue,
            outbox=output_queue,
            log=Log(self.log_file)
        )
        self.raft_server = RaftServer(
            host=self.host,
            port=self.port,
            protocol=FixedHeaderMessageProtocol(8),
            input_queue=input_queue,
            output_queue=output_queue,
        )

    def do_tick(self):
        self.raft.handle_tick()

    def drive_clock(self, ticks_per_second=1.0):
        while True:
            time.sleep(1 / ticks_per_second)
            self.do_tick()

    def run(self):
        raft_server_inbox_thread = Thread(target=self.raft_server.run)
        raft_server_outbox_thread = Thread(target=self.raft_server.handle_output)
        raft_server_inbox_thread.start()
        raft_server_outbox_thread.start()

        raft_request_processor = Thread(target=self.raft.handle_msg)
        raft_clock_driver = Thread(target=self.drive_clock)
        raft_request_processor.start()
        raft_clock_driver.start()


if __name__ == "__main__":
    raft_server_controller = RaftServerController(
        server_id=int(os.environ.get('SERVER_ID')),
        num_servers=int(os.environ.get('NUM_SERVERS')),
    )
    raft_server_controller.run()
