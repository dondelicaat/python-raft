import logging
from queue import Queue
from collections import deque
from threading import Thread

from raft.fixed_header_message import FixedHeaderMessageProtocol
from raft.keyval_server import KeyValueServer
from raft.message_processor import MessageProcessor
from raft.storageclient import StorageClient

logger = logging.getLogger(__name__)


class ServerController:
    def __init__(
        self, storage_client: StorageClient,
        protocol: FixedHeaderMessageProtocol,
        port: int, host: str,
    ):
        self.input_queue = Queue()
        self.output_queue = deque()

        self.message_processor = MessageProcessor(
            in_queue=self.input_queue,
            out_queue=self.output_queue,
            storage_client=storage_client
        )
        self.server = KeyValueServer(
            host=host, port=port,
            input_queue=self.input_queue,
            output_queue=self.output_queue,
            protocol=protocol
        )

    def run(self):
        self.message_processor.start()

        server_thread = Thread(target=self.server.run, args=())
        message_processor_thread = Thread(target=self.message_processor.process, args=())

        logging.info("starting server")
        server_thread.start()
        logging.info("starting message processor")
        message_processor_thread.start()


if __name__ == "__main__":
    protocol = FixedHeaderMessageProtocol(header_size=8)
    aof_log = "/Users/4468379/Documents/xccelerated/raft/data/aof.log"
    storage_client = StorageClient(aof_file_path=aof_log)

    server_controller = ServerController(
        storage_client=storage_client,
        protocol=protocol,
        port=8000,
        host='localhost'
    )

    server_controller.run()
