import logging
from queue import Queue
from threading import Thread

from raft.fixed_header_message import FixedHeaderMessageProtocol
from raft.keyval_server import KeyValueServer
from raft.message_processor import MessageProcessor
from raft.storageclient import StorageClient

logger = logging.getLogger(__name__)


class ServerController:
    def __init__(
        self, queue: Queue, storage_client: StorageClient,
        protocol: FixedHeaderMessageProtocol, port: int, host: str,
    ):
        self.message_processor = MessageProcessor(
            queue=queue, storage_client=storage_client,
            protocol=protocol
        )
        self.server = KeyValueServer(
            host=host, port=port,
            queue=queue,
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
    data_path = "/Users/4468379/Documents/xccelerated/raft/data"
    aof_log = data_path + "/aof.log"
    storage_client = StorageClient(aof_file_path=aof_log)

    message_queue = Queue()

    message_processor = MessageProcessor(
        queue=message_queue, storage_client=storage_client,
        protocol=protocol
    )

    server_controller = ServerController(
        queue=message_queue,
        storage_client=storage_client,
        protocol=protocol,
        port=8000,
        host='localhost'
    )

    server_controller.run()
