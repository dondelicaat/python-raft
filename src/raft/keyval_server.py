import socket
import logging
from threading import Thread
from queue import Queue

from raft.fixed_header_message import FixedHeaderMessageProtocol
from raft.message import Message, Close

logging.basicConfig(level=logging.INFO)


class KeyValueServer:
    def __init__(
            self, host, port, protocol: FixedHeaderMessageProtocol,
            queue: Queue, concurrent_clients=16
    ):
        self.host = host
        self.port = port
        self.protocol = protocol
        self.concurrent_clients = concurrent_clients
        self.queue = queue

    def run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:

            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.host, self.port))
            s.listen(self.concurrent_clients)

            while True:
                logging.info("waiting for client")
                client, client_address = s.accept()
                logging.info(f'got client at {client_address}')

                client_connection = Thread(target=self.handle_client, args=(client, client_address))
                client_connection.start()

    def handle_client(self, client, client_address):
        logging.info("started a new connection")

        with client:
            while True:
                msg_bytes = self.protocol.receive_message(client)
                msg = Message.from_bytes(msg_bytes)
                if isinstance(msg.action, Close):
                    logging.info("Closing connection")
                    break
                self.queue.put((msg, client))

