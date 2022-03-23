import socket
import logging
import uuid
from threading import Thread
from queue import Queue

from raft.fixed_header_message import FixedHeaderMessageProtocol
from raft.rpc_calls import AppendEntries, RequestVote

logging.basicConfig(level=logging.INFO)


class KeyValueServer:
    def __init__(
            self, host, port, protocol: FixedHeaderMessageProtocol,
            input_queue: Queue, output_queue: Queue,
            concurrent_clients=16
    ):
        self.host = host
        self.port = port
        self.protocol = protocol
        self.concurrent_clients = concurrent_clients
        self.in_queue = input_queue
        self.out_queue = output_queue
        self.clients = {}

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

    def handle_output(self):
        while True:
            client_id, message = self.out_queue.get()
            client = self.clients[client_id]
            self.protocol.send_message(socket=client, body=bytes(message))

    def handle_client(self, client, client_address):
        logging.info("started a new connection")
        client_id = str(uuid.uuid4())

        with client:
            while True:
                msg_bytes = self.protocol.receive_message(client)
                msg = Message.from_bytes(msg_bytes)
                if isinstance(msg.action, Close):
                    logging.info("Closing connection")
                    break
                self.in_queue.put((msg, client_id))
                self.clients[client_id] = client

