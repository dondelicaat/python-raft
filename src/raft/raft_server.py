import logging
import socket
from threading import Thread
from queue import Queue
from typing import List, Tuple

from raft.fixed_header_message import FixedHeaderMessageProtocol
from raft.rpc_calls import Message, Close

logger = logging.getLogger(__name__)


class RaftServer:
    def __init__(
            self, server_id: Tuple[str, int], protocol: FixedHeaderMessageProtocol,
            servers: List[Tuple[str, int]], inbox: Queue, concurrent_clients=16
    ):
        self.protocol = protocol
        self.servers = servers
        self.server_id = server_id
        self.concurrent_clients = concurrent_clients
        self.inbox = inbox
        self.clients = {}
        self.raft_clients = {}

    def run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:

            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(self.server_id)
            s.listen(self.concurrent_clients)

            while True:
                logging.info("waiting for client")
                client, client_address = s.accept()
                logging.info(f'got client at {client_address}')

                client_connection = Thread(target=self.handle_client, args=(client, client_address))
                client_connection.start()

    def handle_client(self, client, client_address):
        logging.info("started a new connection")

        try:
            with client:
                while True:
                    msg_bytes = self.protocol.receive_message(client)
                    msg = Message.from_bytes(msg_bytes)

                    # Non-raft client
                    if msg.sender is None and msg.receiver is None:
                        logging.info(f"received a message from client {client_address} to {self.server_id}")
                        msg.sender = client_address
                        msg.receiver = self.server_id
                        self.clients[msg.sender] = client

                    if isinstance(msg.action, Close):
                        logging.info("Closing connection")
                        break
                    self.inbox.put(msg)
        except ConnectionResetError:
            logging.info("Client closed the connection")

    def send(self, message):
        logger.info(
            f"Trying to send message: {message.action} "
            f"from {message.sender} to {message.receiver}!"
        )
        if message.receiver in self.servers:
            client = self.get_connection()
            try:
                client.connect(message.receiver)
                self.protocol.send_message(socket=client, body=bytes(message))
                self.close(client, message.sender, message.receiver)
            except ConnectionError:
                logger.info(f"Could not connect to {message.receiver}")
        else:
            client = self.clients[message.receiver]
            logger.info("Trying to send msg to client!")
            self.protocol.send_message(socket=client, body=bytes(message))
            logger.info("Message successfully sent to client!")

    def get_connection(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s

    def close(self, s, sender, receiver):
        msg = Message(Close(), sender=sender, receiver=receiver)
        self.protocol.send_message(s, bytes(msg))
        s.close()
