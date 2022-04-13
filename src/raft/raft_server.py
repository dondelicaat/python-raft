import logging
import socket
import uuid
from threading import Thread
from queue import Queue

from raft.fixed_header_message import FixedHeaderMessageProtocol
from raft.rpc_calls import Message, Close

logger = logging.getLogger(__name__)


class RaftServer:
    def __init__(
            self, host, port, protocol: FixedHeaderMessageProtocol,
            servers: dict, server_id: int, inbox: Queue, concurrent_clients=16
    ):
        self.host = host
        self.port = port
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

                # Non-raft client
                if client_address not in self.servers.values():
                    msg.host = client_address[0]
                    msg.port = client_address[1]
                    self.clients[f"{msg.host}:{msg.port}"] = client

                if isinstance(msg.action, Close):
                    logging.info("Closing connection")
                    break
                self.inbox.put(msg)

    def send(self, message):
        # try:
        #     client = self.clients[client_id]
        # except KeyError as key_err:
        #     logging.info("Key %s not found in clients dict", client_id)
        #     if client_id in self.servers.keys():
        #         client = self.get_connection(**self.servers[client_id])
        #         self.clients[client_id] = client
        #     else:
        #         # Todo: create / fetch non-raft client connection?
        #         logging.info("Could not send message, seems the client has disconnected.")
        #         return
        client_id = message.receiver
        logger.info(f"Trying to send message: {message.action} to {message.host} : {message.port}!")
        if client_id in self.servers.keys():
            client_address = self.servers[client_id]
            client = self.get_connection()
            try:
                client.connect(client_address)
                self.protocol.send_message(socket=client, body=bytes(message))
                self.close(client, message.sender, message.receiver)
            except ConnectionError:
                logger.info(f"Could not connect to {client_address}")

        elif message.port and message.host:
            logger.info("Fetching host creds")
            client = self.clients[f"{message.host}:{message.port}"]
            self.protocol.send_message(socket=client, body=bytes(message))



    def get_connection(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s

    def close(self, s, sender, receiver):
        msg = Message(Close(), sender=sender, receiver=receiver)
        self.protocol.send_message(s, bytes(msg))
        s.close()
