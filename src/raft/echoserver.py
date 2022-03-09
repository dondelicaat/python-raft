import socket
import logging
from threading import Thread
from time import sleep

from src.raft.fixed_header_message import FixedHeaderMessageProtocol

logging.basicConfig(level=logging.INFO)


class EchoServer:
    def __init__(self, host, port, protocol: FixedHeaderMessageProtocol, concurrent_clients=16):
        self.host = host
        self.port = port
        self.protocol = protocol
        self.concurrent_clients = concurrent_clients

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.host, self.port))
            s.listen(self.concurrent_clients)

            while True:
                logging.info("waiting for client")
                # Accept opens a new socket on a different port since communication is port to port not multicast.
                # With 16 clients we open 16 new ports with a socket. This also sets limit on concurrent connections.
                client, client_address = s.accept()
                logging.info(f'got client at {client_address}')

                client_connection = Thread(target=self.handle_client, args=(client, client_address))
                client_connection.start()

    def handle_client(self, client, client_address):
        logging.info("started a new connection")

        with client:
            while True:
                msg = self.protocol.receive_message(socket)
                if msg.decode().strip() == 'exit':
                    logging.info("Closing connection")
                    client.send(b'connection closed\r\n')
                    break
                logging.info(f'Received message: {msg}')
                self.protocol.send_message(client, b'OK')
                sleep(0.01)


if __name__ == "__main__":
    protocol = FixedHeaderMessageProtocol(header_size=8)
    server = EchoServer(
        host='localhost', port=8000,
        protocol=protocol
    )
    server.start()


# how to keep this running
# how do we accept connections from multiple clients
# How do we make sure to get complete messages
