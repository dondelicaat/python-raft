import socket
import logging
from threading import Thread, Lock

from raft.fixed_header_message import FixedHeaderMessageProtocol
from raft.message import Message, SetValue, GetValue, Ok, Value, DelValue, Close
from raft.storageclient import StorageClient

logging.basicConfig(level=logging.INFO)


class KeyValueServer:
    def __init__(
            self, host, port, protocol: FixedHeaderMessageProtocol,
            storage_client: StorageClient, concurrent_clients=16
    ):
        self.host = host
        self.port = port
        self.protocol = protocol
        self.concurrent_clients = concurrent_clients
        self.storage_client = storage_client
        self.lock = Lock()

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s, self.storage_client:
            self.storage_client.replay_aof()

            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.host, self.port))
            s.listen(self.concurrent_clients)

            while True:
                logging.info("waiting for client")
                client, client_address = s.accept()
                logging.info(f'got client at {client_address}')

                client_connection = Thread(target=self.handle_client, args=(self.lock, client, client_address))
                client_connection.start()

    def handle_client(self, lock, client, client_address):
        logging.info("started a new connection")

        with client:
            while True:
                msg_bytes = self.protocol.receive_message(client)
                msg = Message.from_bytes(msg_bytes)
                if isinstance(msg.action, Close):
                    logging.info("Closing connection")
                    break
                resp = self.handle_msg(lock, msg)
                self.protocol.send_message(client, bytes(resp))

    def handle_msg(self, lock: Lock, msg: Message) -> Message:
        with lock:
            if isinstance(msg.action, SetValue):
                self.handle_set(key=msg.action.key, value=msg.action.value)
                resp = Ok(True)
            elif isinstance(msg.action, GetValue):
                try:
                    val = self.handle_get(key=msg.action.key)
                    resp = Value(value=val)
                except KeyError:
                    resp = Ok(False)
            elif isinstance(msg.action, DelValue):
                self.handle_del(key=msg.action.key)
                resp = Ok(True)
            else:
                raise ValueError(f'unknown message action: {msg.action}')

        return Message(resp)

    def handle_set(self, key, value):
        self.storage_client[key] = value

    def handle_get(self, key):
        return self.storage_client[key]

    def handle_del(self, key):
        try:
            del self.storage_client[key]
        except KeyError:
            return


if __name__ == "__main__":
    protocol = FixedHeaderMessageProtocol(header_size=8)
    data_path = "/Users/4468379/Documents/xccelerated/raft/data"
    aof_log = data_path + "/aof.log"
    storage_client = StorageClient(aof_file_path=aof_log)
    server = KeyValueServer(
        host='localhost', port=8000,
        storage_client=storage_client,
        protocol=protocol
    )
    server.start()
