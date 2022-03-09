import socket

from raft.message import Message, SetValue, GetValue, DelValue, Close
from src.raft.fixed_header_message import FixedHeaderMessageProtocol


class Client:
    def __init__(self, host, port, protocol: FixedHeaderMessageProtocol):
        self.host = host
        self.port = port
        self.protocol = protocol
        self.socket = None

    def connect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.connect((self.host, self.port))

    def close(self):
        msg = Message(Close())
        self.protocol.send_message(self.socket, bytes(msg))
        self.socket.close()

    def __setitem__(self, key, value):
        msg = Message(SetValue(key, value))
        self.protocol.send_message(self.socket, bytes(msg))
        response = self.protocol.receive_message(self.socket)
        return Message.from_bytes(response)

    def __getitem__(self, key):
        msg = Message(GetValue(key))
        self.protocol.send_message(self.socket, bytes(msg))
        response = self.protocol.receive_message(self.socket)
        return Message.from_bytes(response).action.value

    def __delitem__(self, key):
        msg = Message(DelValue(key))
        self.protocol.send_message(self.socket, bytes(msg))
        response = self.protocol.receive_message(self.socket)
        return Message.from_bytes(response)


if __name__ == "__main__":
    protocol = FixedHeaderMessageProtocol(header_size=8)
    client = Client(
        host='localhost', port=8000,
        protocol=protocol
    )
    client.connect()
    client["TEST"] = "KEY"
    print(client["k"])
    client.close()
