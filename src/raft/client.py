import socket
import uuid

from raft.rpc_calls import Message, SetValue, GetValue, DelValue, Close, Forward
from raft.fixed_header_message import FixedHeaderMessageProtocol


class Client:
    def __init__(self, host, port, protocol: FixedHeaderMessageProtocol):
        self.host = host
        self.port = port
        self.protocol = protocol
        self.socket = None

    def connect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(5.0)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        print(f"Connecting to {self.host}:{self.port}")
        self.socket.connect((self.host, self.port))
        self.socket.settimeout(30.0)

    def reconnect(self):
        print("Trying to reconnect...")
        self.close()
        self.connect()
        print("Reconnected.")

    def close(self):
        msg = Message(Close(), receiver=None, sender=None)
        self.protocol.send_message(self.socket, bytes(msg))
        self.socket.close()

    def __setitem__(self, key, value):
        msg = Message(SetValue(str(uuid.uuid4()), key, value), receiver=None, sender=None)
        response = self.send(msg)
        return response

    def __getitem__(self, key):
        msg = Message(GetValue(str(uuid.uuid4()), key), receiver=None, sender=None)
        response = self.send(msg)
        return response.action.value

    def __delitem__(self, key):
        msg = Message(DelValue(str(uuid.uuid4()), key), receiver=None, sender=None)
        response = self.send(msg)
        return response

    def send(self, msg):
        print(f"Sending message to {self.host}:{self.port}")
        self.protocol.send_message(self.socket, bytes(msg))
        response = Message.from_bytes(self.protocol.receive_message(self.socket))
        if isinstance(response.action, Forward):
            print(
                f"Discovered a new leader on "
                f"{response.action.leader_host}:{response.action.leader_port}"
            )
            self.host = response.action.leader_host
            self.port = response.action.leader_port
            self.reconnect()
            self.protocol.send_message(self.socket, bytes(msg))
            response = Message.from_bytes(self.protocol.receive_message(self.socket))

        return response


if __name__ == "__main__":
    protocol = FixedHeaderMessageProtocol(header_size=8)
    client = Client(
        host='localhost',
        port=9090,
        protocol=protocol
    )
    client.connect()
    client["TEST"] = "KEY"
    print("SET KEY, NOW GET IT!")
    print(client["k"])
    client.close()
