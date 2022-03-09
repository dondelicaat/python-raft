import socket


class FixedHeaderMessageProtocol:
    def __init__(self, header_size):
        self.header_size = header_size

    def _recv_size(self, socket: socket.socket, msg_size: int) -> bytes:
        contents = b''

        while len(contents) != msg_size:
            contents += socket.recv(msg_size - len(contents))

        return contents

    def receive_message(self, socket: socket.socket) -> bytes:
        header = self._recv_size(socket, self.header_size)
        content_size = int.from_bytes(header, 'little', signed=False)
        body = self._recv_size(socket, content_size)

        return body

    def send_message(self, socket: socket.socket, body: bytes):
        msg_size = len(body)

        try:
            header = msg_size.to_bytes(self.header_size, 'little', signed=False)
        except OverflowError:
            raise ValueError(f'The message is too long. Max size is {2**self.header_size}')

        socket.send(header)
        socket.send(body)



