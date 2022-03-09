import logging
from queue import Queue
from collections import deque

from raft.storageclient import StorageClient
from raft.message import Message, SetValue, GetValue, Ok, Value, DelValue

logger = logging.getLogger(__name__)


class MessageProcessor:
    def __init__(self, in_queue: Queue, out_queue: deque, storage_client: StorageClient):
        self.out_queue = out_queue
        self.in_queue = in_queue
        self.storage_client = storage_client

    def start(self):
        self.storage_client.start()

    def stop(self):
        self.storage_client.stop()

    def process(self):
        while True:
            message, client_id = self.in_queue.get(block=True)
            resp = self.handle_msg(message)
            self.out_queue.appendleft((resp, client_id))

    def handle_msg(self, msg: Message) -> Message:
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
