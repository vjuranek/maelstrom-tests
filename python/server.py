#!/usr/bin/env python

import threading

from functools import partial

from node import Node


class EchoServer(Node):
    def __init__(self):
        super().__init__()
        self.register_handler("echo", partial(self.echo_handler, self))

    @staticmethod
    def echo_handler(cls, body):
        return {
            "type": "echo_ok",
            "echo": body["echo"],
        }


class BroadcastServer(Node):
    def __init__(self):
        super().__init__()

        self.neighbors = []
        self.messages = set()
        self.msg_lock = threading.RLock()

        self.register_handler("topology", self.topology_handler)
        self.register_handler("broadcast", self.broadcast_handler)
        self.register_handler("read", self.read_handler)

    def topology_handler(self, body):
        self.neighbors = body["topology"][self.node_id]
        return {
            "type": "topology_ok",
        }

    def broadcast_handler(self, body):
        msg = body["message"]

        with self.msg_lock:
            if msg not in self.messages:
                self.messages.add(msg)

                for node in self.neighbors:
                    broadcast_body = {
                        "type": "broadcast",
                        "message": msg,
                        "internal": True,
                    }
                    self.send(node, broadcast_body)

        if "msg_id" in body:
            return {
                "type": "broadcast_ok",
            }
        else:
            return None

    def read_handler(self, body):
        with self.msg_lock:
            return {
                "type": "read_ok",
                "messages": list(self.messages),
            }


def main():
    test_server = BroadcastServer()
    # test_server = EchoServer()
    test_server.run()


if __name__ == "__main__":
    main()
