#!/usr/bin/env python

import threading

from functools import partial

from node import Node


class EchoServer(Node):
    def __init__(self):
        super().__init__()
        self.register_handler("echo", self.echo_handler)

    def echo_handler(self, req):
        body = req["body"]
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

    def topology_handler(self, req):
        body = req["body"]
        self.neighbors = body["topology"][self.node_id]
        return {
            "type": "topology_ok",
        }

    def broadcast_handler(self, req):
        body = req["body"]
        msg = body["message"]

        new_msg = False
        with self.msg_lock:
            if msg not in self.messages:
                self.messages.add(msg)
                new_msg = True

        if new_msg:
            broadcast_body = {
                "type": "broadcast",
                "message": msg,
                "internal": True,
            }
            for node in self.neighbors:
                if node != req["src"]:
                    self.send(node, broadcast_body)

        if "msg_id" in body:
            return {
                "type": "broadcast_ok",
            }
        else:
            return None

    def read_handler(self, req):
        with self.msg_lock:
            return {
                "type": "read_ok",
                "messages": list(self.messages),
            }
