#!/usr/bin/env python

import asyncio
import json
import threading
import time

from node import Node


class EchoServer(Node):
    def __init__(self):
        super().__init__()
        self.register_handler("echo", self.echo_handler)

    async def echo_handler(self, req):
        body = req["body"]
        resp_body = {
            "type": "echo_ok",
            "echo": body["echo"],
        }
        self.reply(req, resp_body)


class BroadcastServer(Node):
    def __init__(self):
        super().__init__()

        self.neighbors = []
        self.messages = set()
        self.msg_lock = threading.RLock()

        self.register_handler("topology", self.topology_handler)
        self.register_handler("broadcast", self.broadcast_handler)
        self.register_handler("read", self.read_handler)

    async def topology_handler(self, req):
        body = req["body"]
        self.neighbors = body["topology"][self.node_id]
        resp_body = {
            "type": "topology_ok",
        }
        self.reply(req, resp_body)

    async def broadcast_handler(self, req):
        """
        Handler implementing broadcast. Broadcast messages sent by nodes in
        the cluster contain field `internal` set to `True`. It also contains
        `broadcast_id`, which has similar purpose as `msg_id`, but to
        distinguish it from `msg_id`, it's called `broadcast_id`.

        Once node receives such message, it replies with `broadcast_ok`
        message. This message also contains `callback_id` which determines
        the callback function to be called by the sender once it receives
        `broadcast_ok` message.
        """
        neighbors_ack = self.neighbors.copy()

        if "internal" in req["body"]:
            # Don't resend message back to the sender.
            neighbors_ack.remove(req["src"])
            self.send(req["src"], {
                "type": "broadcast_ok",
                "callback_id": "{}_{}".format(
                    req["body"]["broadcast_id"], self.node_id)
            })
        else:
            self.reply(req, {"type": "broadcast_ok"})

        msg = req["body"]["message"]
        new_msg = False
        with self.msg_lock:
            if msg not in self.messages:
                self.messages.add(msg)
                new_msg = True

        async def broadcast_ack_handler(req):
            """
            Handler which removes sender from the list of nodes which haven't
            accepted new broadcast message yet.
            """
            if req["body"]["type"] == "broadcast_ok":
                neighbors_ack.remove(req["src"])

        def broadcast_neighbors():
            while neighbors_ack:
                for node in neighbors_ack:
                    self.send(
                        node,
                        broadcast_body,
                        callback=broadcast_ack_handler,
                        callback_id="{}_{}".format(broadcast_id, node)
                    )
                time.sleep(1)

        if new_msg:
            broadcast_id = req["body"].get("broadcast_id")
            if broadcast_id is None:
                # `broadcast_id` has similar function as msg_is in
                # client-server communication.
                broadcast_id = req["body"]["msg_id"]
            broadcast_body = {
                "type": "broadcast",
                "message": msg,
                "internal": True,
                "broadcast_id": broadcast_id
            }

            # This has to be run in separate thread not to block event loop, as
            # in case of network partition, this would block until the
            # connection is re-established.
            asyncio.gather(asyncio.to_thread(broadcast_neighbors))

    async def read_handler(self, req):
        with self.msg_lock:
            resp_body = {
                "type": "read_ok",
                "messages": list(self.messages),
            }
            self.reply(req, resp_body)


class GSetServer(Node):
    def __init__(self):
        super().__init__()

        self._set = set()
        self.set_lock = threading.RLock()

        self.register_handler("read", self.read_handler)
        self.register_handler("add", self.add_handler)
        self.register_handler("replicate", self.replicate_handler)

        self._periodic_tasks.append({"f": self._replicate, "dt": 5})

    def _replicate(self):
        with self.set_lock:
            values = list(self._set)

        self.log("Replicating set {}", values)
        for node in self.node_ids:
            if node != self.node_id:
                resp_body = {
                    "type": "replicate",
                    "value": values,
                }
                self.send(node, resp_body)

    async def read_handler(self, req):
        with self.set_lock:
            resp_body = {
                "type": "read_ok",
                "value": list(self._set),
            }
            self.reply(req, resp_body)

    async def add_handler(self, req):
        with self.set_lock:
            self._set.add(req["body"]["element"])
        self.reply(req, {"type": "add_ok"})

    async def replicate_handler(self, req):
        with self.set_lock:
            self._set = self._set.union(req["body"]["value"])


class GCounter:
    def __init__(self, counters=dict()):
        self.counters = counters

    def sum(self):
        s = 0
        for v in self.counters.values():
            s += v
        return s

    def add(self, node_id, increment):
        counters = self.counters.copy()
        counters[node_id] = counters.get(node_id, 0) + increment
        return GCounter(counters)

    def merge(self, other):
        merge_counter = {**self.counters, **other.counters}
        for k in merge_counter.keys():
            if k in self.counters and k in other.counters:
                merge_counter[k] = max(self.counters[k], other.counters[k])
        return GCounter(merge_counter)

    def to_json(self):
        return json.dumps(self.counters)

    @classmethod
    def from_json(cls, counters):
        return GCounter(json.loads(counters))


class GCounterServer(Node):
    def __init__(self):
        super().__init__()

        self._counters = GCounter()
        self.lock = threading.RLock()

        self.register_handler("read", self.read_handler)
        self.register_handler("add", self.add_handler)
        self.register_handler("replicate", self.replicate_handler)

        self._periodic_tasks.append({"f": self._replicate, "dt": 5})

    def _replicate(self):
        counter_json = self._counters.to_json()
        self.log("Replicating set {}", counter_json)
        for node in self.node_ids:
            if node != self.node_id:
                resp_body = {
                    "type": "replicate",
                    "value": counter_json,
                }
                self.send(node, resp_body)

    async def read_handler(self, req):
        with self.lock:
            resp_body = {
                "type": "read_ok",
                "value": self._counters.sum(),
            }
            self.reply(req, resp_body)

    async def add_handler(self, req):
        with self.lock:
            self._counters = self._counters.add(
                self.node_id, req["body"]["delta"])
        self.reply(req, {"type": "add_ok"})

    async def replicate_handler(self, req):
        other = GCounter.from_json(req["body"]["value"])
        with self.lock:
            self._counters = self._counters.merge(other)
