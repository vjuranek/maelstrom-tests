import asyncio
import json
import sys
import threading


class Node:
    def __init__(self):
        self.node_id = None
        self.msg_id = 0
        self._lock = threading.RLock()
        self._log_lock = threading.RLock()
        self._handlers = {
            "init": self.init_handler,
        }

    def send(self, dest, body):
        with self._lock:
            self.msg_id += 1
            resp = {
                "src": self.node_id,
                "dest": dest,
                "body": {
                    **body,
                },
            }
            self.log("{} -> {}: {}", self.node_id, dest, resp)
            sys.stdout.write(json.dumps(resp) + "\n")
            sys.stdout.flush()

    def reply(self, req, resp_body):
        body = {
            **resp_body,
            "msg_id": self.msg_id,
            "in_reply_to": req["body"]["msg_id"],
        }
        self.send(req["src"], body)

    async def run(self):
        for line in sys.stdin:
            req, body = parse_req(line)
            req_type = body["type"]
            if req_type not in self._handlers:
                raise Exception("No handler for request type %r" % req_type)
            task = asyncio.create_task(self._handlers[req_type](req))
            await task

    def register_handler(self, req_type, handler):
        if req_type in self._handlers:
            raise Exception("Handler for %r already registered" % req_type)
        self._handlers[req_type] = handler

    async def init_handler(self, req):
        body = req["body"]
        self.node_id = body["node_id"]
        resp_body = {
            "type": "init_ok",
        }
        self.reply(req, resp_body)

    def log(self, log_msg, *args):
        if args:
            log_msg = log_msg.format(*args)
        with self._log_lock:
            sys.stderr.write(json.dumps(log_msg) + "\n")
            sys.stderr.flush()


def parse_req(line):
    req = json.loads(line)
    body = req["body"]
    return req, body
