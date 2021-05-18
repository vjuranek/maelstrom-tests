import asyncio
import json
import sys
import threading
import time


class Node:
    def __init__(self):
        self.node_id = None
        self.msg_id = 0
        self._lock = threading.RLock()
        self._log_lock = threading.RLock()
        self._handlers = {
            "init": self.init_handler,
        }
        self._callbacks = {}
        self._periodic_tasks = {}

    def send(self, dest, body, callback=None, callback_id=None):
        if callback and callback_id:
            self._callbacks[callback_id] = callback

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
        await self.process_lines()

    async def process_lines(self):
        for line in sys.stdin:
            req, body = parse_req(line)
            try:
                handler = self._get_handler(body)
            except Exception:
                pass
            else:
                task = asyncio.create_task(handler(req))
                await task

    def _get_handler(self, body):
        req_type = body["type"]
        callback_id = body.get("callback_id")

        if callback_id and callback_id in self._callbacks:
            handler = self._callbacks[callback_id]
            del self._callbacks[callback_id]
        else:
            if req_type not in self._handlers:
                raise Exception(
                    "No handler for request type %r" % req_type)
            handler = self._handlers[req_type]

        return handler

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
        self.start_periodic_tasks()

    def start_periodic_tasks(self):
        for task in self._periodic_tasks:
            t = threading.Thread(target=self._run_periodic_task, args=(task,))
            t.start()

    def _run_periodic_task(self, task):
        while True:
            task["f"]()
            time.sleep(task["dt"])

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
