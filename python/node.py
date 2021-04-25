import json
import sys
import threading


class Node:
    def __init__(self):
        self.node_id = None
        self.msg_id = 0
        self._lock = threading.RLock()

    def send(self, dest, body):
        with self._lock:
            self.msg_id += 1
            resp = {
                "src": self.node_id,
                "dest": dest,
                "body": {
                    **body,
                    "msg_id": self.msg_id,
                },
            }
            # sys.stderr.write(json.dumps(resp) + "\n")
            sys.stdout.write(json.dumps(resp) + "\n")
            sys.stdout.flush()

    def reply(self, req, body):
        body = {
            **body,
            "in_reply_to": req["body"]["msg_id"],
        }
        self.send(req["src"], body)

    def run(self):
        for line in sys.stdin:
            req, body = parse_req(line)
            resp_body = self.response(req.copy(), body)
            self.reply(req, resp_body)

    def response(self, req, req_body):
        raise NotImplementedError()


def parse_req(line):
    req = json.loads(line)
    body = req["body"]
    return req, body
