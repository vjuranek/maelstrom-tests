import json
import sys


class Node:
    def __init__(self):
        self.node_id = None
        self.msg_id = 1

    def reply(self, dest, body):
        resp = {
            "src": self.node_id,
            "dest": dest,
            "body": body
        }
        # sys.stderr.write(json.dumps(resp) + "\n")
        sys.stdout.write(json.dumps(resp) + "\n")
        sys.stdout.flush()
