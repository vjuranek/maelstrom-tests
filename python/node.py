import json
import sys
import threading


class Node:
    def __init__(self):
        self.node_id = None
        self.msg_id = 0
        self._lock = threading.RLock()

    def reply(self, dest, body):
        with self._lock:
            resp = {
                "src": self.node_id,
                "dest": dest,
                "body": body
            }
            # sys.stderr.write(json.dumps(resp) + "\n")
            sys.stdout.write(json.dumps(resp) + "\n")
            sys.stdout.flush()
