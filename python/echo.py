#!/usr/bin/env python

import json
import sys

from node import Node


class EchoServer(Node):
    def run(self):
        for line in sys.stdin:
            msg = json.loads(line)
            body = msg["body"]
            self.msg_id += 1
            resp_body = {
                "msg_id": self.msg_id,
                "in_reply_to": body["msg_id"],
            }

            if body["type"] == "init":
                self.node_id = body["node_id"]
                resp_body["type"] = "init_ok"

            if body["type"] == "echo":
                resp_body ["type"] = "echo_ok"
                resp_body["echo"] = body["echo"]

            self.reply(msg["src"], resp_body)


def main():
    echo_server = EchoServer()
    echo_server.run()


if __name__ == "__main__":
    main()
