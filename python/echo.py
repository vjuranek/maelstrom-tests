#!/usr/bin/env python

import sys

import node


class EchoServer(node.Node):
    def run(self):
        for line in sys.stdin:
            req, body = node.parse_req(line)
            resp_body = {}

            if body["type"] == "init":
                self.node_id = body["node_id"]
                resp_body["type"] = "init_ok"

            if body["type"] == "echo":
                resp_body["type"] = "echo_ok"
                resp_body["echo"] = body["echo"]

            self.reply(req, resp_body)


def main():
    echo_server = EchoServer()
    echo_server.run()


if __name__ == "__main__":
    main()
