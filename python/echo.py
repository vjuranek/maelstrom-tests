#!/usr/bin/env python

from node import Node


def echo_handler(body):
    return {
        "type": "echo_ok",
        "echo": body["echo"],
    }


class EchoServer(Node):
    def __init__(self):
        super().__init__()
        self.register_handler("echo", echo_handler)


def main():
    echo_server = EchoServer()
    echo_server.run()


if __name__ == "__main__":
    main()
