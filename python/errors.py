import json


class MaelstromError(Exception):
    err_no = None

    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg.format(self=self)

    def to_dict(self):
        return {
            "type": "error",
            "number": self.err_no,
            "reason": self.msg,
        }

    def to_json(self):
        return json.dumps(self.to_dict())


class TimeoutError(MaelstromError):
    err_no = 0


class NotSupportedError(MaelstromError):
    err_no = 10


class TemporarilyUnavailableError(MaelstromError):
    err_no = 11


class MalformedRequestError(MaelstromError):
    err_no = 12


class CrashError(MaelstromError):
    err_no = 13


class AbortError(MaelstromError):
    err_no = 14


class KeyDoesnExistError(MaelstromError):
    err_no = 20


class KeyAlreadyExistsError(MaelstromError):
    err_no = 21


class PreconditionFailedError(MaelstromError):
    err_no = 22


class TxnConflictError(MaelstromError):
    err_no = 30
