import json
import threading

from errors import AbortError
from errors import TxnConflictError


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


class TxnState:
    SERVICE = "lin-kv"
    KEY = "root"

    def __init__(self, node, id_gen):
        self._node = node
        self.id_gen = id_gen

    def apply_txn(self, txn):
        curr_json = self._lin_kv_read(self.KEY) or "{}"
        current_db = DbNode.from_json(self._node, self.id_gen, curr_json)
        if current_db is None:
            current_db = DbNode(self._node, self.id_gen)
        new_db = current_db.copy()
        new_db, res = new_db.apply_txn(txn)
        new_db.save()
        if current_db != new_db:
            self._lin_kv_cas(self.KEY, current_db.to_json(), new_db.to_json())
        return res

    def _lin_kv_read(self, key):
        req = {
            "type": "read",
            "key": key,
        }
        resp = self._node.service_rpc(self.SERVICE, req)
        return resp["body"].get("value")

    def _lin_kv_cas(self, key, current, new):
        req = {
            "type": "cas",
            "key": key,
            "from": current,
            "to": new,
            "create_if_not_exists": True,
        }
        resp = self._node.service_rpc(self.SERVICE, req)
        if resp["body"]["type"] != "cas_ok":
            raise TxnConflictError(resp["body"]["text"])


class ServiceRequest:
    TIMEOUT = 5

    def __init__(self):
        self.lock = threading.RLock()
        self.finish = threading.Event()
        self.value = None

    def wait(self):
        self.finish.clear()

        if self.value:
            return self.value

        self.finish.wait(self.TIMEOUT)

        if self.value:
            return self.value
        else:
            raise Exception("Timeout while waiting for service response.")

    def set(self, value):
        with self.lock:
            self.value = value
            self.finish.set()


class MonotonicId:

    def __init__(self, node_id):
        self._node_id = node_id
        self._lock = threading.RLock()
        self._id = 0

    def next(self):
        with self._lock:
            self._id += 1
        return "{}-{}".format(self._node_id, self._id)


class Thunk:
    SERVICE = "lin-kv"

    def __init__(self, node, id, value, saved):
        self.node = node
        self._id = id
        self._value = value
        self.saved = saved

    def id(self):
        return self._id

    def to_json(self):
        return self._value

    @classmethod
    def from_json(cls, value_json):
        return value_json

    def value(self):
        if not self._value:
            body = {
                "type": "read",
                "key": self._id,
            }
            resp = self.node.service_rpc(self.SERVICE, body)
            self._value = Thunk.from_json(resp["body"]["value"])

        return self._value

    def save(self):
        if not self.saved:
            body = {
                "type": "write",
                "key": self.id(),
                "value": self.to_json(),
            }
            resp = self.node.service_rpc(self.SERVICE, body)

            if resp["body"]["type"] == "write_ok":
                self.saved = True
            else:
                raise AbortError("Unable to save thunk {}".format(self._id()))


class DbNode:

    def __init__(self, node, id_gen, map={}):
        self.node = node
        self.id_gen = id_gen
        self._map = map

    def copy(self):
        return DbNode(self.node, self.id_gen, self._map.copy())

    def to_json(self):
        db_map = {}
        for key, thunks in self._map.items():
            thunks_ids = []
            for thunk in thunks:
                thunks_ids.append(thunk.id())
            db_map[key] = thunks_ids
        return json.dumps(db_map)

    @classmethod
    def from_json(cls, node, id_gen, db_json):
        db_map = {}
        pairs = json.loads(db_json)
        if pairs:
            for key, thunk_ids in pairs.items():
                thunks = []
                for id in thunk_ids:
                    thunks.append(Thunk(node, id, None, True))
                db_map[key] = thunks
        return DbNode(node, id_gen, db_map)

    def save(self):
        for thunks in self._map.values():
            for thunk in thunks:
                thunk.save()

    def get(self, key):
        thunks = self._map.get(key)
        if thunks:
            values = []
            for thunk in thunks:
                values.append(thunk.value())
            return values

    def apply_txn(self, txn):
        res = []
        for fn, key, value in txn:
            # DB is dict str -> list.
            key = str(key)

            if fn == "r":
                res.append([fn, key, self.get(key)])
            elif fn == "append":
                res.append([fn, key, value])
                value_list = self._map[key].copy() if key in self._map else []
                thunk = Thunk(
                    self.node, self.id_gen.next(), value, False)
                value_list.append(thunk)
                self._map[key] = value_list
            else:
                raise Exception("Unknown TXN operation {!r}".format(fn))
        return [self, res]

    def __eq__(self, other):
        if not isinstance(other, DbNode):
            return False
        return self._map == other._map
