import json
import threading


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
    KEY = "root"

    def __init__(self, node):
        self._node = node

    def apply_txn(self, txn):
        curr_json = self._lin_kv_read(self.KEY) or "{}"
        current_db = DbNode.from_json(curr_json)
        if current_db is None:
            current_db = DbNode()
        new_db = current_db.copy()
        new_db, res = new_db.apply_txn(txn)
        if current_db != new_db:
            self._lin_kv_cas(self.KEY, current_db.to_json(), new_db.to_json())
        return res

    def _lin_kv_read(self, key):
        req = {
            "type": "read",
            "key": key,
        }
        resp = self._node.service_rpc("lin-kv", req)
        return resp["body"].get("value")

    def _lin_kv_cas(self, key, current, new):
        req = {
            "type": "cas",
            "key": key,
            "from": current,
            "to": new,
            "create_if_not_exists": True,
        }
        resp = self._node.service_rpc("lin-kv", req)
        if resp["body"]["type"] != "cas_ok":
            raise Exception("CAS operation failed: {!r}". format(
                resp["body"]["text"]))


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


class DbNode:

    def __init__(self, db={}):
        self._db = db

    def copy(self):
        return DbNode(self._db.copy())

    def to_json(self):
        return json.dumps(self._db)

    @classmethod
    def from_json(cls, db_json):
        return DbNode(json.loads(db_json))

    def get(self, key):
        return self._db.get(key)

    def append(self, key, value):
        new_db = self._db.copy()
        new_db[key] = value
        return new_db

    def apply_txn(self, txn):
        res = []
        for fn, key, value in txn:
            if fn == "r":
                res.append([fn, key, self._db.get(str(key))])
            elif fn == "append":
                res.append([fn, key, value])
                # DB is dict str -> list.
                key = str(key)
                value_list = self._db[key].copy() if key in self._db else []
                value_list.append(value)
                self._db[key] = value_list
            else:
                raise Exception("Unknown TXN operation {!r}".format(fn))
        return [self, res]

    def __eq__(self, other):
        if not isinstance(other, DbNode):
            return False
        return self._db == other._db
