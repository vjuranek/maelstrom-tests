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
    def __init__(self):
        self._state = {}

    def apply_txn(self, txn):
        res = []
        for fn, key, value in txn:
            if fn == "r":
                res.append([fn, key, self._state.get(key, [])])
            if fn == "append":
                res.append([fn, key, value])
                s = self._state.get(key, []).copy()
                s.append(value)
                self._state[key] = s
        return res


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
