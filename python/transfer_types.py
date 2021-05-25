import json


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
