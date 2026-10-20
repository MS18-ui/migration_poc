"""A recording stand-in for the slice of PySpark the validator engine uses.

It does not compute anything: it tracks column NAMES through each operation and records a textual plan, so
the engine's translation logic (expressions, parameter substitution, window text, join/aggregate wiring) can be
unit-tested on a machine without Java. Semantics are tested with real Spark (see test_validator.py).
"""
import re
from typing import List, Tuple

from agentic.testdata import _split_top


class FCol:
    def __init__(self, txt: str):
        self.txt = txt

    def alias(self, n):
        return FCol(f"{self.txt} AS {n}")

    def cast(self, t):
        return FCol(f"CAST({self.txt} AS {t})")

    @property
    def name(self) -> str:
        m = re.search(r" AS (\w+)$", self.txt)
        if m:
            return m.group(1)
        m = re.match(r"(?:col|expr)\(`?([\w.]+)`?\)$", self.txt)  # Spark names expr('r.rule_id') as rule_id
        return m.group(1).split(".")[-1] if m else self.txt


class FFunctions:
    @staticmethod
    def expr(t):
        return FCol(f"expr({t})")

    @staticmethod
    def col(n):
        return FCol(f"col({n})")

    @staticmethod
    def explode(c):
        return FCol(f"explode({c.txt})")

    @staticmethod
    def broadcast(df):
        return FDF(df.cols, df.ops + ["broadcast"])


def parse_ddl(ddl: str) -> List[Tuple[str, str]]:
    out = []
    for item in _split_top(ddl):
        m = re.match(r"\s*`?(\w+)`?\s+(.*)$", item, re.S)
        if m:
            out.append((m.group(1), m.group(2).strip()))
    return out


class FGroup:
    def __init__(self, df, keys):
        self.df, self.keys = df, keys

    def agg(self, *cols):
        names = [(k.name, "string") for k in self.keys] + [(c.name, "string") for c in cols]
        return FDF(names, self.df.ops + [f"groupBy({', '.join(k.txt for k in self.keys)}).agg({', '.join(c.txt for c in cols)})"])


class FDF:
    def __init__(self, cols, ops=None):
        self.cols, self.ops = list(cols), list(ops or [])

    @property
    def dtypes(self):
        return list(self.cols)

    def _n(self, cols, op):
        return FDF(cols, self.ops + [op])

    def withColumn(self, name, c):
        cols = [x for x in self.cols if x[0] != name] if any(x[0] == name for x in self.cols) else list(self.cols)
        if any(x[0] == name for x in self.cols):
            cols = [(n, t) if n != name else (name, "string") for n, t in self.cols]
        else:
            cols = list(self.cols) + [(name, "string")]
        return self._n(cols, f"withColumn({name} := {c.txt})")

    def filter(self, c):
        return self._n(self.cols, f"filter({c.txt})")

    def select(self, *args):
        cols = []
        for a in args:
            if a == "*":
                cols += self.cols
            else:
                cols.append((a.name, "string"))
        return self._n(cols, "select(" + ", ".join("*" if a == "*" else a.txt for a in args) + ")")

    def selectExpr(self, *exprs):
        cols = [(re.search(r"AS `?(\w+)`?$", e).group(1), "string") for e in exprs]
        return self._n(cols, "selectExpr(" + "; ".join(exprs) + ")")

    def join(self, other, on=None, how="inner"):
        names = {n for n, _ in self.cols}
        cols = list(self.cols) + [c for c in other.cols if c[0] not in names]
        cond = on if isinstance(on, list) else getattr(on, "txt", on)
        bcast = " broadcast" if "broadcast" in other.ops else ""
        return self._n(cols, f"join(on={cond}, how={how}{bcast})")

    def crossJoin(self, other):
        return self._n(self.cols + other.cols, "crossJoin")

    def groupBy(self, *keys):
        return FGroup(self, list(keys))

    def dropDuplicates(self, keys):
        return self._n(self.cols, f"dropDuplicates({keys})")

    def distinct(self):
        return self._n(self.cols, "distinct")

    def unionByName(self, other):
        return self._n(self.cols, "unionByName")

    def createOrReplaceTempView(self, name):
        self.ops.append(f"view({name})")

    def collect(self):
        return []

    def toJSON(self):
        return self


class FReader:
    def __init__(self):
        self._cols = []

    def schema(self, ddl):
        self._cols = parse_ddl(ddl)
        return self

    def option(self, *a):
        return self

    def json(self, path):
        return FDF(self._cols, ["read.json"])


class FSpark:
    def __init__(self):
        self.read = FReader()

    def createDataFrame(self, rows, ddl):
        return FDF(parse_ddl(ddl), ["createDataFrame(empty)"])

    def sql(self, text):
        return FDF([("sql_col", "string")], [f"sql({text})"])


def plan(engine, node_id: str) -> str:
    return "\n".join(engine.df[node_id].ops)
