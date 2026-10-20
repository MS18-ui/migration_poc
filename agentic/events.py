"""Structured run events: what the UI (or a log file) shows while a migration runs.

Code anywhere in the pipeline calls `emit(kind, **data)`. Events go to the `RunEvents` bound to the CURRENT thread
(a ContextVar), so concurrent jobs do not mix, and nothing happens at all when no sink is bound (the CLI).

Kinds used:
  run_start / run_done / run_failed   whole-run lifecycle
  stage      {stage, title, status: start|done|error|skipped, seconds?, error?}
  tool       {agent, turn, tool, ok, detail}      one agent tool call and its result
  log        {agent, message}                      agent notes (nudges, accepted on turn N...)
  file       {label, name, path}                   an intermediate or final file was written
  note       {message}                             anything else worth showing
Events are also appended to `events_<stamp>.jsonl` in the run folder once the folder exists, so a finished run can be
reopened later with its full step history.
"""
import contextvars
import json
import threading
import time
from typing import Any, Dict, List, Optional


class RunEvents:
    def __init__(self, path: Optional[str] = None):
        self.items: List[Dict[str, Any]] = []
        self.path = path
        self._lock = threading.Lock()

    def set_path(self, path: str) -> None:
        """Start persisting to `path` (events emitted before the run folder existed are written first)."""
        with self._lock:
            self.path = path
            with open(path, "a", encoding="utf-8") as f:
                for ev in self.items:
                    f.write(json.dumps(ev, default=str) + "\n")

    def emit(self, kind: str, **data: Any) -> Dict[str, Any]:
        ev = {"ts": time.time(), "t": time.strftime("%H:%M:%S"), "kind": kind, **data}
        with self._lock:
            self.items.append(ev)
            if self.path:
                try:
                    with open(self.path, "a", encoding="utf-8") as f:
                        f.write(json.dumps(ev, default=str) + "\n")
                except OSError:
                    pass  # a log write must never break a migration
        return ev

    def snapshot(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self.items)


_current: "contextvars.ContextVar[Optional[RunEvents]]" = contextvars.ContextVar("agentic_run_events", default=None)


def bind(events: RunEvents):
    """Bind `events` to the current thread/context; returns a token for unbind()."""
    return _current.set(events)


def unbind(token) -> None:
    _current.reset(token)


def current() -> Optional[RunEvents]:
    return _current.get()


def emit(kind: str, **data: Any) -> None:
    ev = _current.get()
    if ev is not None:
        ev.emit(kind, **data)


def load_events(path: str) -> List[Dict[str, Any]]:
    """Read a persisted events_<stamp>.jsonl (missing/garbled lines are skipped)."""
    out: List[Dict[str, Any]] = []
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                try:
                    out.append(json.loads(line))
                except ValueError:
                    continue
    except OSError:
        pass
    return out
