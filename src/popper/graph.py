from threading import Thread, Lock
from typing import Callable, Any, Optional
import time


class Node:
    def __init__(self, id: str, func: Callable[[Any], Any],
                 num_threads: int = 1):
        self.id = id
        self.func = func
        self.in_edges = []
        self.out_edges = []
        self._threads = []
        self._stop_flag = False
        self._num_threads = num_threads
        self._active_threads = 0
        self._lock = Lock()

    def add_in_edge(self, edge):
        self.in_edges.append(edge)

    def add_out_edge(self, edge):
        self.out_edges.append(edge)

    def start(self):
        self._stop_flag = False
        self._threads = [
            Thread(target=self._run, name=f"{self.id}-worker-{i}")
            for i in range(self._num_threads)
        ]
        self._active_threads = self._num_threads
        for t in self._threads:
            t.start()

    def stop(self):
        self._stop_flag = True
        for t in self._threads:
            t.join()

    def _run(self):
        while True:
            work_done = False
            for edge in self.in_edges:
                item = edge.claim_item()
                if item is not None:
                    result = self.func(item)
                    for out_edge in self.out_edges:
                        out_edge.add_item(result)
                    work_done = True
            if not work_done:
                if all(e._is_completed for e in self.in_edges):
                    self._mark_thread()
                    return
                else:
                    time.sleep(0.01)

    def _mark_thread(self):
        with self._lock:
            self._active_threads -= 1
            if self._active_threads == 0:
                self._stop_flag = True

    @property
    def is_completed(self):
        return self._stop_flag


class ClaimableList:
    def __init__(self):
        self._items = []
        self._lock = Lock()

    def add(self, item: Any):
        """Add an item to the list."""
        with self._lock:
            self._items.append(item)

    def claim(self) -> Optional[Any]:
        """
        Atomically remove and return the first item.
        Returns None if empty.
        """
        with self._lock:
            if not self._items:
                return None
            return self._items.pop(0)

    def __len__(self):
        with self._lock:
            return len(self._items)

    def __repr__(self):
        with self._lock:
            return f"ClaimableList({self._items})"


class Edge:
    def __init__(self, source: Node, target: Node):
        self.source = source
        self.target = target
        self.items = ClaimableList()
        self.completed = False

    def add_item(self, item: Any):
        print("Adding: ", item)
        self.items.add(item)

    def claim_item(self) -> Optional[Any]:
        return self.items.claim()

    def mark_completed(self):
        self.completed = True

    @property
    def _is_completed(self):
        return self.source.is_completed

    def __repr__(self):
        return (f"Edge({self.source.id}->{self.target.id}, "
                f"todo_items={len(self.items)}, "
                f"completed={self.completed})")
