from __future__ import annotations

import time
from contextlib import contextmanager


class MetricsCollector:
    def __init__(self):
        self.values: dict[str, float] = {}

    @contextmanager
    def timer(self, key: str):
        start = time.perf_counter()
        try:
            yield
        finally:
            self.values[key] = time.perf_counter() - start

    def set_value(self, key: str, value: float) -> None:
        self.values[key] = value
