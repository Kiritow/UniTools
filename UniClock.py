# UniClock: United Clock (for timing, date calculate)
import time


class UniClock(object):
    def __init__(self):
        self.begin_time = time.time()
        self.end_time = time.time()

    def start(self):
        self.begin_time = time.time()

    def count(self):
        return time.time() - self.begin_time

    def stop(self):
        self.end_time = time.time()

    def duration(self):
        return self.end_time - self.begin_time

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb): # type: ignore
        self.stop()

    @staticmethod
    def now():
        return time.time()
