# UniClock: United Clock (for timing, date calculate)

import time

VERSION = "UniClock v0.1 (Build 20190514.1) Dev"


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

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.stop()

    @staticmethod
    def now():
        return time.time()


def TimeFormat(second):
    second = int(second)
    if second < 60:
        return "{}s".format(second)
    elif second < 3600:
        minute = int(second / 60)
        second = int(second % 60)
        return "{}m{}s".format(minute, second)
    else:
        hour = int(second / 3600)
        minute = int((second - hour * 3600) / 60)
        second = int(second - hour * 3600 - minute * 60)
        return "{}h{}m{}s".format(hour, minute, second)
