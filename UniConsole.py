# UniConsole: United Console (Console writer, Progress bar...)

import sys
import subprocess
from .UniClock import TimeFormat

VERSION = "UniConsole v0.2 (Build 20191203) Dev"


def console_nextline(msg, keep=False):
    if sys.stdout.isatty():
        if keep:
            print("\n" + msg)
        else:
            print("\033[2K\r" + msg)


class UniConsole(object):
    def __init__(self, stream=sys.stdout):
        self.stream = stream

        self.is_tty = None
        self.height = None
        self.width = None

        self.reload()

    def reload(self):
        self.is_tty = self.stream.isatty()
        self.height, self.width = subprocess.check_output(['stty', 'size']).decode().split()  # Linux ONLY
        self.height = int(self.height)
        self.width = int(self.width)

    def write(self, msg):
        if self.is_tty:
            self.stream.write("\033[2K\r" + msg)
            self.stream.flush()

    # keep: keep last line of output
    def writeline(self, msg, keep=False):
        if keep:
            self.stream.write("\n" + msg + "\n")
        else:
            self.stream.write("\033[2K\r" + msg + "\n")


class BaseConsoleWidget(object):
    def draw(self, console):
        pass

    def to_string(self):
        raise NotImplementedError()


class ProgressBar(BaseConsoleWidget):
    def __init__(self):
        self.percent = 0
        self.width = 10
        self.style = "|=> |"

    def to_string(self):
        if self.percent < 0 or self.percent > 1:
            raise Exception("Invalid percent value for progress bar.")
        bar_len = int(self.width * self.percent)
        space_len = self.width - bar_len - 1
        return "{}{}{}{}{}".format(self.style[0], self.style[1] * bar_len, self.style[2] if bar_len != self.width else '', self.style[3] * space_len, self.style[4])


class ETA(object):
    def __init__(self, total):
        import time
        self.done = 0
        self.total = total
        self.begin_time = time.time()

    def get(self, progress_length=0):
        import time
        now = time.time()
        return "{}/{} ({}%) Speed: {}/s TimeSpent: {} ETA: {}{}".format(
            self.done,
            self.total,
            round(self.done * 100.0 / self.total, 2),
            round(self.done / (now - self.begin_time), 2) if (now-self.begin_time) > 0 else "INF",
            TimeFormat(now-self.begin_time),
            TimeFormat((now - self.begin_time) * (self.total - self.done) / self.done) if self.done > 0 else "...",
            " [{}{}]".format('#' * int(progress_length * float(self.done) / self.total), '.' * int(progress_length * float(self.total - self.done) / self.total)) if progress_length > 0 else "",
        )
