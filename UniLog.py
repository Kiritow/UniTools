# UniLog: United Logger
import logging
import sys
from typing import TextIO


def get_or_create_logger(name: str | None = None, filename: str | None = None,
                 fileonly: bool=False, level: int = logging.INFO,
                 default_encoding: str ='utf-8',
                 log_format: str ="%(asctime)s @%(module)s [%(levelname)s] %(funcName)s: %(message)s"):
    if name is None:
        name = __name__
    
    if not filename and fileonly:
        raise Exception("fileonly set to true but no filename provided.")
    
    logger = logging.getLogger(name)
    if not getattr(logger, "_is_configured", None):
        formatter = logging.Formatter(log_format)
        if not fileonly:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
        if filename is not None:
            file_handler = logging.FileHandler(filename, encoding=default_encoding)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        logger.setLevel(level)
        setattr(logger, "_is_configured", True)
    
    return logger


class ConsoleLog(object):
    def __init__(self, filename: str, stream: TextIO | None =sys.__stdout__, level: int = logging.INFO, default_encoding: str ='utf-8'):
        self.level = level
        self.stream = stream
        self.under_log = get_or_create_logger(filename=filename, fileonly=True, level=level, default_encoding=default_encoding, log_format="%(asctime)s: %(message)s")
        self.buffer = ""

    def write(self, message: str):
        if message.endswith("\n"):
            self.under_log.log(self.level, self.buffer + message[:-2])
            self.buffer = ""
        else:
            self.buffer += message
            # self.under_log.log(self.level, message)

        if self.stream:
            self.stream.write(message)


def redirect_stdout(filename: str, keep: bool = False):
    if sys.stdout == sys.__stdout__:
        sys.stdout = ConsoleLog(filename, stream=sys.__stderr__ if keep else None)
    else:
        sys.stderr.write("[Warning] Unable to redirect stdout.\n")
