import collections.abc
import json
from threading import Thread

import sys


def sysarg(index, default_value):
    return sys.argv[index] if len(sys.argv) > index else default_value


def sysargint(index, default_value):
    return int(sysarg(index, default_value))


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


class SpawnThread:
    success = False

    def __init__(self, thread_number, func):
        self.number = thread_number
        self.external_target = func
        self.name = "SpawnThread-{}".format(self.number)

    def __target(self):
        print("Running {}".format(self.name))
        self.external_target(self)
        self.success = True

    def start(self):
        self.thread = Thread(target=self.__target, args=())
        self.thread.name = self.name
        self.thread.start()

    def join(self, timeout=None):
        self.thread.join(timeout)
        if self.thread.is_alive():
            errmsg = "{} reach timeout {}".format(self.name, timeout)
            raise Exception(errmsg)


class JsonArrayString:
    def __init__(self, array):
        self.array = array

    def __eq__(self, other):
        other = json.loads(other)
        return self._equal(self.array, other)

    def __str__(self):
        return json.dumps(self.array, separators=(',', ':'))

    def __add__(self, other):
        return str(self) + other

    def encode(self):
        return str(self).encode()

    def _equal(self, a, b):
        if not isinstance(b, collections.abc.Sequence):
            return False
        for item in a:
            if item not in b:
                return False
        return True
