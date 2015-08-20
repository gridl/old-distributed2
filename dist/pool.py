import asyncio
import random
from queue import Queue
from toolz import merge, partial, pipe, concat, frequencies
from toolz.curried import map
import uuid

from .core import (read, write, connect, delay, manage_data, serve, send_recv,
        spawn_loop, sync)


class Pool(object):
    def __init__(self, center_ip, center_port, loop=None, start=True):
        self.center_ip = center_ip
        self.center_port = center_port
        self.loop = loop or asyncio.new_event_loop()

        if start:
            self.start()
            self.sync_center()

    @asyncio.coroutine
    def _sync_center(self):
        reader, writer = yield from connect(self.center_ip, self.center_port,
                                            loop=self.loop)
        self.who_has = yield from send_recv(reader, writer, op='who-has',
                                            reply=True)
        self.has_what = yield from send_recv(reader, writer, op='has-what',
                                             reply=True, close=True)
        writer.close()

    def sync_center(self):
        cor = self._sync_center()
        return sync(self.loop, cor)

    def start(self):
        self._kill_q = Queue()

        @asyncio.coroutine
        def f():
            while self._kill_q.empty():
                yield from asyncio.sleep(0.01)
            self._kill_q.get()

        self._thread, _ = spawn_loop(f(), loop=self.loop)

    def close(self):
        self._kill_q.put('')
        self._thread.join()

    @asyncio.coroutine
    def _apply_async(self, func, args=(), kwargs={}, key=None):
        needed, args2, kwargs2 = needed_args_kwargs(args, kwargs)

        ip, port = choose_worker(needed, self.who_has, self.has_what)

        if key is None:
            key = str(uuid.uuid1())

        pc = PendingComputation(key, func, args2, kwargs2, needed,
                                loop=self.loop)
        yield from pc._start(ip, port, self.who_has, self.has_what)
        return pc

    def apply_async(self, func, args=(), kwargs={}, key=None):
        cor = self._apply_async(func, args, kwargs, key)
        return sync(self.loop, cor)


class PendingComputation(object):
    def __init__(self, key, function, args, kwargs, needed, loop=None):
        self.key = key
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.needed = needed
        self.loop = loop

    @asyncio.coroutine
    def _start(self, ip, port, who_has=None, has_what=None):
        msg = dict(op='compute', key=self.key, function=self.function,
                   args=self.args, kwargs=self.kwargs, needed=self.needed,
                   reply=True)
        self.ip = ip
        self.port = port
        self._has_what = has_what
        self._who_has = who_has
        self.reader, self.writer = yield from connect(ip, port)
        yield from write(self.writer, msg)

    @asyncio.coroutine
    def _get(self):
        result = yield from read(self.reader)
        assert result == b'OK'
        self._who_has[self.key].add((self.ip, self.port))
        self._has_what[(self.ip, self.port)].add(self.key)
        self._result = RemoteData(self.key, reader=self.reader,
                                  writer=self.writer, loop=self.loop)
        return self._result

    def get(self):
        try:
            return self._result
        except AttributeError:
            return sync(self.loop, self._get())


class RemoteData(object):
    def __init__(self, key, reader=None, writer=None, loop=None):
        self.key = key
        self.reader = reader
        self.writer = writer
        self.loop = loop

    @asyncio.coroutine
    def _get(self):
        result = yield from send_recv(self.reader, self.writer, op='get-data',
                                      keys=[self.key], reply=True, close=True)
        self.writer.close()
        self._result = result[self.key]
        return self._result

    def get(self):
        try:
            return self._result
        except AttributeError:
            return sync(self.loop, self._get())


def choose_worker(needed, who_has, has_what):
    """ Select worker to run computation

    Currently selects the worker with the most data local
    """
    counts = pipe(needed, map(who_has.__getitem__), concat, frequencies)
    if not counts:
        return random.choice(list(has_what))
    else:
        biggest = max(counts.values())
        best = {k: v for k, v in counts.items() if v == biggest}
        return random.choice(list(best))


def needed_args_kwargs(args, kwargs):
    """ Replace RemoteData objects with keys, fill needed """
    needed = set()
    args2 = []
    for arg in args:
        if isinstance(arg, RemoteData):
            args2.append(arg.key)
            needed.add(arg.key)
        else:
            args2.append(arg)

    kwargs2 = {}
    for k, v in kwargs.items():
        if isinstance(arg, RemoteData):
            kwargs2[k] = arg.key
            needed.add(arg.key)
        else:
            kwargs2[k] = arg

    return needed, args2, kwargs2
