import asyncio
from collections import defaultdict
from functools import partial
from queue import Queue
from time import sleep

from .core import read, write, serve, spawn_loop


class Center(object):
    def __init__(self, ip, port, bind='*', loop=None, start=False, block=True):
        self.ip = ip
        self.port = port
        self.bind = bind
        self.who_has = defaultdict(set)
        self.has_what = defaultdict(set)
        self.loop = loop or asyncio.new_event_loop()

        if start:
            self.start(block)

    @asyncio.coroutine
    def go(self):
        cor = partial(manage_metadata, self.who_has, self.has_what)
        handlers = {'add-keys': cor,
                    'del-keys': cor,
                    'who-has': cor,
                    'has-what': cor,
                    'register': cor,
                    'unregister': cor}

        self.server = yield from serve(self.bind, self.port, handlers, loop=self.loop)
        yield from self.server.wait_closed()

    def start(self, block):
        if block:
            self.loop.run_until_complete(self.go())
        else:
            self._thread, _ = spawn_loop(self.go(), loop=self.loop)

    def close(self):
        self.loop.call_soon_threadsafe(self.server.close)
        if hasattr(self, '_thread'):
            self._thread.join()


@asyncio.coroutine
def manage_metadata(who_has, has_what, reader, writer, msg):
    if msg['op'] == 'register':
        has_what[msg['address']] = set(msg.get('keys', ()))
        if msg.get('reply'):
            yield from write(writer, b'OK')

    if msg['op'] == 'unregister':
        keys = has_what.pop(msg['address'])
        for key in keys:
            who_has[key].remove(msg['address'])
        if msg.get('reply'):
            yield from write(writer, b'OK')

    if msg['op'] == 'add-keys':
        has_what[msg['address']].update(msg['keys'])
        for key in msg['keys']:
            who_has[key].add(msg['address'])
        if msg.get('reply'):
            yield from write(writer, b'OK')

    if msg['op'] == 'del-keys':
        for key in msg['keys']:
            if key in has_what[msg['address']]:
                has_what[msg['address']].remove(key)
            try:
                who_has[key].remove(msg['address'])
            except KeyError:
                pass
        if msg.get('reply'):
            yield from write(writer, b'OK')

    if msg['op'] == 'who-has':
        if 'keys' in msg:
            result = {k: who_has[k] for k in msg['keys']}
        else:
            result = who_has
        yield from write(writer, result)

    if msg['op'] == 'has-what':
        if 'keys' in msg:
            result = {k: has_what[k] for k in msg['keys']}
        else:
            result = has_what
        yield from write(writer, result)
