import asyncio
from collections import defaultdict
from functools import partial

from .core import read, write, serve


class Center(object):
    def __init__(self, ip, port, bind='*', loop=None):
        self.ip = ip
        self.port = port
        self.bind = bind
        self.who_has = defaultdict(set)
        self.has_what = defaultdict(set)
        self.loop = loop

    @asyncio.coroutine
    def go(self):
        cor = partial(manage_metadata, self.who_has, self.has_what)
        handlers = {'add-keys': cor,
                    'del-keys': cor,
                    'who-has': cor,
                    'has-what': cor}

        yield from serve(self.bind, self.port, handlers, loop=self.loop)

@asyncio.coroutine
def manage_metadata(who_has, has_what, reader, writer, msg):
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
        result = {k: who_has[k] for k in msg['keys']}
        yield from write(writer, result)

    if msg['op'] == 'has-what':
        result = {k: has_what[k] for k in msg['keys']}
        yield from write(writer, result)
