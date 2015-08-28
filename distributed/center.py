import asyncio
from collections import defaultdict
from functools import partial
from queue import Queue
from time import sleep

from .core import read, write, client_connected, spawn_loop

log = print

class Center(object):
    """ Central metadata storage

    A Center serves as central point of metadata storage among workers.  It
    maintains dictionaries of which worker has which keys and which keys are
    owned by which workers.  Computational systems tend to check in with a
    Center to determine their available resources.

    Example
    -------

    A center can be run in an event loop

    >>> c = Center('192.168.0.123', 8000)
    >>> coroutine = c.go()

    Or separately in a thread

    >>> c = Center('192.168.0.123', 8000, start=True, block=False)  # doctest: +SKIP
    >>> c.close()  # doctest: +SKIP
    """
    def __init__(self, ip, port, bind='*', loop=None, start=False, block=True):
        self.ip = ip
        self.port = port
        self.bind = bind
        self.who_has = defaultdict(set)
        self.has_what = defaultdict(set)
        self.ncores = dict()
        self.loop = loop or asyncio.new_event_loop()

        if start:
            self.start(block)

    @asyncio.coroutine
    def go(self):
        cor = partial(manage_metadata, self.who_has, self.has_what,
                                       self.ncores)
        handlers = {'add-keys': cor,
                    'del-keys': cor,
                    'who-has': cor,
                    'has-what': cor,
                    'register': cor,
                    'ncores': cor,
                    'unregister': cor}

        self.server = yield from asyncio.start_server(
                client_connected(handlers), self.bind, self.port,
                loop=self.loop)
        log("Center server up")
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
def manage_metadata(who_has, has_what, ncores, reader, writer, msg):
    """ Main coroutine to manage metadata

    Operations:

    *  register: register a new worker
    *  unregister: remove a known worker
    *  add-keys: state that a worker has gained certain keys
    *  del-keys: state that a worker has lost certain keys
    *  who-has: ask which workers have certain keys
    *  has-what: ask which keys a set of workers has
    """
    log("msg received: " + str(msg))
    if msg['op'] == 'register':
        has_what[msg['address']] = set(msg.get('keys', ()))
        ncores[msg['address']] = msg['ncores']
        print("Register %s" % str(msg['address']))
        if msg.get('reply'):
            yield from write(writer, b'OK')

    if msg['op'] == 'unregister':
        keys = has_what.pop(msg['address'])
        del ncores[msg['address']]
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

    if msg['op'] == 'ncores':
        if 'addresses' in msg:
            result = {k: ncores[k] for k in msg['addresses']}
        else:
            result = ncores
        yield from write(writer, result)
