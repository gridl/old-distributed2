import asyncio
import struct
from queue import Queue
from threading import Thread
from time import time

from dill import loads, dumps
from toolz import curry


log = print

@asyncio.coroutine
def read(reader):
    """ Read Python object from reader socket

    Uses dill to deserialize.

    Reads two messages, eight bytes of message length, followed by the message
    """
    b = b''
    while len(b) < 8:
        b += yield from reader.read(8 - len(b))
    nbytes = struct.unpack('L', b)[0]
    msg = b''
    while len(msg) < nbytes:
        msg += yield from reader.read(nbytes - len(msg))
    try:
        return loads(msg)
    except:
        return msg


@asyncio.coroutine
def write(writer, msg):
    """ Write Python object to writer socket

    Uses dill to serialize.

    Sends two messages, eight bytes of message length, followed by the message
    """
    if not isinstance(msg, bytes):
        msg = dumps(msg)
    writer.write(struct.pack('L', len(msg)))
    writer.write(msg)


@asyncio.coroutine
def connect(host, port, delay=0.1, timeout=1, loop=None):
    """ Connect to ip/port pair.  Return reader/writer pair

    This is like ``asyncio.open_connection`` but retries if the remote
    connection is not yet up.
    """
    reader, writer = None, None
    start = time()
    while not reader:
        try:
            reader, writer = yield from asyncio.open_connection(
                    host=host, port=port, loop=loop)
        except OSError:
            if timeout is not None and time() - start > timeout:
                raise
            else:
                yield from asyncio.sleep(delay, loop=loop)
    return reader, writer


@curry
@asyncio.coroutine
def client_connected(handlers, reader, writer):
    """ Dispatch new connections to coroutine-handlers

    handlers is a dictionary mapping operation names (e.g. {'op': 'get_data'})
    to coroutines that take ``reader, writer, msg`` triples.

    This is given to ``serve`` which uses ``asyncio.start_server``
    """
    try:
        while True:
            msg = yield from read(reader)
            op = msg.pop('op')
            close = msg.pop('close', False)
            reply = msg.pop('reply', True)
            if op == 'close':
                if reply:
                    yield from write(writer, b'OK')
                break
            try:
                handler = handlers[op]
            except KeyError:
                result = b'No handler found: ' + op.encode()
                log(result)
            else:
                if iscoroutine(handler):
                    result = yield from handler(reader, writer, **msg)
                else:
                    result = handler(reader, writer, **msg)
            if reply:
                yield from write(writer, result)
            if close:
                break
    finally:
        writer.close()


def pingpong(reader, writer):
    """ Coroutine to send ping reply """
    return b'pong'


def get_data(data, reader, writer, keys=()):
    return {k: data.get(k) for k in keys}

def update_data(old_data, reader, writer, data={}):
    old_data.update(data)
    return b'OK'

def delete_data(data, reader, writer, keys=()):
    for key in keys:
        del data[key]
    return b'OK'


from concurrent.futures import ThreadPoolExecutor
executor = ThreadPoolExecutor(50)


def delay(loop, func, *args, **kwargs):
    """ Run function in separate thread, turn into coroutine """
    future = executor.submit(func, *args, **kwargs)
    return asyncio.futures.wrap_future(future, loop=loop)


@asyncio.coroutine
def send_recv(reader, writer, reply=True, loop=None, **kwargs):
    """ Send and recv with a reader/writer pair

    Keyword arguments turn into the message

    response = yield from send_recv(reader, writer, op='ping', reply=True)
    """
    if isinstance(reader, (bytes, str)) and isinstance(writer, int):
        given_ip_port = True
        reader, writer = yield from connect(reader, writer, loop=loop)
    else:
        given_ip_port = False
    msg = kwargs
    msg['reply'] = reply
    if 'close' not in msg:
        msg['close'] = given_ip_port
    yield from write(writer, msg)
    if reply:
        response = yield from read(reader)
    else:
        response = None
    if kwargs['close']:
        writer.close()
    return response


class rpc(object):
    """ Use send_recv to cause rpc computations on client_connected calls

    By convention the `client_connected` coroutine looks for operations by name
    in the `op` key of a message.

    >>> msg = {'op': 'func', 'key1': 100, 'key2': 1000}
    >>> result = yield from send_recv(reader, writer, **msg)  # doctest: +SKIP

    This class uses this convention to provide a Python interface for calling
    remote functions

    >>> remote = rpc(reader, writer)  # doctest: +SKIP
    >>> result = yield from remote.func(key1=100, key2=1000)  # doctest: +SKIP
    """
    def __init__(self, reader, writer, loop=None):
        self.reader = reader
        self.writer = writer
        self.loop = loop

    def __getattr__(self, key):
        def _(**kwargs):
            if 'loop' not in kwargs:
                kwargs['loop'] = self.loop
            return send_recv(self.reader, self.writer, op=key,
                            **kwargs)
        return _


def sync(cor, loop=None):
    """ Run coroutine in event loop running in other thread

    Block and return result.

    See Also:
        spawn_loop: start up a long running coroutine
    """
    loop = loop or asyncio.get_event_loop()
    q = Queue()
    @asyncio.coroutine
    def f():
        result = yield from cor
        q.put(result)

    if not loop.is_running():
        loop.run_until_complete(f())
    else:
        loop.call_soon_threadsafe(asyncio.async, f())
    return q.get()


def spawn_loop(cor, loop=None):
    """ Run a coroutine in a loop in a new thread

    Generally this coroutine is long running, like ``start_server``

    See Also:
        sync: run short-running coroutine in thread on other thread
    """
    def f(loop, cor):
        asyncio.set_event_loop(loop)
        loop.run_until_complete(cor)

    t = Thread(target=f, args=(loop, cor))
    t.start()
    return t, loop


def iscoroutine(func):
    if hasattr(func, 'func'):
        return iscoroutine(func.func)
    if hasattr(func, '_is_coroutine'):
        return func._is_coroutine
    return False
