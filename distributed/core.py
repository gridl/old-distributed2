import asyncio
import struct
from queue import Queue
from threading import Thread
from time import time

from dill import loads, dumps
from toolz import curry


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
def connect(host, port, delay=0.1, timeout=None, loop=None):
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
                yield from asyncio.sleep(delay)
    return reader, writer


@curry
@asyncio.coroutine
def client_connected(handlers, reader, writer):
    """ Dispatch new connections to coroutine-handlers

    handlers is a dictionary mapping operation names (e.g. {'op': 'get-data'})
    to coroutines that take ``reader, writer, msg`` triples.

    This is given to ``serve`` which uses ``asyncio.start_server``
    """
    try:
        while True:
            msg = yield from read(reader)
            if msg['op'] == 'close':
                if msg.get('reply'):
                    yield from write(writer, b'OK')
                break
            handler = handlers[msg['op']]
            yield from handler(reader, writer, msg)
            if msg.get('close'):
                break
    finally:
        writer.close()


@asyncio.coroutine
def pingpong(reader, writer, msg):
    """ Coroutine to send ping reply """
    assert msg['op'] == 'ping'
    yield from write(writer, b'pong')


@asyncio.coroutine
def manage_data(data, reader, writer, msg):
    """ Coroutine to serve data from dictionary """
    if msg['op'] == 'get-data':
        out = {k: data.get(k) for k in msg['keys']}
    if msg['op'] == 'update-data':
        data.update(msg['data'])
        out = b'OK'
    if msg['op'] == 'del-data':
        for key in msg['keys']:
            del data[key]
        out = b'OK'
    yield from write(writer, out)


def serve(bind, port, handlers, loop=None):
    return asyncio.start_server(client_connected(handlers), bind, port, loop=loop)


from concurrent.futures import ThreadPoolExecutor


executor = ThreadPoolExecutor(20)


def delay(loop, func, *args, **kwargs):
    """ Run function in separate thread, turn into coroutine """
    future = executor.submit(func, *args, **kwargs)
    return asyncio.futures.wrap_future(future, loop=loop)


@asyncio.coroutine
def send_recv(reader, writer, reply=True, **kwargs):
    """ Send and recv with a reader/writer pair

    Keyword arguments turn into the message

    response = yield from send_recv(reader, writer, op='ping', reply=True)
    """
    if isinstance(reader, (bytes, str)) and isinstance(writer, int):
        reader, writer = yield from connect(reader, writer,
                                            loop=kwargs.pop('loop', None))
    msg = kwargs
    msg['reply'] = reply
    yield from write(writer, msg)
    if reply:
        response = yield from read(reader)
    else:
        response = None
    if kwargs.get('close'):
        writer.close()
    return response


def sync(loop, cor):
    """ Run coroutine in event loop running in other thread

    Block and return result.

    See Also:
        spawn_loop: start up a long running coroutine
    """
    q = Queue()
    @asyncio.coroutine
    def f():
        result = yield from cor
        q.put(result)

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
