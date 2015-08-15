import asyncio
import struct
from dill import loads, dumps
from toolz import curry


@asyncio.coroutine
def read(reader):
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
    if not isinstance(msg, bytes):
        msg = dumps(msg)
    writer.write(struct.pack('L', len(msg)))
    writer.write(msg)


@asyncio.coroutine
def connect(host, port, delay=0.1, timeout=None, loop=None):
    reader, writer = None, None
    while not reader:
        try:
            reader, writer = yield from asyncio.open_connection(
                    host=host, port=port, loop=loop)
        except OSError:
            yield from asyncio.sleep(delay)
    return reader, writer


@curry
@asyncio.coroutine
def client_connected(handlers, reader, writer):
    try:
        while True:
            msg = yield from read(reader)
            handler = handlers[msg['op']]
            yield from handler(reader, writer, msg)
            if msg.get('close'):
                break
    finally:
        writer.close()


@asyncio.coroutine
def pingpong(reader, writer, msg):
    assert msg['op'] == 'ping'
    yield from write(writer, b'pong')


@asyncio.coroutine
def manage_data(data, reader, writer, msg):
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
