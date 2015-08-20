from dist.core import serve, read, write, pingpong, manage_data, connect
from functools import partial
import asyncio


loop = asyncio.get_event_loop()


def test_server():
    handlers = {'ping': pingpong}

    @asyncio.coroutine
    def f():
        server = yield from serve('*', 8003, handlers, loop=loop)
        reader, writer = yield from connect('127.0.0.1', 8003, loop=loop)

        msg = {'op': 'ping', 'close': True}

        yield from write(writer, msg)
        response = yield from read(reader)

        assert response == b'pong'
        server.close()
        yield from server.wait_closed()

    loop.run_until_complete(asyncio.gather(f()))


def test_manage_data():
    d = dict()
    handlers = {'ping': pingpong,
                'get-data': partial(manage_data, d),
                'update-data': partial(manage_data, d),
                'del-data': partial(manage_data, d)}
    s = serve('*', 8004, handlers, loop=loop)

    @asyncio.coroutine
    def f():
        reader, writer = yield from connect('127.0.0.1', 8004, loop=loop)

        msg = {'op': 'update-data', 'data': {'x': 1, 'y': 2}}
        yield from write(writer, msg)
        response = yield from read(reader)
        assert response == b'OK'
        assert d == {'x': 1, 'y': 2}

        msg = {'op': 'get-data', 'keys': ['y']}
        yield from write(writer, msg)
        response = yield from read(reader)
        assert response == {'y': 2}

        msg = {'op': 'del-data', 'keys': ['y'], 'close': True}
        yield from write(writer, msg)
        response = yield from read(reader)
        assert response == b'OK'
        assert d == {'x': 1}

    loop.run_until_complete(asyncio.gather(s, f()))
