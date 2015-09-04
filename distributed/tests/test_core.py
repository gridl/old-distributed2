from distributed.core import (read, write, pingpong, get_data, update_data,
        delete_data, connect,
        client_connected)
from functools import partial
import asyncio


loop = asyncio.get_event_loop()


def test_server():
    handlers = {'ping': pingpong}

    @asyncio.coroutine
    def f():
        server = yield from asyncio.start_server(client_connected(handlers),
                '*', 8003, loop=loop)
        reader, writer = yield from connect('127.0.0.1', 8003, loop=loop)

        msg = {'op': 'ping', 'close': False}
        yield from write(writer, msg)
        response = yield from read(reader)
        assert response == b'pong'

        msg = {'op': 'foo', 'close': True}
        yield from write(writer, msg)
        response = yield from read(reader)
        assert b'foo' in response

        server.close()
        yield from server.wait_closed()

    loop.run_until_complete(asyncio.gather(f()))


def test_manage_data():
    d = dict()
    handlers = {'ping': pingpong,
                'get_data': partial(get_data, d),
                'update_data': partial(update_data, d),
                'del_data': partial(delete_data, d)}
    s = asyncio.start_server(client_connected(handlers),
                             '*', 8004, loop=loop)

    @asyncio.coroutine
    def f():
        reader, writer = yield from connect('127.0.0.1', 8004, loop=loop)

        msg = {'op': 'update_data', 'data': {'x': 1, 'y': 2}}
        yield from write(writer, msg)
        response = yield from read(reader)
        assert response == b'OK'
        assert d == {'x': 1, 'y': 2}

        msg = {'op': 'get_data', 'keys': ['y']}
        yield from write(writer, msg)
        response = yield from read(reader)
        assert response == {'y': 2}

        msg = {'op': 'del_data', 'keys': ['y'], 'close': True}
        yield from write(writer, msg)
        response = yield from read(reader)
        assert response == b'OK'
        assert d == {'x': 1}

    loop.run_until_complete(asyncio.gather(s, f()))


def test_connect_timeout():
    @asyncio.coroutine
    def f():
        try:
            reader, writer = yield from connect('127.0.0.1', 9876, timeout=0.5)
            raise Exception("Did not raise")
        except ConnectionRefusedError:
            pass

    loop.run_until_complete(f())
