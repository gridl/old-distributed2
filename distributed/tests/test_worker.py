import asyncio
from operator import add
from time import sleep

from distributed.core import read, write, connect, send_recv
from distributed.center import Center
from distributed.worker import Worker

loop = asyncio.new_event_loop()


def test_worker():
    c = Center('127.0.0.1', 8007, loop=loop)

    a = Worker('127.0.0.1', 8008, c.ip, c.port, loop=loop)
    b = Worker('127.0.0.1', 8009, c.ip, c.port, loop=loop)

    @asyncio.coroutine
    def f():
        a_reader, a_writer = yield from connect('127.0.0.1', a.port, loop=loop)
        b_reader, b_writer = yield from connect('127.0.0.1', b.port, loop=loop)

        response = yield from send_recv(a_reader, a_writer, op='compute',
            key='x', function=add, args=[1, 2], needed=[], reply=True)
        assert response == b'success'
        assert a.data['x'] == 3
        assert c.who_has['x'] == set([('127.0.0.1', a.port)])

        response = yield from send_recv(b_reader, b_writer, op='compute',
            key='y', function=add, args=['x', 10], needed=['x'], reply=True)
        assert response == b'success'
        assert b.data['y'] == 13
        assert c.who_has['y'] == set([('127.0.0.1', b.port)])

        def bad_func():
            1 / 0
        response = yield from send_recv(b_reader, b_writer, op='compute',
            key='z', function=bad_func, args=(), needed=(), reply=True)
        assert response == b'error'
        assert isinstance(b.data['z'], ZeroDivisionError)

        yield from write(a_writer, {'op': 'close', 'reply': True})
        yield from write(b_writer, {'op': 'close', 'reply': True})
        yield from read(a_reader)
        yield from read(b_reader)

        a_writer.close()
        b_writer.close()
        a.close()
        b.close()
        c.close()

    loop.run_until_complete(
            asyncio.gather(c.go(), a.go(), b.go(), f(), loop=loop))



def test_thread():
    c = Center('127.0.0.1', 8000, start=True, block=False)
    assert c.loop.is_running()
    w = Worker('127.0.0.1', 8001, c.ip, c.port, start=True, block=False)
    assert w.loop.is_running()
    while not hasattr(w, 'server'):
        sleep(0.01)
    c.close()
    w.close()
    assert not w.loop.is_running()


def test_log():
    c = Center('127.0.0.1', 8037, loop=loop)
    a = Worker('127.0.0.1', 8038, c.ip, c.port, loop=loop)
    @asyncio.coroutine
    def f():
        reader, writer = yield from connect('127.0.0.1', a.port, loop=loop)
        assert a._log

        yield from write(writer, {'op': 'close', 'reply': True})
        yield from read(reader)

        writer.close()
        a.close()
        c.close()

    loop.run_until_complete(asyncio.gather(c.go(), a.go(), f(), loop=loop))
