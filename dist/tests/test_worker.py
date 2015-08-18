import asyncio
from operator import add

from dist.core import serve, read, write, connect, manage_data, send_recv
from dist.center import Center
from dist.worker import Worker

loop = asyncio.get_event_loop()


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
        assert response == b'OK'
        assert a.data['x'] == 3
        assert c.who_has['x'] == set([('127.0.0.1', a.port)])

        response = yield from send_recv(b_reader, b_writer, op='compute',
        key='y', function=add, args=['x', 10], needed=['x'], reply=True)
        assert response == b'OK'
        assert b.data['y'] == 13
        assert c.who_has['y'] == set([('127.0.0.1', b.port)])

        yield from write(a_writer, {'op': 'close', 'reply': True})
        yield from write(b_writer, {'op': 'close', 'reply': True})
        yield from read(a_reader)
        yield from read(b_reader)

        a_writer.close()
        b_writer.close()

    loop.run_until_complete(asyncio.gather(c.go(), a.go(), b.go(), f()))
