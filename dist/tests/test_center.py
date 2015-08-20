import asyncio
from time import sleep

from dist.core import read, write, connect, send_recv
from dist.center import Center

loop = asyncio.get_event_loop()


def test_metadata():
    c = Center('127.0.0.1', 8006, loop=loop)

    @asyncio.coroutine
    def f():
        reader, writer = yield from connect('127.0.0.1', 8006, loop=loop)

        response = yield from send_recv(reader, writer, op='register', address='alice')
        assert 'alice' in c.has_what

        response = yield from send_recv(reader, writer, op='add-keys',
                address='alice', keys=['x', 'y'], reply=True)
        assert response == b'OK'

        response = yield from send_recv(reader, writer, op='add-keys',
                address='bob', keys=['y', 'z'], reply=True)
        assert response == b'OK'

        response = yield from send_recv(reader, writer, op='who-has',
                keys=['x', 'y'])
        assert response == {'x': set(['alice']), 'y': set(['alice', 'bob'])}

        response = yield from send_recv(reader, writer, op='del-keys',
                address='bob', keys=['y'], reply=True)
        assert response == b'OK'

        response = yield from send_recv(reader, writer, op='has-what',
                keys=['alice', 'bob'])
        assert response == {'alice': set(['x', 'y']), 'bob': set(['z'])}

        response = yield from send_recv(reader, writer, op='unregister',
                                        address='alice', reply=True, close=True)
        assert response == b'OK'
        assert 'alice' not in c.has_what

        c.close()

    loop.run_until_complete(asyncio.gather(c.go(), f()))


def test_thread():
    c = Center('127.0.0.1', 8000, start=True, block=False)
    assert c.loop.is_running()
    while not hasattr(c, 'server'):
        sleep(0.01)
    c.close()
    assert not c.loop.is_running()
