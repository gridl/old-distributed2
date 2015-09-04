import asyncio
from time import sleep

from distributed.core import read, write, connect, rpc
from distributed.center import Center

loop = asyncio.get_event_loop()


def test_metadata():
    c = Center('127.0.0.1', 8006, loop=loop)

    @asyncio.coroutine
    def f():
        reader, writer = yield from connect('127.0.0.1', 8006, loop=loop)

        cc = rpc(reader, writer)
        response = yield from cc.register(address='alice', ncores=4)
        assert 'alice' in c.has_what
        assert c.ncores['alice'] == 4

        response = yield from cc.add_keys(address='alice', keys=['x', 'y'])
        assert response == b'OK'

        response = yield from cc.register(address='bob', ncores=4)
        response = yield from cc.add_keys(address='bob', keys=['y', 'z'])
        assert response == b'OK'

        response = yield from cc.who_has(keys=['x', 'y'])
        assert response == {'x': set(['alice']), 'y': set(['alice', 'bob'])}

        response = yield from cc.remove_keys(address='bob', keys=['y'])
        assert response == b'OK'

        response = yield from cc.has_what(keys=['alice', 'bob'])
        assert response == {'alice': set(['x', 'y']), 'bob': set(['z'])}

        response = yield from cc.ncores()
        assert response == {'alice': 4, 'bob': 4}

        response = yield from cc.unregister(address='alice', close=True)
        assert response == b'OK'
        assert 'alice' not in c.has_what
        assert 'alice' not in c.ncores

        yield from c._close()

    loop.run_until_complete(asyncio.gather(c.go(), f()))


def test_thread():
    c = Center('127.0.0.1', 8000, start=True, block=False)
    assert c.loop.is_running()
    while not hasattr(c, 'server'):
        sleep(0.01)
    c.close()
    assert not c.loop.is_running()
