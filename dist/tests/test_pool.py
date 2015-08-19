import asyncio
from operator import add

from dist.core import serve, read, write, connect, manage_data, send_recv
from dist.center import Center
from dist.worker import Worker
from dist.pool import Pool

loop = asyncio.get_event_loop()


def test_pool():
    c = Center('127.0.0.1', 8017, loop=loop)

    a = Worker('127.0.0.1', 8018, c.ip, c.port, loop=loop)
    b = Worker('127.0.0.1', 8019, c.ip, c.port, loop=loop)

    p = Pool(c.ip, c.port, loop=loop)

    @asyncio.coroutine
    def f():
        yield from p._start()

        computation = yield from p._apply_async(add, [1, 2], {})
        x = yield from computation._get()
        result = yield from x._get()
        assert result == 3

        computation = yield from p._apply_async(add, [x, 10], {})
        y = yield from computation._get()
        result = yield from y._get()
        assert result == 13

        assert set((len(a.data), len(b.data))) == set((0, 2))

    loop.run_until_complete(asyncio.gather(c.go(), a.go(), b.go(), f()))
